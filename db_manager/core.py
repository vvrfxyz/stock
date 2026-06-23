"""引擎/会话生命周期与底层批量写入基础设施。"""
import os
from contextlib import contextmanager
from datetime import datetime, timezone

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from data_models.models import PipelineTaskRun

from .helpers import _build_upsert_statement, _dedupe_rows_by_key

load_dotenv()


class DatabaseManagerCore:
    def __init__(self, db_url: str = None):
        if db_url is None:
            db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("数据库URL未找到。请在 .env 文件中设置 DATABASE_URL 或在初始化时提供。")
        self.engine = create_engine(db_url, pool_pre_ping=True)
        self._session_factory = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        logger.info("数据库引擎创建成功。")

    def close(self):
        if self.engine:
            self.engine.dispose()
            logger.success("数据库引擎已成功关闭。")

    @contextmanager
    def get_session(self) -> Session:
        session = self._session_factory()
        try:
            yield session
        except Exception as e:
            logger.opt(exception=e).error(f"Session 上下文管理器捕获到异常，将执行回滚。原因: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def _sync_model_id_sequence(self, conn, model) -> None:
        """
        确保 PostgreSQL 自增序列不落后于现有主键数据。
        这在手工迁移/导库后很常见，否则后续 INSERT 会命中 duplicate key on *_pkey。
        """
        table = getattr(model, "__table__", None)
        if table is None or "id" not in table.columns:
            return

        table_name = table.name
        stmt = text(
            f"""
            WITH seq_name AS (
                SELECT pg_get_serial_sequence('{table_name}', 'id') AS name
            ),
            table_max AS (
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM {table_name}
            ),
            seq_state AS (
                SELECT COALESCE(last_value, 0) AS last_value
                FROM seq_name, pg_sequences
                WHERE schemaname || '.' || sequencename = seq_name.name
            )
            SELECT setval(
                (SELECT name FROM seq_name),
                GREATEST(
                    (SELECT max_id FROM table_max),
                    COALESCE((SELECT last_value FROM seq_state), 0),
                    1
                ),
                true
            )
            """
        )
        conn.execute(stmt)

    def _lock_model_sequence_sync(self, conn, model) -> None:
        table = getattr(model, "__table__", None)
        if table is None or "id" not in table.columns:
            return
        lock_stmt = text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))")
        conn.execute(lock_stmt, {"lock_key": f"seq-sync:{table.name}"})

    def _batch_upsert(
        self,
        model,
        data_list: list[dict],
        index_elements: list[str],
        *,
        update_on_conflict: bool = False,
        protected_columns: set[str] | None = None,
    ):
        """通用批量插入/忽略冲突的方法"""
        if not data_list:
            return 0

        data_list = _dedupe_rows_by_key(data_list, index_elements)

        stmt = _build_upsert_statement(
            model,
            data_list,
            index_elements,
            update_on_conflict=update_on_conflict,
            protected_columns=protected_columns,
        )

        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, model)
            self._sync_model_id_sequence(conn, model)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def bulk_update_mappings(self, model, mappings: list[dict]) -> int:
        """
        高效批量更新（executemany UPDATE），适合已知主键且只更新部分列的场景。
        :param model: SQLAlchemy ORM 模型类。
        :param mappings: 每条更新的字典，必须包含主键字段。
        :return: 尝试更新的记录数（不保证每条都实际命中行）。
        """
        if not mappings:
            return 0
        with self.get_session() as session:
            session.bulk_update_mappings(model, mappings)
            session.commit()
        return len(mappings)

    # ------------------------------------------------------------------ #
    # pipeline_task_runs 运行记录
    # ------------------------------------------------------------------ #
    def start_task_run(self, run_id: str, task_name: str) -> int:
        """记录一个 task 开始执行，返回 task_run.id。"""
        now = datetime.now(timezone.utc)
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, PipelineTaskRun)
            self._sync_model_id_sequence(conn, PipelineTaskRun)
            result = conn.execute(
                pg_insert(PipelineTaskRun).values(
                    run_id=run_id, task_name=task_name,
                    started_at=now, status="RUNNING",
                ).returning(PipelineTaskRun.id)
            )
            task_run_id = result.scalar_one()
            conn.commit()
        return task_run_id

    def finish_task_run(
        self, task_run_id: int, *, exit_code: int, error_sample: str | None = None,
        stats: dict | None = None,
    ) -> None:
        """标记一个 task 执行结束。stats dict 会 JSON 序列化追加到 error_sample。"""
        import json as _json
        now = datetime.now(timezone.utc)
        status = "SUCCESS" if exit_code == 0 else "FAILED"
        note_parts = []
        if error_sample:
            note_parts.append(error_sample)
        if stats:
            note_parts.append(_json.dumps(stats, ensure_ascii=False))
        note = " | ".join(note_parts) if note_parts else None
        with self.engine.connect() as conn:
            conn.execute(
                update(PipelineTaskRun)
                .where(PipelineTaskRun.id == task_run_id)
                .values(ended_at=now, exit_code=exit_code, status=status,
                        error_sample=note[:500] if note else None)
            )
            conn.commit()
