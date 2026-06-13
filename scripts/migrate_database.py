# scripts/migrate_database.py
import os
import sys
from typing import Any

from loguru import logger
from sqlalchemy import and_, or_
from tqdm import tqdm

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Base

# 批量插入的大小
BATCH_SIZE = 1000


def to_dict(obj):
    """将 SQLAlchemy ORM 对象转换为字典，移除内部状态。"""
    return {c.key: getattr(obj, c.key) for c in obj.__table__.columns}


def _models_in_dependency_order() -> list[type]:
    """从 ORM metadata 派生迁移清单，避免新增现役表后脚本静默漏迁。"""
    model_by_table = {mapper.local_table.name: mapper.class_ for mapper in Base.registry.mappers}
    return [model_by_table[table.name] for table in Base.metadata.sorted_tables if table.name in model_by_table]


def _pk_after_condition(pk_columns: list, last_key: tuple[Any, ...]):
    """复合主键 keyset 分页：WHERE pk_tuple > last_key。"""
    clauses = []
    for index, column in enumerate(pk_columns):
        prefix_equal = [pk_columns[i] == last_key[i] for i in range(index)]
        clauses.append(and_(*prefix_equal, column > last_key[index]))
    return or_(*clauses)


class DataMigrator:
    def __init__(self, old_db_url: str, new_db_url: str):
        if not old_db_url or not new_db_url:
            raise ValueError("OLD_DATABASE_URL 和 NEW_DATABASE_URL 必须在 .env 文件中设置。")

        logger.info("正在连接到旧数据库 (源)...")
        self.source_db = DatabaseManager(db_url=old_db_url)
        logger.info("正在连接到新数据库 (目标)...")
        self.target_db = DatabaseManager(db_url=new_db_url)

    def migrate_table(self, model_class):
        """迁移单个表的数据：按主键稳定排序 + keyset 分页。"""
        table_name = model_class.__tablename__
        logger.info(f"--- 开始迁移表: {table_name} ---")

        with self.source_db.get_session() as source_session, self.target_db.get_session() as target_session:
            try:
                query = source_session.query(model_class)
                total_rows = query.count()

                if total_rows == 0:
                    logger.info(f"表 '{table_name}' 为空，跳过迁移。")
                    return

                pk_columns = list(model_class.__mapper__.primary_key)
                logger.info(f"找到 {total_rows} 条记录，开始分批迁移...")

                migrated = 0
                last_key: tuple[Any, ...] | None = None
                with tqdm(total=total_rows, desc=f"迁移 {table_name}") as progress:
                    while True:
                        batch_query = source_session.query(model_class).order_by(*pk_columns)
                        if last_key is not None:
                            batch_query = batch_query.filter(_pk_after_condition(pk_columns, last_key))
                        batch = batch_query.limit(BATCH_SIZE).all()
                        if not batch:
                            break

                        data_to_insert = [to_dict(row) for row in batch]
                        target_session.bulk_insert_mappings(model_class, data_to_insert)
                        target_session.flush()

                        migrated += len(batch)
                        progress.update(len(batch))
                        last_row = batch[-1]
                        last_key = tuple(getattr(last_row, column.key) for column in pk_columns)

                target_session.commit()

                # 迁移保留了源端主键 id；目标自增序列若不追平，应用首条 INSERT 会撞 *_pkey。
                if "id" in model_class.__table__.columns:
                    with self.target_db.engine.connect() as conn:
                        self.target_db._sync_model_id_sequence(conn, model_class)
                        conn.commit()

                logger.success(f"✅ 成功迁移 {migrated} 条记录到表 '{table_name}'。")

            except Exception as e:
                logger.opt(exception=e).error(f"迁移表 '{table_name}' 时发生错误: {e}")
                target_session.rollback()
                raise

    def run_migration(self):
        """执行所有表的迁移。"""
        logger.info("🚀 开始数据库迁移流程...")
        logger.info("正在目标数据库上创建/检查所有表...")
        Base.metadata.create_all(self.target_db.engine)
        logger.success("目标数据库表结构准备就绪。")

        models = _models_in_dependency_order()

        # 本脚本是一次性全量导库，INSERT 无 ON CONFLICT；目标非空会在中途撞主键，
        # 且前面的表已 commit 无法回滚。开跑前先确认目标为空，避免半迁移的脏状态。
        non_empty = []
        with self.target_db.get_session() as session:
            for model in models:
                if session.query(model).limit(1).count() > 0:
                    non_empty.append(model.__tablename__)
        if non_empty:
            raise RuntimeError(
                "目标库以下表已有数据，拒绝迁移（本脚本不可重入、无 ON CONFLICT）："
                f"{', '.join(non_empty)}。请清空目标或迁到全新库。"
            )

        logger.info("迁移表清单 {} 张: {}", len(models), ", ".join(model.__tablename__ for model in models))
        for model in models:
            self.migrate_table(model)

        logger.success("🎉 所有数据迁移任务已成功完成！")

    def close(self):
        self.source_db.close()
        self.target_db.close()


def main(argv: list[str] | None = None) -> int:
    old_url = os.getenv("OLD_DATABASE_URL")
    new_url = os.getenv("NEW_DATABASE_URL")

    migrator = None
    try:
        migrator = DataMigrator(old_db_url=old_url, new_db_url=new_url)
        migrator.run_migration()
        return 0
    except Exception as e:
        logger.opt(exception=e).critical(f"数据迁移过程中发生致命错误: {e}")
        return 1
    finally:
        if migrator:
            migrator.close()


if __name__ == "__main__":
    raise SystemExit(main())
