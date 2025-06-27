# db_manager.py (已优化)
import os
from contextlib import contextmanager
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data_models.models import Base, Security  # 移除了无用导入

load_dotenv()


class DatabaseManager:
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
            logger.error(f"Session 上下文管理器捕获到异常，将执行回滚。原因: {e}", exc_info=True)
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_security_info(self, security_data: dict):
        """
        智能地更新或插入 Security 信息 (UPSERT)。
        - 如果记录不存在，则插入新记录。
        - 如果记录已存在，则根据传入的 `security_data` 字典更新字段。
        - **关键**: 字典中未包含的字段 (如 em_code) 将保持不变，从而保护了现有数据。
        - 更新操作通过主键 `id` 进行定位，确保精确性。
        """
        if 'id' not in security_data:
            raise ValueError("更新数据必须包含 'id' 字段以定位记录。")

        # 使用 SQLAlchemy 2.0 风格的 insert 语句
        stmt = pg_insert(Security).values(security_data)

        # 定义冲突时的更新策略
        # 1. 从传入的字典中提取需要更新的列名
        #    排除主键 'id' 和唯一约束 'symbol'，因为它们用于冲突判断，不能在 set_ 中更新。
        update_columns = {
            col.name: col
            for col in stmt.excluded
            if col.name not in ['id', 'symbol', 'em_code']  # 再次确保 em_code 不被意外更新
        }

        # 2. 无论如何都要更新时间戳
        update_columns['info_last_updated_at'] = func.now()

        # 3. 构建完整的 on_conflict_do_update 语句
        #    当 'id' 冲突时，执行更新操作
        final_stmt = stmt.on_conflict_do_update(
            index_elements=['id'],  # 使用主键 'id' 进行冲突检测
            set_=update_columns
        )

        # 4. 执行
        with self.engine.connect() as conn:
            conn.execute(final_stmt)
            conn.commit()

        logger.success(f"✅ 成功更新 Security (ID: {security_data['id']}, Symbol: {security_data.get('symbol', 'N/A')})")
