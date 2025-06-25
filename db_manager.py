# db_manager.py
import os
from contextlib import contextmanager
from loguru import logger
from dotenv import load_dotenv
from datetime import date

from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data_models.models import Base, Security, DailyPrice, CorporateAction, MarketType, AssetType

load_dotenv()


class DatabaseManager:
    """
    一个用于管理数据库连接和操作的类。
    结合了高效的批量操作和安全的会话管理。
    """

    def close(self):
        """
        显式关闭数据库引擎和连接池。
        """
        if self.engine:
            logger.info("正在关闭数据库引擎连接池...")
            self.engine.dispose()
            logger.success("数据库引擎已成功关闭。")

    def __init__(self, db_url: str = None):
        if db_url is None:
            db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("数据库URL未找到。请在 .env 文件中设置 DATABASE_URL 或在初始化时提供。")

        try:
            self.engine = create_engine(db_url, pool_pre_ping=True)
            self._session_factory = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            logger.info("数据库引擎创建成功。")
        except Exception as e:
            logger.error(f"创建数据库引擎失败: {e}")
            raise

    @contextmanager
    def get_session(self) -> Session:
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            logger.error(f"Session 回滚，原因: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def create_tables(self):
        logger.info("正在创建数据库表（如果不存在）...")
        Base.metadata.create_all(self.engine)
        logger.success("数据库表检查/创建完成。")

    def get_or_create_security_id(self, symbol: str, defaults: dict = None) -> int:
        """
        根据 symbol 获取或创建 Security 记录，并返回其 ID。
        :return: Security 记录的主键 ID (int)。
        """
        with self.get_session() as session:
            security_id = session.query(Security.id).filter_by(symbol=symbol).scalar()
            if security_id:
                logger.trace(f"在数据库中找到 Security: {symbol}, ID: {security_id}")
                return security_id

            logger.info(f"数据库中未找到 {symbol}，将创建新记录。")
            if defaults is None:
                defaults = {}

            params = {'symbol': symbol, **defaults}
            security = Security(**params)
            session.add(security)
            session.flush()

            new_id = security.id
            logger.info(f"已为 {symbol} 创建 Security 记录，ID: {new_id}")
            return new_id

    # --- OPTIMIZATION START: 新增方法 ---
    def get_last_price_date(self, security_id: int) -> date | None:
        """
        查询指定 security_id 在数据库中的最新价格日期。
        :param security_id: 证券的ID。
        :return: 最新的日期 (datetime.date)，如果不存在则返回 None。
        """
        with self.get_session() as session:
            last_record = session.query(DailyPrice.date).filter(
                DailyPrice.security_id == security_id
            ).order_by(desc(DailyPrice.date)).first()

            if last_record:
                logger.debug(f"找到 security_id={security_id} 的最新数据日期: {last_record[0]}")
                return last_record[0]

            logger.info(f"数据库中未找到 security_id={security_id} 的任何价格数据。")
            return None

    # --- OPTIMIZATION END ---

    def upsert_security_info(self, security_data: dict):
        stmt = pg_insert(Security).values(security_data)
        update_cols = {col.name: col for col in stmt.excluded if col.name not in ['symbol', 'id']}
        stmt = stmt.on_conflict_do_update(index_elements=['symbol'], set_=update_cols)

        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        logger.success(f"成功更新/插入 Security 信息: {security_data.get('symbol')}")

    def bulk_upsert(self, model_class, data: list[dict], index_elements: list[str], constraint: str = None):
        if not data:
            logger.info(f"没有 {model_class.__tablename__} 数据需要更新。")
            return

        stmt = pg_insert(model_class).values(data)
        update_cols = {col.name: col for col in stmt.excluded if col.name not in index_elements}
        conflict_target = {'constraint': constraint} if constraint else {'index_elements': index_elements}
        stmt = stmt.on_conflict_do_update(**conflict_target, set_=update_cols)

        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        logger.success(f"成功批量更新/插入 {len(data)} 条记录到 {model_class.__tablename__}。")

