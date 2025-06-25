# db_manager.py
import os
from contextlib import contextmanager
from loguru import logger
from dotenv import load_dotenv
from datetime import date, datetime

from sqlalchemy import create_engine, desc, or_
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data_models.models import Base, Security, DailyPrice, CorporateAction

load_dotenv()


class DatabaseManager:
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

    def close(self):
        if self.engine:
            logger.info("正在关闭数据库引擎连接池...")
            self.engine.dispose()
            logger.success("数据库引擎已成功关闭。")

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
        with self.get_session() as session:
            security = session.query(Security).filter_by(symbol=symbol).first()
            if security:
                logger.trace(f"在数据库中找到 Security: {symbol}, ID: {security.id}")
                return security.id

            logger.info(f"数据库中未找到 {symbol}，将创建新记录。")
            if defaults is None:
                defaults = {}

            params = {'symbol': symbol, **defaults}
            new_security = Security(**params)
            session.add(new_security)
            session.flush()

            new_id = new_security.id
            logger.info(f"已为 {symbol} 创建 Security 记录，ID: {new_id}")
            return new_id

    def get_last_price_date(self, security_id: int) -> date | None:
        """
        高效地从 Security 表获取指定 security_id 的最新价格日期。
        :param security_id: 证券的ID。
        :return: 最新的日期 (datetime.date)，如果不存在则返回 None。
        """
        with self.get_session() as session:
            last_date = session.query(Security.price_data_latest_date).filter(
                Security.id == security_id
            ).scalar()

            if last_date:
                logger.debug(f"从 Security 表找到 security_id={security_id} 的最新数据日期: {last_date}")
                return last_date

            logger.info(f"Security 表中未记录 security_id={security_id} 的价格数据日期。")
            return None

    # --- OPTIMIZATION START: 新增方法用于更新状态和获取待更新列表 ---
    def update_security_latest_price_date(self, security_id: int, latest_date: date):
        """
        更新指定 Security 的 price_data_latest_date 字段。
        """
        with self.get_session() as session:
            session.query(Security).filter(Security.id == security_id).update(
                {'price_data_latest_date': latest_date}
            )
        logger.debug(f"已更新 security_id={security_id} 的最新价格日期为 {latest_date}")

    def get_securities_to_update(self, target_date: date) -> list[str]:
        """
        获取所有需要进行数据更新的股票代码列表。
        条件：is_active=True 且 (price_data_latest_date < target_date 或 price_data_latest_date 为空)
        """
        with self.get_session() as session:
            results = session.query(Security.symbol).filter(
                Security.is_active == True,
                or_(
                    Security.price_data_latest_date == None,
                    Security.price_data_latest_date < target_date
                )
            ).all()
            symbols = [r[0] for r in results]
            logger.info(f"找到 {len(symbols)} 个需要更新数据的股票。")
            return symbols


    def upsert_security_info(self, security_data: dict):
        stmt = pg_insert(Security).values(security_data)
        update_cols = {col.name: col for col in stmt.excluded if
                       col.name not in ['symbol', 'id', 'price_data_latest_date']}
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
