# db_manager.py
import os
from contextlib import contextmanager
from typing import Optional, Tuple

from loguru import logger
from dotenv import load_dotenv
from datetime import date, datetime

from sqlalchemy import create_engine, desc, or_, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert, INTERVAL

from data_models.models import Base, Security, DailyPrice, CorporateAction, MarketType, TradingCalendar

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
            # session.commit()
        except Exception as e:
            logger.error(f"Session 上下文管理器捕获到异常，将执行回滚。原因: {e}")
            logger.critical(f"异常详情: {type(e).__name__} - {e}", exc_info=True)
            session.rollback()
            raise
        finally:
            session.close()

    def is_trading_day(self, market: MarketType, check_date: date) -> bool:
        """检查指定市场和日期是否为交易日"""
        with self.get_session() as session:
            # 这里假设 TradingCalendar 表已经填充了数据
            # 可以使用 exchange_calendars 库来填充这个表
            exists = session.query(TradingCalendar.id).filter_by(
                market=market,
                trade_date=check_date
            ).first()
            return exists is not None

    def get_securities_for_auto_full_refresh(self) -> list[str]:
        """获取达到自动全量刷新周期的股票列表"""
        with self.get_session() as session:
            # 使用数据库的日期/时间函数进行计算
            # a. full_data_last_updated_at IS NULL (从未全量更新过)
            # b. NOW() > full_data_last_updated_at + (full_refresh_interval * '1 day'::interval) (已到更新周期)
            # 注意: '1 day'::interval 是 PostgreSQL 语法，其他数据库可能不同
            securities_to_refresh = session.query(Security.symbol).filter(
                Security.is_active == True,
                or_(
                    Security.full_data_last_updated_at.is_(None),
                    func.now() > Security.full_data_last_updated_at +
                    (Security.full_refresh_interval * func.cast('1 day', INTERVAL))  # sqlalchemy interval
                )
            ).all()
            return [s[0] for s in securities_to_refresh]

    def update_security_full_refresh_timestamp(self, security_id: int):
        """当全量刷新成功后，更新时间戳"""
        with self.get_session() as session:
            session.query(Security).filter(Security.id == security_id).update(
                {'full_data_last_updated_at': func.now()}
            )
            logger.info(f"已更新 security_id={security_id} 的全量数据更新时间戳。")

    def has_corporate_actions(self, security_id: int) -> bool:
        """
        快速检查指定的 security_id 是否在 CorporateAction 表中存在任何记录。

        :param security_id: 证券ID。
        :return: 如果存在记录则返回 True，否则返回 False。
        """
        with self.get_session() as session:
            # 使用 .first() 是最高效的方式，因为它找到第一条记录后就会停止查询
            exists = session.query(CorporateAction.id).filter(
                CorporateAction.security_id == security_id
            ).first()
            return exists is not None

    def update_security_actions_timestamp(self, security_id: int):
        """当公司行动数据成功同步后，更新时间戳"""
        with self.get_session() as session:
            session.query(Security).filter(Security.id == security_id).update(
                {'actions_last_updated_at': func.now()}
            )
            session.commit()  # 确保立即提交这个单一的更新
            logger.debug(f"已更新 security_id={security_id} 的公司行动数据更新时间戳。")

    def create_tables(self):
        logger.info("正在创建数据库表（如果不存在）...")
        Base.metadata.create_all(self.engine)
        logger.success("数据库表检查/创建完成。")

    def get_or_create_security_id(self, symbol: str, defaults: dict = None) -> int:
        symbol = symbol.lower()
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
        """
        智能地更新或插入 Security 信息。
        在冲突时，只更新 security_data 字典中提供的字段，而不会覆盖其他字段（如 em_code）。
        """
        stmt = pg_insert(Security).values(security_data)
        # 核心修改：只更新传入字典中存在的键对应的列
        # 1. 获取传入数据的所有键
        keys_to_update = security_data.keys()

        # 2. 构建一个只包含这些键的更新字典
        #    我们从 `stmt.excluded` 中获取值，这是 SQLAlchemy on_conflict 的标准做法。
        #    我们还排除了唯一约束的键，因为它们不能在 UPDATE 部分被更新。
        update_cols = {
            key: getattr(stmt.excluded, key)
            for key in keys_to_update
            if key not in ['symbol', 'market', 'type', 'id']  # 排除唯一约束和主键
        }
        # 3. 无论如何都要更新时间戳
        update_cols['info_last_updated_at'] = func.now()
        # 4. 构建完整的 on_conflict 语句
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'market', 'type'],
            set_=update_cols
        )
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        logger.success(f"成功更新/插入 Security 信息: {security_data.get('symbol')}")

    def get_security_by_symbol(self, symbol: str) -> Security | None:
        """通过股票代码获取完整的 Security 对象"""
        with self.get_session() as session:
            return session.query(Security).filter(Security.symbol == symbol).first()

    def get_latest_daily_price_details(self, security_id: int) -> Optional[dict]:
        """
        获取指定 security_id 最新的 DailyPrice 记录的核心验证字段。
        返回一个字典，而不是绑定的ORM对象，以避免会话问题。

        :param security_id: 证券ID。
        :return: 包含 'date' 和 'adj_factor' 的字典，或在无记录时返回 None。
        """
        with self.get_session() as session:
            latest_record = session.query(
                DailyPrice.date,
                DailyPrice.adj_factor
            ).filter(
                DailyPrice.security_id == security_id
            ).order_by(
                DailyPrice.date.desc()
            ).first()
            if latest_record:
                # 返回一个普通字典，这是安全的
                return {
                    'date': latest_record.date,
                    'adj_factor': float(latest_record.adj_factor)
                }

            return None

    def get_security_details_for_update(self, symbol: str) -> Optional[Tuple[int, MarketType, bool]]:
        """
        获取用于更新决策所需的核心安全信息，以避免传递游离对象。
        :param symbol: 股票代码。
        :return: 一个包含 (id, market, is_active) 的元组，如果找不到则返回 None。
        """
        with self.get_session() as session:
            # 只查询我们需要的列
            details = session.query(
                Security.id,
                Security.market,
                Security.is_active
            ).filter(Security.symbol == symbol).first()
            return details  # SQLAlchemy 返回的是一个类似元组的 RowProxy 对象，这是安全的

    def get_latest_trading_day(self, market: MarketType, as_of_date: date) -> date | None:
        """
        获取指定市场在 as_of_date 或之前的最后一个交易日。
        :param market: 市场类型。
        :param as_of_date: 查询的截止日期。
        :return: 最后一个交易日的日期，如果日历中没有数据则返回 None。
        """
        with self.get_session() as session:
            latest_trade_date = session.query(func.max(TradingCalendar.trade_date)).filter(
                TradingCalendar.market == market,
                TradingCalendar.trade_date <= as_of_date
            ).scalar()
            return latest_trade_date

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
