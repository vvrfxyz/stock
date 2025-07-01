# db_manager.py (已优化)
import os
from contextlib import contextmanager
from datetime import date
from sqlalchemy import create_engine, func, text
from loguru import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from data_models.models import Base, Security, StockDividend, StockSplit, DailyPrice
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

    def _batch_upsert(self, model, data_list: list[dict], index_elements: list[str]):
        """通用批量插入/忽略冲突的方法"""
        if not data_list:
            return 0

        stmt = pg_insert(model).values(data_list)
        # 使用 on_conflict_do_nothing 策略，如果记录已存在则忽略
        stmt = stmt.on_conflict_do_nothing(index_elements=index_elements)

        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def upsert_dividends(self, security_id: int, dividends_data: list[dict]):
        """批量插入分红数据，如果已存在则忽略"""
        if not dividends_data:
            return
        # 为每条记录添加 security_id
        for item in dividends_data:
            item['security_id'] = security_id

        # 使用在 StockDividend 模型中定义的唯一约束字段
        self._batch_upsert(StockDividend, dividends_data, ['security_id', 'ex_dividend_date', 'cash_amount'])
        logger.debug(f"为 Security ID {security_id} 同步 {len(dividends_data)} 条分红记录。")

    def upsert_splits(self, security_id: int, splits_data: list[dict]):
        """批量插入拆股数据，如果已存在则忽略"""
        if not splits_data:
            return
        for item in splits_data:
            item['security_id'] = security_id

        # 使用在 StockSplit 模型中定义的唯一约束字段
        self._batch_upsert(StockSplit, splits_data, ['security_id', 'execution_date'])
        logger.debug(f"为 Security ID {security_id} 同步 {len(splits_data)} 条拆股记录。")

    def update_security_timestamp(self, security_id: int, field_name: str):
        """更新 Security 表中指定的时间戳字段为当前时间"""
        allowed_fields = [
            'info_last_updated_at', 'price_data_latest_date',
            'full_data_last_updated_at', 'actions_last_updated_at'
        ]
        if field_name not in allowed_fields:
            raise ValueError(f"无效的时间戳字段名: {field_name}")
        stmt = (
            update(Security)
            .where(Security.id == security_id)
            .values({field_name: func.now()})
        )
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def bulk_update_records(self, records: list) -> int:
        """
        【推荐】通用的、高效的批量更新 ORM 对象列表的方法。
        它使用 session.merge() 来处理更新。
        :param records: 包含已修改数据的 ORM 对象列表（可以是任何模型）。
        :return: 成功更新的记录数。
        """
        if not records:
            return 0
        with self.get_session() as session:
            for record in records:
                session.merge(record)
            session.commit()
        return len(records)

    def upsert_daily_prices(self, price_data: list[dict]) -> int:
        """
        批量插入或更新日线价格数据 (基于UPSERT)。
        此方法适用于需要高性能批量插入/更新的场景，如 akshare 脚本。
        """
        if not price_data:
            return 0

        stmt = pg_insert(DailyPrice).values(price_data)
        # 动态构建更新集
        update_keys = price_data[0].keys()
        update_columns = {}
        if 'open' in update_keys: update_columns['open'] = stmt.excluded.open
        if 'high' in update_keys: update_columns['high'] = stmt.excluded.high
        if 'low' in update_keys: update_columns['low'] = stmt.excluded.low
        if 'close' in update_keys: update_columns['close'] = stmt.excluded.close
        if 'volume' in update_keys: update_columns['volume'] = stmt.excluded.volume
        if 'turnover' in update_keys: update_columns['turnover'] = stmt.excluded.turnover
        if 'vwap' in update_keys: update_columns['vwap'] = stmt.excluded.vwap
        if 'turnover_rate' in update_keys: update_columns['turnover_rate'] = stmt.excluded.turnover_rate
        if not update_columns:
            stmt = stmt.on_conflict_do_nothing(index_elements=['security_id', 'date'])
        else:
            stmt = stmt.on_conflict_do_update(
                index_elements=['security_id', 'date'],
                set_=update_columns
            )
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def update_security_price_latest_date(self, security_id: int, latest_date: date, is_full_run: bool):
        """
        更新 Security 表中的价格数据最新日期和全量更新时间戳。
        """
        values_to_update = {
            'price_data_latest_date': latest_date
        }
        if is_full_run:
            values_to_update['full_data_last_updated_at'] = func.now()
        stmt = (
            update(Security)
            .where(Security.id == security_id)
            .values(values_to_update)
        )
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    # ==============================================================================
    #  【新增】原生SQL方法 (专供 update_actions_from_polygon.py 使用)
    # ==============================================================================
    def upsert_dividends_native_sql(self, security_id: int, dividends_data: list[dict]):
        """
        【原生SQL - 调试版】逐条插入分红数据，如果已存在则忽略。
        如果单条记录插入失败，会打印详细的错误信息并继续处理下一条。
        注意：此版本为调试优化，性能低于批量版本。
        """
        if not dividends_data:
            return
        # SQL模板只定义一次
        sql_template = text("""
            INSERT INTO stock_dividends (
                security_id, ex_dividend_date, declaration_date, record_date, pay_date, 
                cash_amount, currency, frequency
            ) VALUES (
                :security_id, :ex_dividend_date, :declaration_date, :record_date, :pay_date,
                :cash_amount, :currency, :frequency
            )
            ON CONFLICT (security_id, ex_dividend_date, cash_amount) DO NOTHING
        """)
        success_count = 0
        fail_count = 0
        # 开启一次连接和事务，包裹整个循环，以提高效率
        with self.engine.connect() as conn:
            with conn.begin() as trans:
                for item in dividends_data:
                    # 为当前记录添加 security_id
                    item['security_id'] = security_id

                    try:
                        # 对单条记录执行SQL
                        conn.execute(sql_template, item)
                        success_count += 1
                    except Exception as e:
                        fail_count += 1
                        # --- 核心错误处理逻辑 ---
                        # 1. 在控制台打印醒目的错误信息
                        print("-" * 80)
                        print(f"🚨 [DATABASE ERROR] Failed to insert a dividend record for security_id: {security_id}")

                        # 2. 打印导致错误的SQL模板 (SQLAlchemy不会直接渲染值，这是为了安全)
                        print("\n[SQL TEMPLATE]:")
                        print(sql_template)

                        # 3. 打印导致错误的具体数据
                        print("\n[PROBLEM DATA]:")
                        # 使用 json 更易读
                        import json
                        print(json.dumps(item, indent=2, default=str))  # default=str 处理日期等对象

                        # 4. 打印具体的异常信息
                        print(f"\n[EXCEPTION]:\n{e}")
                        print("-" * 80)

                        # 5. 在日志文件中记录完整的错误堆栈信息
                        logger.error(
                            f"Failed to insert dividend record for security_id={security_id}. Data: {item}",
                            exc_info=True  # exc_info=True 会记录完整的错误堆栈
                        )
                        # 循环会继续，不会在此处中断
        if fail_count > 0:
            logger.warning(
                f"[原生SQL] For Security ID {security_id}, "
                f"processed {len(dividends_data)} dividend records. "
                f"Succeeded: {success_count}, Failed: {fail_count}."
            )
        else:
            logger.debug(
                f"[原生SQL] For Security ID {security_id}, "
                f"successfully processed {success_count} dividend records."
            )
    def upsert_splits_native_sql(self, security_id: int, splits_data: list[dict]):
        """
        【原生SQL】批量插入拆股数据，如果已存在则忽略。
        使用 PostgreSQL 的 ON CONFLICT DO NOTHING。
        """
        if not splits_data:
            return

        for item in splits_data:
            item['security_id'] = security_id
        sql = text("""
            INSERT INTO stock_splits (
                security_id, execution_date, declaration_date, split_to, split_from
            ) VALUES (
                :security_id, :execution_date, :declaration_date, :split_to, :split_from
            )
            ON CONFLICT (security_id, execution_date) DO NOTHING
        """)
        with self.engine.connect() as conn:
            with conn.begin() as trans:
                conn.execute(sql, splits_data)
        logger.debug(f"[原生SQL] 为 Security ID {security_id} 同步 {len(splits_data)} 条拆股记录。")

    def update_security_timestamp_native_sql(self, security_id: int, field_name: str):
        """
        【原生SQL】更新 Security 表中指定的时间戳字段为当前时间。
        """
        allowed_fields = [
            'info_last_updated_at', 'price_data_latest_date',
            'full_data_last_updated_at', 'actions_last_updated_at'
        ]
        if field_name not in allowed_fields:
            raise ValueError(f"无效的时间戳字段名: {field_name}")
        # 使用 f-string 插入列名是安全的，因为我们已经通过白名单验证了 field_name
        # 值（如 security_id）必须通过参数绑定传递
        sql = text(f"""
            UPDATE securities 
            SET {field_name} = NOW() 
            WHERE id = :security_id
        """)
        with self.engine.connect() as conn:
            with conn.begin() as trans:
                conn.execute(sql, {"security_id": security_id})