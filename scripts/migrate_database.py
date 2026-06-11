# scripts/migrate_database.py
import os
import sys
from loguru import logger
from tqdm import tqdm

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import (
    Base,
    DailyPrice,
    CorporateAction,
    Exchange,
    HistoricalShare,
    InstitutionalHolding,
    InsiderTransaction,
    SecFiling,
    Security,
    SecurityIdentifier,
    SecuritySymbolHistory,
    TradingCalendar,
)

# --- 配置 ---
# 定义需要迁移的表，按照依赖关系排序 (Security 表通常最先)
TABLES_TO_MIGRATE = [
    Security,
    Exchange,
    TradingCalendar,
    SecurityIdentifier,
    SecFiling,
    InsiderTransaction,
    InstitutionalHolding,
    DailyPrice,
    HistoricalShare,
    SecuritySymbolHistory,
    CorporateAction,
]

# 批量插入的大小
BATCH_SIZE = 1000


def to_dict(obj):
    """将 SQLAlchemy ORM 对象转换为字典，移除内部状态"""
    return {c.key: getattr(obj, c.key) for c in obj.__table__.columns}


class DataMigrator:
    def __init__(self, old_db_url: str, new_db_url: str):
        if not old_db_url or not new_db_url:
            raise ValueError("OLD_DATABASE_URL 和 NEW_DATABASE_URL 必须在 .env 文件中设置。")

        logger.info("正在连接到旧数据库 (源)...")
        self.source_db = DatabaseManager(db_url=old_db_url)
        logger.info("正在连接到新数据库 (目标)...")
        self.target_db = DatabaseManager(db_url=new_db_url)

    def migrate_table(self, model_class):
        """迁移单个表的数据"""
        table_name = model_class.__tablename__
        logger.info(f"--- 开始迁移表: {table_name} ---")

        with self.source_db.get_session() as source_session, self.target_db.get_session() as target_session:
            try:
                # 1. 从源数据库查询所有数据
                logger.debug(f"正在从源数据库查询 '{table_name}' 的所有记录...")
                query = source_session.query(model_class)
                total_rows = query.count()

                if total_rows == 0:
                    logger.info(f"表 '{table_name}' 为空，跳过迁移。")
                    return

                logger.info(f"找到 {total_rows} 条记录，开始分批迁移...")

                # 2. 分批处理和批量插入
                for offset in tqdm(range(0, total_rows, BATCH_SIZE), desc=f"迁移 {table_name}"):
                    # 获取一批数据
                    batch = query.offset(offset).limit(BATCH_SIZE).all()
                    
                    # 将ORM对象转换为字典列表
                    data_to_insert = [to_dict(row) for row in batch]
                    
                    # 批量插入到目标数据库
                    target_session.bulk_insert_mappings(model_class, data_to_insert)

                # 3. 提交事务
                logger.debug(f"正在提交对 '{table_name}' 的所有更改...")
                target_session.commit()
                logger.success(f"✅ 成功迁移 {total_rows} 条记录到表 '{table_name}'。")

            except Exception as e:
                logger.opt(exception=e).error(f"迁移表 '{table_name}' 时发生错误: {e}")
                target_session.rollback()
                raise

    def run_migration(self):
        """执行所有表的迁移"""
        logger.info("🚀 开始数据库迁移流程...")
        # 确保目标数据库有最新的表结构
        logger.info("正在目标数据库上创建/检查所有表...")
        Base.metadata.create_all(self.target_db.engine)
        logger.success("目标数据库表结构准备就绪。")

        for model in TABLES_TO_MIGRATE:
            self.migrate_table(model)

        logger.success("🎉 所有数据迁移任务已成功完成！")

    def close(self):
        self.source_db.close()
        self.target_db.close()


def main():
    old_url = os.getenv("OLD_DATABASE_URL")
    new_url = os.getenv("NEW_DATABASE_URL")

    migrator = None
    try:
        migrator = DataMigrator(old_db_url=old_url, new_db_url=new_url)
        migrator.run_migration()
    except Exception as e:
        logger.critical(f"数据迁移过程中发生致命错误: {e}")
    finally:
        if migrator:
            migrator.close()

if __name__ == "__main__":
    main()
