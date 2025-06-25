# populate_calendars.py

import sys
from datetime import date
import exchange_calendars as xcals
from loguru import logger

# 假设你的项目结构如下，需要将项目根目录添加到 sys.path
# a_quant_project/
# |- populate_calendars.py
# |- db_manager.py
# |- data_models/
#    |- models.py
sys.path.append('.')

from db_manager import DatabaseManager
from data_models.models import MarketType, TradingCalendar

# 1. 定义市场与日历的映射关系
MARKET_CALENDAR_MAP = {
    MarketType.CNA: "XSHG",  # China - Shanghai Stock Exchange
    MarketType.HK: "XHKG",  # Hong Kong - Hong Kong Stock Exchange
    MarketType.US: "XNYS",  # USA - New York Stock Exchange
}

# 2. 定义要填充的年份范围
START_YEAR = 2010
# 填充到明年，确保日历总是最新的
END_YEAR = date.today().year + 1


def populate_trading_calendars():
    """
    使用 exchange_calendars 库填充 trading_calendars 表。
    """
    logger.info("开始填充交易日历数据...")
    db_manager = None
    try:
        db_manager = DatabaseManager()

        all_calendar_entries = []

        for market_type, calendar_name in MARKET_CALENDAR_MAP.items():
            logger.info(f"正在为市场 [{market_type.value}] 获取日历，使用日历: {calendar_name}")

            try:
                # 获取日历对象
                calendar = xcals.get_calendar(calendar_name)

                # 获取指定范围内的所有有效交易日
                # calendar.schedule 是一个包含所有交易日的 DataFrame
                schedule = calendar.schedule

                # 筛选出指定年份范围内的日期
                start_date_str = f'{START_YEAR}-01-01'
                end_date_str = f'{END_YEAR}-12-31'
                valid_days = schedule.loc[start_date_str:end_date_str].index


                market_entries = [
                    {
                        'market': market_type,
                        'trade_date': d.date()  # 转换为 date 对象
                    }
                    for d in valid_days
                ]

                all_calendar_entries.extend(market_entries)
                logger.success(f"成功为市场 [{market_type.value}] 生成了 {len(market_entries)} 条交易日记录。")

            except Exception as e:
                logger.error(f"为市场 [{market_type.value}] 获取日历时出错: {e}")

        if not all_calendar_entries:
            logger.warning("没有生成任何日历数据，程序退出。")
            return

        logger.info(f"准备将总共 {len(all_calendar_entries)} 条日历记录插入数据库...")

        # 使用我们已有的 bulk_upsert 方法，它能处理重复插入（基于唯一约束）
        db_manager.bulk_upsert(
            model_class=TradingCalendar,
            data=all_calendar_entries,
            # 明确提供构成唯一约束的列
            index_elements=['market', 'trade_date'],
            # 同时提供约束名称，可以提高性能
            constraint='_market_trade_date_uc'
        )

        logger.success("交易日历数据填充/更新成功！")

    except Exception as e:
        logger.critical(f"填充交易日历时发生严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()


if __name__ == "__main__":
    # 配置日志，以便在控制台看到输出
    if __name__ == "__main__":
        # 配置日志，确保只有一个控制台输出
        logger.remove()  # 移除所有已有的处理器
        logger.add(sys.stderr, level="INFO")  # 添加我们想要的处理器
        populate_trading_calendars()
