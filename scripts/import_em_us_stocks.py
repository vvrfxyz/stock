# scripts/import_em_us_stocks.py
from datetime import datetime
import time  # 1. 导入 time 模块
import json
from loguru import logger
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from db_manager import DatabaseManager
from data_sources.yfinance_source import YFinanceSource
from data_models.models import Security, MarketType, AssetType
import sys
import os

# 将项目根目录添加到Python路径中，以便可以导入其他模块
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)


# --- 配置区 ---
JSON_FILE_PATH = os.path.join(os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..')), 'us_stock_spot_data.json')


def setup_logging():
    """配置 Loguru 日志记录器"""
    logger.remove()
    logger.add(sys.stderr, level="DEBUG",
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
    log_dir = os.path.join(os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..')), "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(os.path.join(
        log_dir, "import_em_us_{time}.log"), rotation="10 MB", retention="10 days", level="DEBUG")
    logger.info("日志记录器设置完成。")


def parse_em_us_code(em_code: str) -> tuple[str, str]:
    """简化解析东方财富代码，只返回 yfinance 兼容的代码和东方财富的前缀。"""
    try:
        prefix, standard_symbol = em_code.split('.', 1)
        yfinance_symbol = standard_symbol.replace('_', '.')
        return yfinance_symbol, prefix
    except ValueError:
        logger.error(f"无法解析东方财富代码: {em_code}")
        return None, None


def get_yfinance_info_with_retry(yfinance_source: YFinanceSource, symbol: str) -> dict:
    """
    NEW: 安全地获取 yfinance 信息，并包含重试和终止逻辑。
    """
    for attempt in range(2):  # 共尝试2次
        try:
            info = yfinance_source.get_security_info(symbol)
            if not info:
                return {}

            list_date = info.get('firstTradeDateEpochUtc')
            if list_date:
                try:
                    info['list_date'] = datetime.utcfromtimestamp(
                        list_date).date()
                except (ValueError, TypeError):
                    info['list_date'] = None

            return info  # 成功获取，直接返回
        except Exception as e:
            logger.error(f"第 {attempt + 1} 次尝试获取 '{symbol}' 信息时出错: {e}")
            if attempt == 0:  # 如果是第一次失败
                logger.warning("等待 60 秒后重试...")
                time.sleep(65)
            else:  # 如果是第二次失败
                logger.critical("重试失败，yfinance API 可能无法访问。根据指令，终止脚本运行。")
                sys.exit(1)  # 终止程序
    return {}  # 理论上不会执行到这里


def fallback_exchange_from_prefix(prefix: str) -> str:
    """当 yfinance 无法提供交易所信息时的备用方案。"""
    if prefix == '105':
        return 'NASDAQ'
    if prefix == '106':
        return 'NYSE'
    if prefix == '107':
        return 'AMEX'
    return 'OTC'


def get_security_attributes(yf_info: dict, em_prefix: str) -> tuple[str, AssetType]:
    """根据 yfinance 信息确定交易所和资产类型。"""
    final_exchange = yf_info.get('exchange')
    if not final_exchange:
        final_exchange = fallback_exchange_from_prefix(em_prefix)
        logger.warning(
            f"无法从 yfinance 获取交易所，回退到基于前缀 '{em_prefix}' 的判断: {final_exchange}")

    quote_type = yf_info.get('quoteType', '').upper()

    type_map = {
        'ETF': AssetType.ETF, 'INDEX': AssetType.INDEX, 'CRYPTOCURRENCY': AssetType.CRYPTO,
        'CURRENCY': AssetType.FOREX, 'WARRANT': AssetType.WARRANT, 'PREFERRED_STOCK': AssetType.PREFERRED_STOCK,
        'MUTUALFUND': AssetType.MUTUAL_FUND,
    }
    if quote_type in type_map:
        return final_exchange, type_map[quote_type]

    if quote_type == 'EQUITY':
        if final_exchange and final_exchange.upper() in ['PNK', 'OQB', 'OQX', 'OTCMKTS']:
            return final_exchange, AssetType.OTC
        return final_exchange, AssetType.STOCK

    logger.warning(f"遇到未知的 yfinance quoteType: '{quote_type}'. 默认归类为 STOCK。")
    return final_exchange, AssetType.STOCK


def import_stocks(db_manager: DatabaseManager, yfinance_source: YFinanceSource, file_path: str):
    """
    主导入逻辑：读取JSON，过滤数据，解析代码，使用 yfinance 丰富数据，并存入数据库。
    """
    if not os.path.exists(file_path):
        logger.error(f"JSON文件未找到: {file_path}")
        return

    with open(file_path, 'r', encoding='utf-8') as f:
        stock_list = json.load(f)

    logger.info(f"从 {file_path} 加载了 {len(stock_list)} 条原始股票记录。")

    with db_manager.get_session() as session:
        for i, stock_data in enumerate(stock_list):
            em_code = stock_data.get('代码')
            if not em_code:
                logger.warning(f"记录 {i + 1} 缺少 '代码' 字段，跳过。")
                continue

            # --- NEW: 数据过滤规则 ---
            # 规则 1: 过滤带 _ws 的权证数据
            # if '_ws' in em_code.lower():
            #     logger.trace(f"过滤掉带 '_ws' 的代码: {em_code}")
            #     continue

            # if '_p' in em_code.lower():
            #     logger.trace(f"过滤掉带 '_p' 的代码: {em_code}")
            #     continue

            # 规则 2: 过滤开盘价为 null 的数据
            if stock_data.get('开盘价') is None:
                logger.trace(f"过滤掉开盘价为 null 的代码: {em_code}")
                continue
            # --- 过滤结束 ---

            logger.info(f"--- 处理第 {i + 1}/{len(stock_list)} 条: {em_code} ---")

            existing_security = session.query(Security).filter(
                Security.em_code == em_code).first()
            if existing_security:
                logger.info(
                    f"em_code '{em_code}' 已存在于数据库 (ID: {existing_security.id})，跳过。")
                continue

            yfinance_symbol, em_prefix = parse_em_us_code(em_code)
            if not yfinance_symbol:
                continue

            # 使用新的带重试逻辑的函数
            yf_info = get_yfinance_info_with_retry(
                yfinance_source, yfinance_symbol)

            final_exchange, final_asset_type = get_security_attributes(
                yf_info, em_prefix)

            if yf_info.get('quoteType', '').upper() in ['OPTION', 'FUTURE']:
                logger.info(
                    f"跳过不支持的资产类型 '{yf_info.get('quoteType')}' for symbol {yfinance_symbol}.")
                continue

            new_security_data = {
                'symbol': yfinance_symbol.lower(), 'em_code': em_code,
                'name': yf_info.get('longName') or yf_info.get('shortName') or stock_data.get('名称'),
                'market': MarketType.US, 'exchange': final_exchange.upper(),
                'type': final_asset_type, 'currency': yf_info.get('currency'),
                'sector': yf_info.get('sector'), 'industry': yf_info.get('industry'),
                'list_date': yf_info.get('list_date'),
                'is_active': stock_data.get('最新价') is not None and yf_info.get('regularMarketPrice') is not None
            }
            logger.debug(f"准备创建 Security 对象，数据: {new_security_data}")
            try:
                new_security = Security(**new_security_data)
                session.add(new_security)
                session.commit()
                logger.success(
                    f"成功插入: {new_security.symbol} (exchange: {new_security.exchange}, type: {new_security.type.name})")
            except IntegrityError as e:
                session.rollback()
                logger.error(
                    f"插入 {em_code} 时发生完整性错误 (可能 symbol+market+type 冲突): {e}")
            except Exception as e:
                session.rollback()
                logger.critical(f"插入 {em_code} 时发生未知错误: {e}", exc_info=True)


def main():
    """脚本主入口"""
    setup_logging()

    try:
        db_manager = DatabaseManager()
        db_manager.create_tables()
        yfinance_source = YFinanceSource()
        import_stocks(db_manager, yfinance_source, JSON_FILE_PATH)
    except Exception as e:
        logger.critical(f"脚本执行失败: {e}", exc_info=True)
    finally:
        logger.info("脚本执行完毕。")


if __name__ == "__main__":
    main()
