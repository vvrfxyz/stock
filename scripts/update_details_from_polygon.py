# scripts/update_details_from_polygon.py
import json
import os
import sys
import time
import argparse
from datetime import datetime, date, timedelta, timezone

from loguru import logger
from polygon import RESTClient
# 注意：新版 polygon-python 库可能使用 requests.exceptions 或其自定义的异常
# HTTPError 仍然是常见选择，如果遇到问题，可以检查新库的异常类型
from requests.exceptions import HTTPError

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security, MarketType, AssetType

# --- 配置区 ---
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "YOUR_POLYGON_API_KEY_HERE")
UPDATE_INTERVAL_DAYS = 30


def setup_logging():
    """配置 Loguru 日志记录器"""
    logger.remove()
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(sys.stderr, level="INFO", format=log_format)

    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(os.path.join(log_dir, "update_polygon_details_{time}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("日志记录器设置完成。")


def create_parser() -> argparse.ArgumentParser:
    """
    创建并返回 ArgumentParser 对象。
    """
    parser = argparse.ArgumentParser(
        description="使用 Polygon.io API 更新数据库中股票的详细信息。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'symbols',
        nargs='*',
        help="要更新的股票代码列表 (例如: aapl msft)。如果留空，需使用 --all 或 --market 标志。"
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help="更新数据库中所有标记为活跃的股票。"
    )
    parser.add_argument(
        '--market',
        type=str,
        help="仅更新指定市场的股票 (例如: US, HK, CNA)。"
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help=f"强制更新所有目标股票，忽略最近 {UPDATE_INTERVAL_DAYS} 天内的更新检查。"
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=12.0,  # Polygon免费版每分钟5次API调用限制，12秒延迟是安全的
        help="每次API请求之间的延迟秒数。默认为 12.0 秒。"
    )
    return parser


def map_polygon_market(locale: str) -> MarketType | None:
    """将 Polygon 的 locale 映射到我们的 MarketType 枚举"""
    if not locale: return None
    locale_upper = locale.upper()
    # Polygon 的 'us' locale 可能涵盖全球多种资产，这里根据需要映射
    if locale_upper == 'US': return MarketType.US
    if locale_upper == 'GLOBAL': return MarketType.US
    return None


def map_polygon_asset_type(pg_type: str) -> AssetType | None:
    """将 Polygon 的 type 映射到我们的 AssetType 枚举"""
    if not pg_type: return None
    type_map = {
        'CS': AssetType.STOCK,
        'ETF': AssetType.ETF,
        'ETN': AssetType.ETF, # ETN 也归类为 ETF
        'WARRANT': AssetType.WARRANT,
        'INDEX': AssetType.INDEX,
        'MUTUAL FUND': AssetType.MUTUAL_FUND,
        'PREFERRED STOCK': AssetType.PREFERRED_STOCK,
        # 新增对其他类型的映射，以增强兼容性
        'ADRC': AssetType.STOCK, # 美国存托凭证
    }
    # 使用 .get() 进行安全查找
    mapped_type = type_map.get(pg_type.upper())
    if not mapped_type:
        logger.warning(f"遇到未知的 Polygon asset type: '{pg_type}', 将其归类为 STOCK。")
        return AssetType.STOCK
    return mapped_type


def parse_date_string(date_str: str) -> date | None:
    """安全地将 YYYY-MM-DD 格式的字符串解析为 date 对象"""
    if not date_str: return None
    try:
        # Polygon API 返回的日期通常不带时区信息，或为UTC 'Z'
        if 'Z' in date_str:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
        return date.fromisoformat(date_str)
    except (ValueError, TypeError) as e:
        logger.error(f"无法解析日期字符串: '{date_str}', 错误: {e}")
        return None


def update_security_details(db_manager: DatabaseManager, polygon_client: RESTClient, security: Security, force: bool):
    """为单个股票获取数据并更新数据库"""
    symbol = security.symbol
    logger.info(f"--- 开始处理: {symbol} ---")

    if not force and security.info_last_updated_at:
        # 确保时区感知比较
        last_update_aware = security.info_last_updated_at.astimezone(timezone.utc)
        if last_update_aware > (datetime.now(timezone.utc) - timedelta(days=UPDATE_INTERVAL_DAYS)):
            logger.info(f"[{symbol}] 的信息在 {UPDATE_INTERVAL_DAYS} 天内已更新，跳过。")
            return

    try:
        logger.debug(f"正在为 {symbol.upper()} 调用 Polygon Ticker Details API...")
        details_response = polygon_client.get_ticker_details(symbol.upper())

        # ==================================================================
        # === 新增调试代码：打印从 API 收到的对象为 JSON ===
        # ==================================================================
        try:
            # vars() 将模型对象的大部分属性转换为字典
            # 注意：这可能不适用于所有复杂的嵌套结构，但对于调试通常足够
            response_dict = vars(details_response)

            # 使用 json.dumps 进行格式化输出
            # default=str 用于处理 date/datetime 等非原生JSON类型
            pretty_json = json.dumps(response_dict, indent=4, default=str)

            logger.info(f"--- 收到来自 Polygon 的 TickerDetails 对象 (JSON格式化) for '{symbol}': ---\n"
                        f"{pretty_json}\n"
                        f"--- END OF TickerDetails OBJECT ---")
        except Exception as json_e:
            logger.error(f"无法将 Polygon 响应对象序列化为 JSON: {json_e}")
            # 如果序列化失败，尝试直接打印对象
            logger.info(f"尝试直接打印原始对象: {details_response}")
        # --- 调试代码结束 ---

    except HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"[{symbol}] 在 Polygon API 中未找到 (404)。将标记为不活跃。")
            db_manager.upsert_security_info({'symbol': symbol, 'is_active': False})
        else:
            logger.error(f"[{symbol}] 请求 Polygon API 时发生 HTTP 错误: {e.response.status_code} - {e}")
        return
    except Exception as e:
        logger.error(f"[{symbol}] 请求 Polygon API 时发生未知错误: {e}", exc_info=True)
        return

    # ==================================================================
    # === 核心修改 2: 直接从模型对象属性获取数据 ===
    # details_response 现在是一个 TickerDetails 对象，不再是字典。
    # 我们直接访问其属性，例如 details_response.name。
    # 对于嵌套对象（如 address, branding），先检查其是否存在。
    # ==================================================================
    address = getattr(details_response, 'address', None)
    branding = getattr(details_response, 'branding', None)
    update_data = {
        'symbol': symbol.lower(),
        'name': getattr(details_response, 'name', None),
        'exchange': getattr(details_response, 'primary_exchange', None),
        'currency': getattr(details_response, 'currency_name', None),
        'is_active': getattr(details_response, 'active', False),
        'list_date': parse_date_string(getattr(details_response, 'list_date', None)),
        'delist_date': parse_date_string(getattr(details_response, 'delisted_utc', None)),
        'cik': getattr(details_response, 'cik', None),
        'composite_figi': getattr(details_response, 'composite_figi', None),
        'share_class_figi': getattr(details_response, 'share_class_figi', None),
        'market_cap': getattr(details_response, 'market_cap', None),
        'phone_number': getattr(details_response, 'phone_number', None),
        'description': getattr(details_response, 'description', None),
        'homepage_url': getattr(details_response, 'homepage_url', None),
        'total_employees': getattr(details_response, 'total_employees', None),
        'sic_code': getattr(details_response, 'sic_code', None),
        'industry': getattr(details_response, 'sic_description', None),
        'address_line1': getattr(address, 'address1', None) if address else None,
        'city': getattr(address, 'city', None) if address else None,
        'state': getattr(address, 'state', None) if address else None,
        'postal_code': getattr(address, 'postal_code', None) if address else None,
        'logo_url': getattr(branding, 'logo_url', None) if branding else None,
        'icon_url': getattr(branding, 'icon_url', None) if branding else None,
    }
    # --- 核心修改：智能处理 market 和 type 字段 ---

    # 1. 处理 market 字段
    market_from_api = map_polygon_market(getattr(details_response, 'locale', None))
    # 如果 API 提供了有效值，就使用它；否则，保留数据库中原有的值。
    # `security.market` 是从数据库查询得到的原始值。
    update_data['market'] = market_from_api if market_from_api is not None else security.market
    # 2. 处理 type 字段
    type_from_api = map_polygon_asset_type(getattr(details_response, 'type', None))
    # 如果 API 提供了有效值，就使用它；否则，保留数据库中原有的值。
    # `security.type` 是从数据库查询得到的原始值。
    update_data['type'] = type_from_api if type_from_api is not None else security.type

    # --- 核心修改结束 ---
    # 移除所有值为 None 的键，避免用 None 覆盖数据库中已有的值
    # 注意：这里的 is_active=False 是需要保留的，所以我们不能简单地移除所有 None
    # 我们只移除那些我们不希望用 None 覆盖的字段。
    # 一个更安全的做法是，在构建字典时就避免加入 None 值。让我们重构一下。
    # (重构后的代码)
    address = getattr(details_response, 'address', None)
    branding = getattr(details_response, 'branding', None)
    # 初始的、保证存在的字段
    update_data = {
        'symbol': symbol.lower(),
        'is_active': getattr(details_response, 'active', False),  # is_active 需要特殊处理
    }
    # 动态添加非 None 的字段
    potential_updates = {
        'name': getattr(details_response, 'name', None),
        'exchange': getattr(details_response, 'primary_exchange', None),
        'currency': getattr(details_response, 'currency_name', None),
        'list_date': parse_date_string(getattr(details_response, 'list_date', None)),
        'delist_date': parse_date_string(getattr(details_response, 'delisted_utc', None)),
        'cik': getattr(details_response, 'cik', None),
        'composite_figi': getattr(details_response, 'composite_figi', None),
        'share_class_figi': getattr(details_response, 'share_class_figi', None),
        'market_cap': getattr(details_response, 'market_cap', None),
        'phone_number': getattr(details_response, 'phone_number', None),
        'description': getattr(details_response, 'description', None),
        'homepage_url': getattr(details_response, 'homepage_url', None),
        'total_employees': getattr(details_response, 'total_employees', None),
        'sic_code': getattr(details_response, 'sic_code', None),
        'industry': getattr(details_response, 'sic_description', None),
        'address_line1': getattr(address, 'address1', None) if address else None,
        'city': getattr(address, 'city', None) if address else None,
        'state': getattr(address, 'state', None) if address else None,
        'postal_code': getattr(address, 'postal_code', None) if address else None,
        'logo_url': getattr(branding, 'logo_url', None) if branding else None,
        'icon_url': getattr(branding, 'icon_url', None) if branding else None,
    }
    for key, value in potential_updates.items():
        if value is not None:
            update_data[key] = value
    # --- 智能处理 market 和 type 字段 ---
    market_from_api = map_polygon_market(getattr(details_response, 'locale', None))
    update_data['market'] = market_from_api or security.market
    type_from_api = map_polygon_asset_type(getattr(details_response, 'type', None))
    update_data['type'] = type_from_api or security.type
    # --- 智能处理结束 ---
    if not update_data:
        logger.warning(f"[{symbol}] 从 API 获取的数据为空或无法解析，无法更新。")
        return

    try:

        # ==================================================================
        # === 新增调试代码：打印准备写入数据库的字典 ===
        # ==================================================================
        try:
            # 将枚举类型转换为字符串，以便JSON序列化
            db_data_to_print = {
                key: value.name if isinstance(value, (MarketType, AssetType)) else value
                for key, value in update_data.items()
            }
            db_json = json.dumps(db_data_to_print, indent=4, default=str)
            logger.info(f"--- 准备写入数据库的 Security 数据 (JSON格式化) for '{symbol}': ---\n"
                        f"{db_json}\n"
                        f"--- END OF Security DATA ---")
        except Exception as json_e:
            logger.error(f"无法将待写入数据库的字典序列化为 JSON: {json_e}")
        # --- 调试代码结束 ---

        db_manager.upsert_security_info(update_data)
        logger.success(f"成功更新了 [{symbol}] 的详细信息。")
    except Exception as e:
        logger.error(f"[{symbol}] 更新数据库时出错: {e}", exc_info=True)


def main():
    """脚本主入口"""
    setup_logging()

    parser = create_parser()
    args = parser.parse_args()

    if not any([args.symbols, args.all, args.market]):
        logger.warning("没有指定任何操作。请提供股票代码，或使用 --all / --market 标志。")
        parser.print_help()
        return

    if POLYGON_API_KEY == "YOUR_POLYGON_API_KEY_HERE":
        logger.error("请在环境变量中设置 POLYGON_API_KEY。")
        return

    db_manager = None
    try:
        db_manager = DatabaseManager()
        db_manager.create_tables()

        polygon_client = RESTClient(POLYGON_API_KEY)

        securities_to_process = []
        with db_manager.get_session() as session:
            query = session.query(Security).filter(Security.is_active == True)
            if args.symbols:
                symbols_lower = [s.lower() for s in args.symbols]
                query = query.filter(Security.symbol.in_(symbols_lower))
                logger.info(f"指定模式：将处理 {len(symbols_lower)} 个股票。")
            elif args.market:
                try:
                    market_enum = MarketType[args.market.upper()]
                    query = query.filter(Security.market == market_enum)
                    logger.info(f"市场模式：将处理 {market_enum.name} 市场的所有活跃股票。")
                except KeyError:
                    logger.error(f"无效的市场代码: '{args.market}'. 可用代码: {[m.name for m in MarketType]}")
                    return
            elif args.all:
                logger.info("全量模式：将处理数据库中所有活跃股票。")

            # 优先处理从未更新过信息的股票
            query = query.order_by(Security.info_last_updated_at.asc().nulls_first())
            securities_to_process = query.all()

        if not securities_to_process:
            logger.success("根据条件，没有找到需要处理的股票。")
            return

        logger.info(f"共找到 {len(securities_to_process)} 支股票待处理。")

        total = len(securities_to_process)
        for i, security in enumerate(securities_to_process):
            logger.info(f"进度: {i + 1}/{total}")
            update_security_details(db_manager, polygon_client, security, force=args.force)

            if i < total - 1:
                logger.trace(f"等待 {args.delay} 秒...")
                time.sleep(args.delay)

    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("脚本执行完毕。")


if __name__ == "__main__":
    main()
