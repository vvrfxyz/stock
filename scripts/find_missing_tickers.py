# scripts/find_missing_tickers.py

import json
import os
import sys
from loguru import logger

# --- 路径设置 ---
# 将项目根目录添加到Python路径中，以便可以导入其他模块
# __file__ -> .../scripts/find_missing_tickers.py
# os.path.dirname(__file__) -> .../scripts
# os.path.abspath(os.path.join(..., '..')) -> .../ (项目根目录)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
# --- 路径设置结束 ---

# 导入项目内的模块
from db_manager import DatabaseManager

# --- 配置区 ---
# 输入的JSON文件名（假设在项目根目录下）
INPUT_JSON_FILENAME = "polygon_20250625.json"
# 输出的TXT文件名（将保存在项目根目录下）
OUTPUT_TXT_FILENAME = "missing_tickers_from_polygon.txt"

# 构造文件的完整路径
INPUT_JSON_PATH = os.path.join(project_root, INPUT_JSON_FILENAME)
OUTPUT_TXT_PATH = os.path.join(project_root, OUTPUT_TXT_FILENAME)


def setup_logging():
    """配置 Loguru 日志记录器，与其他脚本保持风格一致。"""
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(os.path.join(log_dir, "find_missing_{time}.log"), rotation="10 MB", retention="10 days", level="DEBUG")
    logger.info("日志记录器设置完成。")


def main():
    """主执行函数"""
    setup_logging()

    # 1. 检查并读取 JSON 文件
    logger.info(f"正在读取JSON文件: {INPUT_JSON_PATH}")
    if not os.path.exists(INPUT_JSON_PATH):
        logger.error(f"输入文件未找到: {INPUT_JSON_PATH}")
        logger.error("请确保 'demo.py' 或其他方式已生成该文件，并放置在项目根目录下。")
        return

    try:
        with open(INPUT_JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"解析JSON文件失败: {e}")
        return
    except Exception as e:
        logger.error(f"读取文件时发生未知错误: {e}")
        return

    if not isinstance(data, list):
        logger.error("JSON文件格式不正确，期望的顶层结构是一个列表 (list)。")
        return

    # 2. 从JSON数据中提取 Tickers
    # 假设 'demo.py' 生成的JSON中，ticker的键是 'T' (根据polygon API)
    # 或者是 'ticker' (根据 vars(agg) 的行为)。我们会同时检查这两个键。
    # 使用 set 来自动去重
    tickers_from_json = set()
    for item in data:
        if isinstance(item, dict):
            # Polygon API v3 `get_grouped_daily_aggs` 的结果中，ticker的键是 'T'
            ticker = item.get('T') or item.get('ticker')
            if ticker:
                tickers_from_json.add(ticker.upper())  # 统一转为大写以便比较

    if not tickers_from_json:
        logger.warning("在JSON文件中没有找到任何有效的 ticker。程序结束。")
        return

    logger.success(f"从JSON文件中成功提取了 {len(tickers_from_json)} 个唯一的 tickers。")

    # 3. 连接数据库并检查 Tickers
    db_manager = None
    missing_tickers = []
    try:
        db_manager = DatabaseManager()
        logger.info("数据库连接成功，开始检查 tickers 是否存在...")

        # 遍历从JSON文件中获取的每一个ticker
        for i, ticker in enumerate(sorted(list(tickers_from_json))):
            # 将 ticker 转换为小写，因为您的数据库似乎使用小写 symbol
            symbol_to_check = ticker.lower()

            # 使用 db_manager 中的方法查询数据库
            # get_security_by_symbol 是一个非常直接的方法
            security = db_manager.get_security_by_symbol(symbol_to_check)

            if security:
                logger.trace(
                    f"[{i + 1}/{len(tickers_from_json)}] ✅ Ticker '{ticker}' (查询为 '{symbol_to_check}') 已存在于数据库中。")
            else:
                logger.info(
                    f"[{i + 1}/{len(tickers_from_json)}] ❌ Ticker '{ticker}' (查询为 '{symbol_to_check}') 在数据库中未找到。")
                missing_tickers.append(ticker)

    except Exception as e:
        logger.critical(f"与数据库交互时发生严重错误: {e}", exc_info=True)
        return
    finally:
        if db_manager:
            db_manager.close()
            logger.info("数据库连接已关闭。")

    # 4. 将未找到的 Tickers 写入 TXT 文件
    if not missing_tickers:
        logger.success("所有来自JSON文件的 Tickers 均已存在于数据库中，无需生成记录文件。")
    else:
        logger.warning(f"共发现 {len(missing_tickers)} 个缺失的 tickers，将它们写入文件...")
        try:
            with open(OUTPUT_TXT_PATH, 'w', encoding='utf-8') as f:
                for ticker in sorted(missing_tickers):
                    f.write(f"{ticker}\n")
            logger.success(f"成功将 {len(missing_tickers)} 个缺失的 tickers 写入到: {OUTPUT_TXT_PATH}")
        except Exception as e:
            logger.error(f"写入文件时发生错误: {e}")

    logger.info("脚本执行完毕。")


if __name__ == '__main__':
    main()
