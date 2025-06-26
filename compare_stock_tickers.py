import json


def clean_eastmoney_code(code: str) -> str:
    """
    清洗东方财富的股票代码。
    1. 将 '_' 替换为 '.'。
    2. 如果代码中包含'.', 并且'.'前的部分是纯数字，则移除这部分。
       例如：'105.YBZN' -> 'YBZN'
       但 'AAPL.O' -> 'AAPL.O' (因为 'AAPL' 不是纯数字)
    """
    if not code:
        return ""

    # 步骤 1: 基础清洗
    cleaned_code = code.replace('_', '.')

    # 步骤 2: 移除数字前缀
    if '.' in cleaned_code:
        parts = cleaned_code.split('.', 1)
        # 检查是否成功分割，并且前缀部分是否为纯数字
        if len(parts) == 2 and parts[0].isdigit():
            # 如果是，则返回后缀部分作为最终代码
            return parts[1]

    # 如果不满足移除条件，返回基础清洗后的代码
    return cleaned_code


def compare_stock_tickers(eastmoney_file, aggs_file, output_file):
    """
    对比两个JSON文件中的股票代码差异，并进行高级清洗。

    :param eastmoney_file: 东方财富数据JSON文件路径。
    :param aggs_file: 另一个来源的股票数据JSON文件路径。
    :param output_file: 输出对比结果的文件路径。
    """
    try:
        # 1. 读取并处理东方财富数据
        print(f"正在读取东方财富文件: {eastmoney_file}...")
        with open(eastmoney_file, 'r', encoding='utf-8') as f:
            eastmoney_data = json.load(f)

        eastmoney_tickers = set()
        initial_eastmoney_count = len(eastmoney_data)

        print("正在清洗和过滤东方财富数据...")
        for stock in eastmoney_data:
            # 过滤条件：开盘价不为null
            if stock.get("开盘价") is None:
                continue

            code = stock.get("代码")
            if code:
                # 使用新的清洗函数进行高级清洗
                final_code = clean_eastmoney_code(code)
                eastmoney_tickers.add(final_code)

        print(f"东方财富数据处理完成：原始数量 {initial_eastmoney_count}, 过滤后有效数量 {len(eastmoney_tickers)}")

        # 2. 读取并处理第二个数据源
        print(f"\n正在读取聚合数据文件: {aggs_file}...")
        with open(aggs_file, 'r', encoding='utf-8') as f:
            aggs_data = json.load(f)

        aggs_tickers = set()
        for stock in aggs_data:
            ticker = stock.get("ticker")
            if ticker:
                aggs_tickers.add(ticker)

        print(f"聚合数据处理完成：总数量 {len(aggs_tickers)}")

        # 3. 对比两个集合
        print("\n正在对比股票代码...")
        eastmoney_only = sorted(list(eastmoney_tickers - aggs_tickers))
        aggs_only = sorted(list(aggs_tickers - eastmoney_tickers))
        common_tickers = sorted(list(eastmoney_tickers & aggs_tickers))

        # 4. 准备输出结果
        result = {
            "summary": {
                "eastmoney_file": eastmoney_file,
                "eastmoney_initial_count": initial_eastmoney_count,
                "eastmoney_filtered_count": len(eastmoney_tickers),
                "aggs_file": aggs_file,
                "aggs_count": len(aggs_tickers),
                "common_tickers_count": len(common_tickers),
                "eastmoney_only_count": len(eastmoney_only),
                "aggs_only_count": len(aggs_only)
            },
            "differences": {
                "eastmoney_only_tickers": eastmoney_only,
                "aggs_only_tickers": aggs_only
            }
        }

        # 5. 将结果写入文件
        print(f"\n正在将结果写入文件: {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=4)

        print("\n--- 对比完成 ---")
        print(f"结果摘要:")
        for key, value in result["summary"].items():
            print(f"  - {key}: {value}")
        print(f"\n详细差异已保存至 {output_file}")

    except FileNotFoundError as e:
        print(f"错误: 文件未找到 - {e}")
    except json.JSONDecodeError as e:
        print(f"错误: JSON文件格式无效 - {e}")
    except Exception as e:
        print(f"发生未知错误: {e}")


if __name__ == "__main__":
    # --- 配置区 ---
    # 请将文件名替换为您的实际文件名
    EASTMONEY_JSON_FILE = "us_stock_spot_data.json"
    AGGS_JSON_FILE = "grouped_daily_aggs_2025-06-25_no_adjusted.json"
    OUTPUT_JSON_FILE = "comparison_result_v2.json"

    # 执行对比函数
    compare_stock_tickers(EASTMONEY_JSON_FILE, AGGS_JSON_FILE, OUTPUT_JSON_FILE)

