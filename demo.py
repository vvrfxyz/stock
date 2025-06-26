import json
from polygon import RESTClient
import os # 建议导入os模块来处理API密钥

# --- 最佳实践：从环境变量中读取API密钥，而不是硬编码在代码里 ---
# 你可以在你的终端设置: export POLYGON_API_KEY="你的真实API密钥"
# 如果环境变量不存在，它会回退到你提供的示例密钥（请替换为你的真实密钥）
api_key = os.getenv("POLYGON_API_KEY", "3VpXypZnLq8hZDDp45pAAPaYRjBeYwqG") 

# 初始化客户端
client = RESTClient(api_key)

# 定义查询日期和输出文件名
query_date = "2025-06-25"
output_filename = f"grouped_daily_aggs_{query_date}_no_adjusted.json"

try:
    # 获取分组日线数据
    grouped_aggs = client.get_grouped_daily_aggs(
        query_date,
        adjusted="false",
    )

    # 1. 数据转换：
    # 'grouped_aggs' 是一个包含多个聚合 (Agg) 对象的迭代器。
    # 我们需要遍历它，并使用 vars() 将每个对象转换为字典，然后存入一个列表。
    # 这样 json 模块才能识别和处理它。
    data_to_save = [vars(agg) for agg in grouped_aggs]

    # 2. 将数据写入 JSON 文件：
    # 使用 'with open' 可以确保文件被正确关闭，即使发生错误。
    # 'w' 表示写入模式。
    # encoding='utf-8' 是一个好习惯，可以避免处理非英文字符时出现问题。
    with open(output_filename, "w", encoding="utf-8") as f:
        # json.dump() 将 Python 对象（这里是列表）序列化为 JSON 格式并写入文件。
        # indent=4 参数会让 JSON 文件格式化，带缩进，更易于阅读。
        # ensure_ascii=False 允许文件中包含非 ASCII 字符（如中文）。
        json.dump(data_to_save, f, indent=4, ensure_ascii=False)

    print(f"数据已成功保存到文件: {output_filename}")
    if not data_to_save:
        print("注意：API返回了空数据列表，生成的文件是空的。")

except Exception as e:
    print(f"发生错误: {e}")

