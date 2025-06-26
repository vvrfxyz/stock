import akshare as ak
import pandas as pd
import json
import os

# 定义输出文件名
output_filename = "us_stock_spot_data.json"

print("🚀 正在尝试从东方财富网获取美股实时行情数据...")

try:
    # 1. 使用 akshare 获取美股实时行情数据
    #    这个函数返回一个包含所有美股实时数据的 pandas DataFrame
    stock_us_spot_em_df = ak.stock_us_spot_em()

    print("\n✅ 数据获取成功！")
    print("数据预览 (前5条):")
    # 使用 head() 预览数据，避免在控制台打印过多内容
    print(stock_us_spot_em_df.head())

    # 2. 将 DataFrame 转换为 JSON 格式并保存到文件
    print(f"\n🔄 正在将数据转换为 JSON 并保存到文件: {output_filename}")

    # 使用 to_json 方法可以很方便地转换
    # orient='records': 将DataFrame转换为 [ {column: value}, ... ] 格式的列表。这是最常用、最直观的格式。
    # indent=4:          让JSON文件格式化，带4个空格的缩进，非常便于阅读。
    # force_ascii=False: 确保中文字符（如股票名称）能被正确写入，而不是被转义成ASCII码。
    stock_us_spot_em_df.to_json(
        output_filename,
        orient='records',
        indent=4,
        force_ascii=False
    )

    # 获取当前工作目录
    current_directory = os.getcwd()
    print(f"\n✅ 文件 '{output_filename}' 已成功保存！")
    print(f"   文件路径: {os.path.join(current_directory, output_filename)}")

except Exception as e:
    print(f"\n❌ 执行过程中发生错误: {e}")
    print("   请检查您的网络连接或 akshare 库是否为最新版本。")

