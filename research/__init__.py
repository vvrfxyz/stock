"""离线研究层：批量读取 + 向量化回测。

只读 PostgreSQL 事实表，绝不回写；复权口径与 utils/adjusted_prices 对齐
（raw_actions_v1，后复权，因子含分红与拆股）。
"""
