# Documentation Index

本文档目录只保留当前有效架构与运行说明。已放弃的数据源路径、旧事实表和历史复盘材料不再保留在主文档树中。

## 当前有效文档

- [因子库](./factors.md)
  研究层因子框架架构、9 个内置因子目录（size / earnings_yield / short_interest_ratio / short_volume_ratio / days_to_cover / institutional_breadth / delta_institutional_ownership / ownership_concentration / insider_net_buy）、评估命令、新增因子指南、路线图。

- [Architecture](./architecture.md)
  当前 PostgreSQL raw truth、Massive-only ingestion、复权 reference/cache。

- [证券身份生命周期](./identity_lifecycle.md)
  身份解析器（SecurityIdentityResolver）原理、rename/recycle 处理流程、存量身份修复工具。

- [数据质量 runbook](./data_quality_runbook.md)
  `health_report` 解读、退出码约定、常见故障排查。

- [Massive-only 重建与每日运行](./massive_rebuild_and_daily_run.md)
  当前推荐命令、默认 `update` 链路、全量重建方式、单项维护入口。

- [生产部署操作手册](./deployment.md)
  253 生产机的 SSH 直推同步流程、迁移与同步后固定动作、运行状态检查。

- [Debian 部署与 systemd timer](../README.debian.md)
  每天 UTC+8 10:00 的生产定时、手动触发和日志检查命令。

- [Massive 免费层日线能力](./massive_free_tier_daily_data.md)
  当前 API key 已验证可拿到的数据范围、字段用途和不纳入范围。

- [API Rate Limiting](./rate_limiting.md)
  Massive 多 key 限流、429 处理和并发注意事项。

## 归档证据

- [Massive API Audit 2026-05-14](./archive/vendor_audits/massive_stock_api_audit_2026-05-14.md)
  Vendor endpoint 能力和 key 权限边界的归档证据，不是当前 ingestion contract。

- [双库混合持久化架构方案（已归档）](./archive/polyglot_persistence_architecture.md)
  PostgreSQL + ClickHouse 的设计存档；ClickHouse 已于 2026-07 按此设计随分钟线回归（见 `minute_vw_backfill_2026-07.md`）。

## 当前事实层边界

- `daily_prices` 不保存复权价格、复权因子、换手率、成交额或技术指标。
- `corporate_actions` 是分红/拆股事件 truth。
- `vendor_adjustment_factors` 是供应商 reference，不是 truth。
- `computed_adjustment_factors` 是内部可重建 cache，不是 truth。
- `historical_shares` / `historical_floats` 是换手率计算的点时输入。
- `exchanges` 保存交易所/MIC 参考数据；`trading_calendars` 保存逐交易所逐日期 session，不能用 `exchanges` 取代。
- `sec_filings` 是 SEC filing index foundation；`insider_transactions` 和 `institutional_holdings` 分别承载 Form 3/4/5 / 13-F 明细。
- `sec_fundamental_facts` 保存 SEC XBRL 原始申报值（`filed_date` 是点时可见边界）；财务比率是读取层计算，不写回事实表。
