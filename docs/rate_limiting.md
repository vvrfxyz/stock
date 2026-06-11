# API Rate Limiting

本项目会在一个 `scheduled_update` 里连续跑多个 Massive 任务（per-symbol daily bars、short data、shares、open/close 等）。
这些任务都共享同一类限制：**Massive 免费层对每个 API key 有固定的每分钟请求数上限**，并且服务端会严格按真实时间窗口计数。

目标：

- 避免 429（Too Many Requests）导致某一步骤“看似失败但整体仍继续”的隐蔽数据缺口；
- 避免在 `main.py` 里硬编码 `sleep()`（脆弱、难调、易造成无谓等待）；
- 在多线程抓取时尽量均衡地使用 key，避免热点 key 被打满。

## KeyRateLimiter 的语义

`utils/key_rate_limiter.py` 提供 `KeyRateLimiter`：

- 线程安全：内部使用单锁保护共享状态。
- 按 key 计数：每个 key 维护最近 N 次请求的时间戳（单调时钟 `time.monotonic()`）。
- 进程内跨任务共享：同一个 `scope + rate_limit + per_seconds` 的 limiter 会共享同一份 request history，
  从而让 `scheduled_update` 的后续步骤在同一个进程内自然“续上”同一个一分钟窗口。
- Round-robin：从上次命中的 key 后继续扫描，降低“永远从第一个 key 开始”的偏置。
- 429 防抖：支持 `block_key(key, seconds)`，用于把刚触发 429 的 key 临时拉黑，避免重试再次命中同一 key。
- 账户级退避：支持 `block_all(seconds)`，用于服务端返回 `Retry-After` 时对整个 scope 做全局退避，避免“换 key 继续打”。

推荐用法（Massive）：

```python
rate_limiter = KeyRateLimiter(keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
source = MassiveSource(rate_limiter=rate_limiter)
```

## MassiveSource 的 429 处理策略

`data_sources/massive_source.py` 的 `_request_json()` 针对 429 做了两层保护：

1. 如果响应头包含 `Retry-After`，优先按该值等待/拉黑 key；
2. 若没有 `Retry-After`，按 `per_seconds`（默认 60s 窗口）保守拉黑 key，避免“一直重试一直 429”。

## 已知限制

- 共享状态仅限“同一 Python 进程内”。如果将来把各步骤改成多个独立进程启动（而不是 `main.py` 直接 import 并调用子脚本
  的 `main()`），则需要引入跨进程的 limiter（文件锁/SQLite/token bucket 等）才能保持相同行为。
- 如果 Massive 在免费层之外还存在“账户级总 RPM”限制，多 key 也无法无限扩展吞吐；此时应降低并发或升级套餐。
