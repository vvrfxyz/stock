import time
import threading
import collections
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple
from loguru import logger


@dataclass
class _RateLimiterState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    history: Dict[str, Deque[float]] = field(default_factory=dict)
    blocked_until: Dict[str, float] = field(default_factory=dict)
    rr_index: int = 0
    global_blocked_until: float = 0.0


_GLOBAL_STATE_LOCK = threading.Lock()
_GLOBAL_STATES: Dict[Tuple[str, int, int], _RateLimiterState] = {}

# 进程内累计限速等待秒数（跨线程求和，只增不减）。
# 消费方：massive_task.run_concurrently 的非 TTY 进度行用它算 rate-wait 占比，
# 一眼区分"配额慢"（该加 key/缩范围）与"IO 慢"（该查网络/vendor）。
# 语义诚实标注：限速器状态是 per-key 进程共享的，此计数只覆盖【本进程】
# 线程的等待，是全局配额压力的下界，不是全局值。
_WAITED_LOCK = threading.Lock()
_WAITED_SECONDS = 0.0


def waited_seconds() -> float:
    """本进程迄今在限速等待上花掉的累计秒数（全部线程求和）。"""
    with _WAITED_LOCK:
        return _WAITED_SECONDS


def _record_wait(seconds: float) -> None:
    global _WAITED_SECONDS
    with _WAITED_LOCK:
        _WAITED_SECONDS += seconds


class KeyRateLimiter:
    """
    一个线程安全的API Key速率限制器和调度器。
    它确保每个key的使用频率不超过指定的速率。
    """

    def __init__(self, keys: List[str], rate_limit: int, per_seconds: int, *, scope: str = "default"):
        """
        初始化速率限制器。
        :param keys: API Key列表。
        :param rate_limit: 在指定时间内的最大请求数 (例如: 5)。
        :param per_seconds: 时间窗口的秒数 (例如: 60)。
        """
        if not keys:
            raise ValueError("API Key列表不能为空。")
        unique_keys = list(dict.fromkeys(key.strip() for key in keys if key and key.strip()))
        if not unique_keys:
            raise ValueError("API Key列表不能为空。")
        scope = (scope or "default").strip()
        if not scope:
            scope = "default"

        self.keys = unique_keys
        self.rate_limit = int(rate_limit)
        self.per_seconds = int(per_seconds)
        self.scope = scope

        config_key = (self.scope, self.rate_limit, self.per_seconds)
        with _GLOBAL_STATE_LOCK:
            state = _GLOBAL_STATES.get(config_key)
            if state is None:
                state = _RateLimiterState()
                _GLOBAL_STATES[config_key] = state
        self._state = state

        # 将 key 注入共享 state，确保跨脚本/跨步骤复用同一份“最近请求历史”。
        with self._state.lock:
            for key in self.keys:
                if key not in self._state.history:
                    self._state.history[key] = collections.deque(maxlen=self.rate_limit)
                self._state.blocked_until.setdefault(key, 0.0)

        # 兼容旧字段（外部若有调试引用）
        self.history = self._state.history
        self.lock = self._state.lock
        logger.info(
            f"KeyRateLimiter 初始化成功(scope={self.scope}): {len(self.keys)}个Key, "
            f"每个Key限制为 {rate_limit}次 / {per_seconds}秒。"
        )

    def block_key(self, api_key: str, duration_seconds: float) -> None:
        """
        将某个 key 标记为临时不可用（例如收到 429 后）。
        仅影响当前进程内的后续请求调度。
        """
        if not api_key:
            return
        duration_seconds = max(0.0, float(duration_seconds))
        if duration_seconds <= 0:
            return
        now = time.monotonic()
        with self._state.lock:
            current_until = self._state.blocked_until.get(api_key, 0.0)
            self._state.blocked_until[api_key] = max(current_until, now + duration_seconds)

    def block_all(self, duration_seconds: float) -> None:
        """
        将整个 scope 的所有 key 暂时标记为不可用（例如遇到账户级 429 或服务端要求 Retry-After）。
        """
        duration_seconds = max(0.0, float(duration_seconds))
        if duration_seconds <= 0:
            return
        now = time.monotonic()
        with self._state.lock:
            self._state.global_blocked_until = max(self._state.global_blocked_until, now + duration_seconds)

    def acquire_key(self) -> str:
        """
        获取一个当前可用的API Key。
        如果所有key都在冷却中，此方法将阻塞并等待，直到有key可用。
        """
        while True:
            with self._state.lock:
                now = time.monotonic()

                if not self.keys:
                    raise ValueError("API Key列表不能为空。")

                global_wait = max(0.0, self._state.global_blocked_until - now)
                if global_wait > 0:
                    wait_duration = global_wait + 0.01
                    wait_reason = "global throttle"
                else:
                    best_wait_time = float("inf")
                    best_key_index = 0

                    # 从 rr_index 开始扫描，避免所有线程永远打在第一个 key 上。
                    start_index = self._state.rr_index % len(self.keys)
                    for offset in range(len(self.keys)):
                        idx = (start_index + offset) % len(self.keys)
                        key = self.keys[idx]
                        key_history = self._state.history[key]

                        # 清理过期的请求时间戳，降低误判风险（maxlen 很小，成本可忽略）。
                        while key_history and (now - key_history[0]) >= self.per_seconds:
                            key_history.popleft()

                        blocked_until = self._state.blocked_until.get(key, 0.0)
                        block_wait = max(0.0, blocked_until - now)

                        if len(key_history) < self.rate_limit and block_wait <= 0:
                            key_history.append(now)
                            self._state.rr_index = idx + 1
                            logger.trace(f"线程 {threading.get_ident()} 获取到Key: ...{key[-4:]}")
                            return key

                        # 计算该 key 的最短等待时间（被 block 或者速率窗口未释放）。
                        window_wait = 0.0
                        if len(key_history) >= self.rate_limit:
                            oldest_request_time = key_history[0]
                            window_wait = max(0.0, (oldest_request_time + self.per_seconds) - now)
                        wait_time = max(block_wait, window_wait)

                        if wait_time < best_wait_time:
                            best_wait_time = wait_time
                            best_key_index = idx

                    # 没有立即可用的 key：在锁外 sleep，避免阻塞其它线程更新状态。
                    wait_duration = max(0.0, best_wait_time) + 0.01
                    wait_reason = "keys cooling"
                    # 下一次优先从更可能解锁的 key 附近开始扫描，提高命中概率。
                    self._state.rr_index = best_key_index + 1

            logger.trace(
                "KeyRateLimiter 等待({})，线程 {} 将等待 {:.2f} 秒。",
                wait_reason,
                threading.get_ident(),
                wait_duration,
            )
            time.sleep(wait_duration)
            _record_wait(wait_duration)
