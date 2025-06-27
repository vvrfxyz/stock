# utils/key_rate_limiter.py (新建文件)
import time
import threading
import collections
from typing import List
from loguru import logger


class KeyRateLimiter:
    """
    一个线程安全的API Key速率限制器和调度器。
    它确保每个key的使用频率不超过指定的速率。
    """

    def __init__(self, keys: List[str], rate_limit: int, per_seconds: int):
        """
        初始化速率限制器。
        :param keys: API Key列表。
        :param rate_limit: 在指定时间内的最大请求数 (例如: 5)。
        :param per_seconds: 时间窗口的秒数 (例如: 60)。
        """
        if not keys:
            raise ValueError("API Key列表不能为空。")
        self.keys = keys
        self.rate_limit = rate_limit
        self.per_seconds = per_seconds
        # 为每个key创建一个双端队列，用于存储最近的请求时间戳
        # maxlen=rate_limit 自动保证队列只保留最近的N次记录
        self.history = {key: collections.deque(maxlen=self.rate_limit) for key in self.keys}
        self.lock = threading.Lock()
        logger.info(
            f"KeyRateLimiter 初始化成功: {len(keys)}个Key, "
            f"每个Key限制为 {rate_limit}次 / {per_seconds}秒。"
        )

    def acquire_key(self) -> str:
        """
        获取一个当前可用的API Key。
        如果所有key都在冷却中，此方法将阻塞并等待，直到有key可用。
        """
        while True:
            with self.lock:
                now = time.monotonic()
                best_key = None
                min_wait_time = float('inf')

                # 遍历所有key，寻找一个立即可用的
                for key in self.keys:
                    key_history = self.history[key]

                    # 条件1: key的使用次数未达到上限
                    if len(key_history) < self.rate_limit:
                        best_key = key
                        break  # 找到一个立即可用的，无需再找

                    # 条件2: key已达到上限，但最旧的请求已过冷却期
                    oldest_request_time = key_history[0]
                    if now - oldest_request_time > self.per_seconds:
                        best_key = key
                        break  # 找到一个立即可用的，无需再找

                # 如果找到了可用的key
                if best_key:
                    self.history[best_key].append(now)
                    logger.trace(f"线程 {threading.get_ident()} 获取到Key: ...{best_key[-4:]}")
                    return best_key

                # 如果没有立即可用的key，计算最短等待时间
                for key in self.keys:
                    oldest_request_time = self.history[key][0]
                    wait_time = (oldest_request_time + self.per_seconds) - now
                    if wait_time < min_wait_time:
                        min_wait_time = wait_time

            # 在锁之外等待，允许其他线程在等待期间检查状态
            wait_duration = max(0, min_wait_time) + 0.01  # 加一点点缓冲
            logger.trace(f"所有Key均在冷却中，线程 {threading.get_ident()} 将等待 {wait_duration:.2f} 秒。")
            time.sleep(wait_duration)

