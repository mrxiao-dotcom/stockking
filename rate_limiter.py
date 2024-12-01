import time
import logging
from threading import Lock

class RateLimiter:
    def __init__(self, calls_per_minute=180):  # tushare默认每分钟180次
        self.calls_per_minute = calls_per_minute
        self.interval = 60.0 / calls_per_minute
        self.last_call = 0
        self.lock = Lock()
        self.call_count = 0
        self.last_reset = time.time()

    def wait(self):
        with self.lock:
            current_time = time.time()

            # 检查是否需要重置计数器（每分钟重置一次）
            if current_time - self.last_reset >= 60:
                self.call_count = 0
                self.last_reset = current_time
                logging.info("API调用计数器已重置")

            # 检查是否达到限制
            if self.call_count >= self.calls_per_minute:
                sleep_time = 60 - (current_time - self.last_reset)
                if sleep_time > 0:
                    logging.info(f"达到API调用限制，等待 {sleep_time:.2f} 秒")
                    time.sleep(sleep_time)
                    self.call_count = 0
                    self.last_reset = time.time()

            # 计算需要等待的时间
            elapsed = current_time - self.last_call
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

            self.last_call = time.time()
            self.call_count += 1
            logging.info(f"当前分钟API调用次数: {self.call_count}/{self.calls_per_minute}")

# 创建全局频率限制器实例
rate_limiter = RateLimiter(calls_per_minute=170)  # 预留一些余量 