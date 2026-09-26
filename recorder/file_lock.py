"""合并与上传共用的文件锁，保留原有加锁和释放行为。"""

import fcntl
import os
import time
from pathlib import Path


class FileLock:
    """文件锁类，防止多个进程同时处理同一个文件"""

    def __init__(self, lock_file_path: Path, timeout: int = 300):
        self.lock_file_path = lock_file_path
        self.timeout = timeout
        self.lock_file = None

    def __enter__(self):
        """获取锁"""
        # 确保锁目录存在
        self.lock_file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            self.lock_file = open(self.lock_file_path, 'w')
            # 尝试获取排他锁
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # 写入进程信息
            self.lock_file.write(f"PID: {os.getpid()}\nTime: {time.time()}\n")
            self.lock_file.flush()
            return self
        except (OSError, IOError):
            if self.lock_file:
                self.lock_file.close()
            return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        """释放锁"""
        if self.lock_file:
            try:
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                self.lock_file.close()
                # 删除锁文件
                if self.lock_file_path.exists():
                    self.lock_file_path.unlink()
            except:
                pass
