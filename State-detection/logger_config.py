import os
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def setup_logger(log_dir, log_name_prefix):
    """
    设置日志记录器
    
    Args:
        log_dir: 日志目录路径 (Path 对象)
        log_name_prefix: 日志文件名前缀 (如 "monitor")
    """
    # 确定日志文件名
    log_file = log_dir / f"{log_name_prefix}.log"
    
    backup_dir = log_dir / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)

    # 设置定时轮转的日志处理器
    handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",      # 每天午夜切换新文件
        interval=1,           # 每 1 天轮转一次
        backupCount=7,        # 保留最近 7 天的日志
        encoding='utf-8',
        utc=False             # 按本地时间切换(UTC=False)
    )
    handler.namer = lambda name: str(backup_dir / os.path.basename(name))
    
    handler.suffix = "%Y-%m-%d"  # 文件名后缀,如 monitor_ALL.log.2025-11-11
    
    # 设置日志格式 (恢复原来的格式)
    formatter = logging.Formatter(
        "%(asctime)s [%(threadName)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    
    # 配置根 logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # 防止重复添加 handler(重启或多次调用时)
    if not root_logger.handlers:
        root_logger.addHandler(handler)
        
        # 控制台输出也要设置格式
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    logging.info("Logger initialized.")