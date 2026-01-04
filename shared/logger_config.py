import logging

# 统一的日志格式
LOG_FORMAT = "%(asctime)s [%(threadName)s] %(levelname)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logger():
    """
    设置基础日志（只输出到控制台）
    日志文件由 systemd 服务控制
    """
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT
    )