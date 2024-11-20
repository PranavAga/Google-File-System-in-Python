import threading
import logging
import sys

class Config:
    chunk_size = 64 * 1024 * 1024  # 64MB
    master_port = '50051'
    chunkserver_ports = ['50052', '50053', '50054', '50055', '50056']
    chunkserver_root = 'root_chunkserver'
    replication_factor = 3
    heartbeat_interval = 5  # seconds
    lease_duration = 60  # seconds
    max_chunkserver_failures = 2  # for consensus algorithms

# Thread-safe singleton decorator
def singleton(cls):
    instances = {}
    lock = threading.Lock()

    def wrapper(*args, **kwargs):
        with lock:
            if cls not in instances:
                instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper

# Logging configuration with colors
class LogColors:
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'

class ColorFormatter(logging.Formatter):
    LOG_COLORS = {
        logging.DEBUG: LogColors.BLUE,
        logging.INFO: LogColors.GREEN,
        logging.WARNING: LogColors.YELLOW,
        logging.ERROR: LogColors.RED,
        logging.CRITICAL: LogColors.RED,
    }

    def format(self, record):
        log_color = self.LOG_COLORS.get(record.levelno, LogColors.GREEN)
        record.msg = f"{log_color}{record.msg}{LogColors.END}"
        return super().format(record)

def setup_logging(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    formatter = ColorFormatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger