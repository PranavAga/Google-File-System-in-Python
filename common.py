import threading
import logging
import os

class Config:
    chunk_size = 64 * 1024 * 1024  # 64MB
    master_port = '50051'
    chunkserver_ports = ['50052', '50053', '50054', '50055', '50056']
    chunkserver_root = 'root_chunkserver'
    replication_factor = 3
    heartbeat_interval = 5  # seconds
    lease_duration = 60  # seconds

    # Logging configuration
    log_file = 'gfs.log'
    log_level = logging.DEBUG  # Set to logging.INFO in production

# Set up logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=Config.log_level,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join('logs', Config.log_file)),
        logging.StreamHandler()
    ]
)

def get_logger(name):
    return logging.getLogger(name)

def singleton(cls):
    instances = {}
    lock = threading.Lock()

    def wrapper(*args, **kwargs):
        with lock:
            if cls not in instances:
                instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper