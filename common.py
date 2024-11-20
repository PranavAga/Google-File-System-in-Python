import threading
import time

class Config(object):
    chunk_size = 64 * 1024 * 1024  # 64MB
    master_loc = "50051"
    chunkserver_locs = ["50052", "50053", "50054", "50055", "50056"]
    chunkserver_root = "root_chunkserver"
    replication_factor = 3
    heartbeat_interval = 5  # seconds
    lease_time = 60  # seconds
    retry_interval = 2  # seconds
    chunkserver_failure_timeout = 15  # seconds for failure detection

class Status(object):
    def __init__(self, v, e):
        self.v = v
        self.e = e

def isint(e):
    try:
        int(e)
        return True
    except ValueError:
        return False

# Thread-safe singleton decorator for master server
def singleton(cls):
    instances = {}
    lock = threading.Lock()

    def wrapper(*args, **kwargs):
        with lock:
            if cls not in instances:
                instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper