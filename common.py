import threading

class Config:
    chunk_size = 64 * 1024 * 1024  # 64MB
    master_port = '50051'
    chunkserver_ports = ['50052', '50053', '50054', '50055', '50056']
    chunkserver_root = 'root_chunkserver'
    replication_factor = 3
    heartbeat_interval = 5  # in seconds
    lease_duration = 60  # in seconds

def is_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

# Thread-safe singleton decorator for the master server
def singleton(cls):
    instances = {}
    lock = threading.Lock()

    def wrapper(*args, **kwargs):
        with lock:
            if cls not in instances:
                instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper