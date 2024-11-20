import threading

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