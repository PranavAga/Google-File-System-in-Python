from concurrent import futures
import threading
import time
import os

import grpc
import gfs_pb2_grpc
import gfs_pb2

from common import Config as cfg
from common import Status

class ChunkServer(gfs_pb2_grpc.ChunkServerServicer):
    def __init__(self, port):
        self.port = port
        self.chunkserver_id = port
        self.root = os.path.join(cfg.chunkserver_root, self.port)
        os.makedirs(self.root, exist_ok=True)
        self.chunks = {}  # Map chunk_handle to ChunkData
        self.lock = threading.Lock()
        self.master_stub = self.connect_to_master()
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def connect_to_master(self):
        channel = grpc.insecure_channel(f'localhost:{cfg.master_loc}')
        stub = gfs_pb2_grpc.MasterServerStub(channel)
        return stub

    def send_heartbeat(self):
        while True:
            with self.lock:
                chunk_handles = list(self.chunks.keys())
            heartbeat_request = gfs_pb2.HeartbeatRequest(chunkserver_id=self.chunkserver_id, chunks=chunk_handles)
            try:
                response = self.master_stub.Heartbeat(heartbeat_request)
                if response.invalid_chunks:
                    # Handle invalid chunks (e.g., discard, update version)
                    pass
            except Exception as e:
                print(f"Heartbeat failed: {e}")
            time.sleep(cfg.heartbeat_interval)

    def PushData(self, request, context):
        chunk_handle = request.chunk_handle
        data = request.data
        version = request.version
        with self.lock:
            # Update chunk data and version
            chunk_path = os.path.join(self.root, chunk_handle)
            with open(chunk_path, 'ab') as f:
                f.write(data)
            self.chunks[chunk_handle] = (chunk_path, version)
        return gfs_pb2.String(st="SUCCESS")

    def ReadData(self, request, context):
        chunk_handle = request.st
        with self.lock:
            if chunk_handle not in self.chunks:
                return gfs_pb2.ChunkData(chunk_handle=chunk_handle, data=b'', version=0)
            chunk_path, version = self.chunks[chunk_handle]
            with open(chunk_path, 'rb') as f:
                data = f.read()
            return gfs_pb2.ChunkData(chunk_handle=chunk_handle, data=data, version=version)

    def ReplicateChunk(self, request, context):
        chunk_handle = request.chunk_handle
        data = request.data
        version = request.version
        with self.lock:
            chunk_path = os.path.join(self.root, chunk_handle)
            with open(chunk_path, 'wb') as f:
                f.write(data)
            self.chunks[chunk_handle] = (chunk_path, version)
        return gfs_pb2.String(st="SUCCESS")

def serve():
    import sys
    if len(sys.argv) > 1:
        port = sys.argv[1]
    else:
        port = cfg.chunkserver_locs[0]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chunkserver = ChunkServer(port)
    gfs_pb2_grpc.add_ChunkServerServicer_to_server(chunkserver, server)
    server.add_insecure_port('[::]:{}'.format(port))
    server.start()
    print(f"Chunkserver started on port {port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()