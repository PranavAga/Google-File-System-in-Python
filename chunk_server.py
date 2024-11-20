import grpc
from concurrent import futures
import threading
import time
import os
import sys

import gfs_pb2
import gfs_pb2_grpc

from common import Config as cfg
from common import Status

class ChunkServerServicer(gfs_pb2_grpc.ChunkServerServicer):
    def __init__(self, port):
        self.port = port
        self.server_id = port
        self.chunks = {}  # Map chunk_handle to (data, version)
        self.chunk_lock = threading.Lock()
        self.root_dir = os.path.join(cfg.chunkserver_root, self.server_id)
        os.makedirs(self.root_dir, exist_ok=True)

        # Connect to master
        self.connect_to_master()
        self.start_heartbeat()

    def connect_to_master(self):
        self.master_channel = grpc.insecure_channel(f'localhost:{cfg.master_loc}')
        self.master_stub = gfs_pb2_grpc.MasterServerStub(self.master_channel)

    def start_heartbeat(self):
        threading.Thread(target=self.send_heartbeat, daemon=True).start()

    def send_heartbeat(self):
        while True:
            try:
                with self.chunk_lock:
                    chunk_list = list(self.chunks.keys())
                request = gfs_pb2.HeartbeatRequest(
                    chunkserver_id=self.server_id,
                    chunks=chunk_list
                )
                response = self.master_stub.Heartbeat(request)
                if response.success:
                    print(f"[{self.server_id}] Heartbeat sent successfully")
                else:
                    print(f"[{self.server_id}] Heartbeat failed")
            except Exception as e:
                print(f"[{self.server_id}] Error sending heartbeat: {e}")
            time.sleep(cfg.heartbeat_interval)

    def PushData(self, request, context):
        chunk_handle = request.chunk_handle
        data = request.data
        version = request.version

        with self.chunk_lock:
            # Update chunk data and version
            self.chunks[chunk_handle] = (data, version)
            # Save data to disk
            chunk_path = os.path.join(self.root_dir, chunk_handle)
            with open(chunk_path, 'wb') as f:
                f.write(data)
            print(f"[{self.server_id}] Received data for chunk {chunk_handle}")
        return gfs_pb2.StringResponse(message="SUCCESS")

    def ReadData(self, request, context):
        chunk_handle = request.chunk_handle
        with self.chunk_lock:
            if chunk_handle not in self.chunks:
                return gfs_pb2.ChunkData(
                    chunk_handle=chunk_handle,
                    data=b'',
                    version=0
                )
            data, version = self.chunks[chunk_handle]
            print(f"[{self.server_id}] Reading data for chunk {chunk_handle}")
        return gfs_pb2.ChunkData(
            chunk_handle=chunk_handle,
            data=data,
            version=version
        )

    def ReplicateChunk(self, request, context):
        chunk_handle = request.chunk_handle
        data = request.data
        version = request.version

        with self.chunk_lock:
            # Save replicated data
            self.chunks[chunk_handle] = (data, version)
            chunk_path = os.path.join(self.root_dir, chunk_handle)
            with open(chunk_path, 'wb') as f:
                f.write(data)
            print(f"[{self.server_id}] Replicated chunk {chunk_handle}")
        return gfs_pb2.StringResponse(message="REPLICATED")

def serve():
    if len(sys.argv) != 2:
        print("Usage: python chunk_server.py <port>")
        sys.exit(1)
    port = sys.argv[1]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ChunkServerServicer(port)
    gfs_pb2_grpc.add_ChunkServerServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Chunk Server started on port {port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)
        print("Chunk Server stopped")

if __name__ == '__main__':
    serve()