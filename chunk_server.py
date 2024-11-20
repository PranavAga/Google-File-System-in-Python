import threading
import time
import os
import sys
from concurrent import futures

import grpc
import gfs_pb2
import gfs_pb2_grpc

from common import Config

class ChunkServer(gfs_pb2_grpc.ChunkServerServicer):
    def __init__(self, port):
        self.port = port
        self.chunkserver_id = port
        self.chunks = {}  # Map chunk_handle to (data, version)
        self.chunks_lock = threading.Lock()
        self.root_dir = os.path.join(Config.chunkserver_root, self.chunkserver_id)
        os.makedirs(self.root_dir, exist_ok=True)
        # Connect to master server
        self.master_channel = grpc.insecure_channel(f'localhost:{Config.master_port}')
        self.master_stub = gfs_pb2_grpc.MasterServerStub(self.master_channel)
        # Start heartbeat thread
        threading.Thread(target=self.send_heartbeats, daemon=True).start()

    def send_heartbeats(self):
        while True:
            with self.chunks_lock:
                chunk_handle_list = list(self.chunks.keys())
            try:
                request = gfs_pb2.HeartbeatRequest(
                    chunkserver_id=self.chunkserver_id,
                    chunk_handle_list=chunk_handle_list
                )
                response = self.master_stub.Heartbeat(request)
                if response.success:
                    print(f"Heartbeat sent successfully from chunkserver {self.chunkserver_id}.")
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")
            time.sleep(Config.heartbeat_interval)

    def PushChunk(self, request, context):
        chunk_handle = request.chunk_handle
        data = request.data
        version = request.version
        with self.chunks_lock:
            self.chunks[chunk_handle] = (data, version)
            # Write data to file
            chunk_path = os.path.join(self.root_dir, chunk_handle)
            with open(chunk_path, 'wb') as f:
                f.write(data)
            print(f"Chunk {chunk_handle} stored at chunkserver {self.chunkserver_id}.")
        return gfs_pb2.StringResponse(message="SUCCESS")

    def ReadChunk(self, request, context):
        chunk_handle = request.message
        with self.chunks_lock:
            if chunk_handle in self.chunks:
                data, version = self.chunks[chunk_handle]
                print(f"Chunk {chunk_handle} read from chunkserver {self.chunkserver_id}.")
                return gfs_pb2.ChunkData(
                    chunk_handle=chunk_handle,
                    data=data,
                    version=version
                )
            else:
                print(f"Chunk {chunk_handle} not found at chunkserver {self.chunkserver_id}.")
                return gfs_pb2.ChunkData(chunk_handle=chunk_handle, data=b'', version=0)

    def ReplicateChunk(self, request, context):
        chunk_handle = request.chunk_handle
        data = request.data
        version = request.version
        with self.chunks_lock:
            self.chunks[chunk_handle] = (data, version)
            # Write data to file
            chunk_path = os.path.join(self.root_dir, chunk_handle)
            with open(chunk_path, 'wb') as f:
                f.write(data)
            print(f"Chunk {chunk_handle} replicated at chunkserver {self.chunkserver_id}.")
        return gfs_pb2.StringResponse(message="REPLICATED")

def serve():
    if len(sys.argv) != 2:
        print("Usage: python chunk_server.py <port>")
        sys.exit(1)
    port = sys.argv[1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_ChunkServerServicer_to_server(ChunkServer(port), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Chunkserver started on port {port}.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("Chunkserver stopping...")
        server.stop(0)

if __name__ == '__main__':
    serve()