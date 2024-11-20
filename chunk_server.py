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
        self.chunk_dir = os.path.join(Config.chunkserver_root, self.chunkserver_id)
        os.makedirs(self.chunk_dir, exist_ok=True)
        self.chunks = {}  # chunk_handle -> version
        self.locks = {}  # chunk_handle -> threading.Lock()
        self.master_channel = grpc.insecure_channel(f'localhost:{Config.master_port}')
        self.master_stub = gfs_pb2_grpc.MasterServerStub(self.master_channel)
        threading.Thread(target=self._send_heartbeat, daemon=True).start()

    def _send_heartbeat(self):
        while True:
            time.sleep(Config.heartbeat_interval)
            chunks = []
            for chunk_handle, version in self.chunks.items():
                chunks.append(gfs_pb2.ChunkMetadata(chunk_handle=chunk_handle, version=version))
            request = gfs_pb2.HeartbeatRequest(
                chunkserver_id=self.chunkserver_id,
                chunks=chunks
            )
            try:
                self.master_stub.Heartbeat(request)
            except Exception as e:
                pass

    def WriteChunk(self, request, context):
        chunk_handle = request.chunk_handle
        version = request.version
        data = request.data
        with self._get_chunk_lock(chunk_handle):
            current_version = self.chunks.get(chunk_handle, 0)
            if version != current_version:
                return gfs_pb2.WriteResponse(success=False, message="Version mismatch")
            temp_file = os.path.join(self.chunk_dir, f'{chunk_handle}.tmp')
            with open(temp_file, 'wb') as f:
                f.write(data)
            self.chunks[chunk_handle] = version  # Version remains same until commit
            return gfs_pb2.WriteResponse(success=True, message="Write prepared")

    def WriteCommit(self, request, context):
        chunk_handle = request.chunk_handle
        version = request.version
        with self._get_chunk_lock(chunk_handle):
            current_version = self.chunks.get(chunk_handle, 0)
            if version != current_version:
                return gfs_pb2.WriteCommitResponse(success=False, message="Version mismatch during commit")
            temp_file = os.path.join(self.chunk_dir, f'{chunk_handle}.tmp')
            final_file = os.path.join(self.chunk_dir, chunk_handle)
            if os.path.exists(temp_file):
                os.rename(temp_file, final_file)
                self.chunks[chunk_handle] = version + 1  # Increment version
                return gfs_pb2.WriteCommitResponse(success=True, message="Commit succeeded")
            else:
                return gfs_pb2.WriteCommitResponse(success=False, message="No prepared write to commit")

    def ReadChunk(self, request, context):
        chunk_handle = request.handle
        chunk_file = os.path.join(self.chunk_dir, chunk_handle)
        with self._get_chunk_lock(chunk_handle):
            if os.path.exists(chunk_file):
                with open(chunk_file, 'rb') as f:
                    data = f.read()
                return gfs_pb2.ReadResponse(data=data)
            else:
                return gfs_pb2.ReadResponse(data=b'')

    def ReplicateChunk(self, request, context):
        return self.WriteChunk(request, context)

    def _get_chunk_lock(self, chunk_handle):
        if chunk_handle not in self.locks:
            self.locks[chunk_handle] = threading.Lock()
        return self.locks[chunk_handle]

def serve():
    if len(sys.argv) != 2:
        sys.exit(1)
    port = sys.argv[1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_ChunkServerServicer_to_server(ChunkServer(port), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()