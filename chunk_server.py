import threading
import time
import os
import sys
import psutil  # For load monitoring
from concurrent import futures

import grpc
import gfs_pb2
import gfs_pb2_grpc

from common import Config, get_logger

logger = get_logger(__name__)

class ChunkServer(gfs_pb2_grpc.ChunkServerServicer):
    def __init__(self, port):
        self.port = port
        self.chunkserver_id = port
        self.chunk_dir = os.path.join(Config.chunkserver_root, self.chunkserver_id)
        os.makedirs(self.chunk_dir, exist_ok=True)
        self.chunks = {}  # chunk_handle -> version
        self.locks = {}  # chunk_handle -> threading.Lock()
        self.role = {}   # chunk_handle -> 'primary' or 'secondary'

        self.master_channel = grpc.insecure_channel(f'localhost:{Config.master_port}')
        self.master_stub = gfs_pb2_grpc.MasterServerStub(self.master_channel)
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        threading.Thread(target=self._listen_for_role_updates, daemon=True).start()

        logger.info(f"ChunkServer {self.chunkserver_id} initialized.")

    def send_heartbeat(self):
        while True:
            time.sleep(Config.heartbeat_interval)
            chunk_handles = list(self.chunks.keys())
            load = self._calculate_current_load()
            request = gfs_pb2.HeartbeatRequest(
                chunkserver_id=self.chunkserver_id,
                chunk_handles=chunk_handles,
                load=load
            )
            try:
                self.master_stub.Heartbeat(request)
                logger.debug(f"Heartbeat sent from chunkserver {self.chunkserver_id} with load {load}.")
            except Exception as e:
                logger.error(f"Heartbeat failed from chunkserver {self.chunkserver_id}: {e}")

    def _calculate_current_load(self):
        # Use psutil to get CPU utilization as an example
        return psutil.cpu_percent(interval=None) / 100.0

    def WriteChunk(self, request, context):
        chunk_handle = request.chunk_handle
        version = request.version
        data = request.data
        client_id = request.client_id

        logger.info(f"WriteChunk received for chunk {chunk_handle} (version {version}) from client {client_id}.")

        lock = self._get_chunk_lock(chunk_handle)
        with lock:
            current_version = self.chunks.get(chunk_handle, 0)
            if version != current_version:
                logger.warning(f"Version mismatch for chunk {chunk_handle}: expected {current_version}, got {version}.")
                return gfs_pb2.WriteResponse(success=False, message="Version mismatch")
            temp_file = os.path.join(self.chunk_dir, f"{chunk_handle}_{client_id}.tmp")
            with open(temp_file, 'wb') as f:
                f.write(data)
            logger.debug(f"Write prepared for chunk {chunk_handle} by client {client_id}.")
            return gfs_pb2.WriteResponse(success=True, message="Write prepared")

    def WriteCommit(self, request, context):
        chunk_handle = request.chunk_handle
        version = request.version
        client_id = request.client_id

        logger.info(f"WriteCommit received for chunk {chunk_handle} (version {version}) from client {client_id}.")

        lock = self._get_chunk_lock(chunk_handle)
        with lock:
            temp_file = os.path.join(self.chunk_dir, f"{chunk_handle}_{client_id}.tmp")
            final_file = os.path.join(self.chunk_dir, chunk_handle)
            if os.path.exists(temp_file):
                with open(temp_file, 'rb') as tf, open(final_file, 'ab') as ff:
                    ff.write(tf.read())
                os.remove(temp_file)
                self.chunks[chunk_handle] = version + 1
                logger.debug(f"Commit succeeded for chunk {chunk_handle} by client {client_id}.")
                return gfs_pb2.WriteCommitResponse(success=True, message="Commit succeeded")
            else:
                logger.error(f"No prepared write to commit for chunk {chunk_handle} by client {client_id}.")
                return gfs_pb2.WriteCommitResponse(success=False, message="No prepared write to commit")

    def ReadChunk(self, request, context):
        chunk_handle = request.handle

        logger.info(f"ReadChunk received for chunk {chunk_handle}.")

        chunk_file = os.path.join(self.chunk_dir, chunk_handle)
        lock = self._get_chunk_lock(chunk_handle)
        with lock:
            if os.path.exists(chunk_file):
                with open(chunk_file, 'rb') as f:
                    data = f.read()
                logger.debug(f"Read successful for chunk {chunk_handle}.")
                return gfs_pb2.ReadResponse(data=data)
            else:
                logger.warning(f"Chunk {chunk_handle} not found.")
                return gfs_pb2.ReadResponse(data=b'')

    def ReplicateChunk(self, request, context):
        chunk_handle = request.chunk_handle
        version = request.version
        data = request.data
        client_id = request.client_id

        logger.info(f"ReplicateChunk called for chunk {chunk_handle} (version {version}).")

        lock = self._get_chunk_lock(chunk_handle)
        with lock:
            chunk_file = os.path.join(self.chunk_dir, chunk_handle)
            with open(chunk_file, 'wb') as f:
                f.write(data)
            self.chunks[chunk_handle] = version
            logger.debug(f"Replication successful for chunk {chunk_handle}.")
            return gfs_pb2.WriteResponse(success=True, message="Replication successful")

    def UpdateRole(self, request, context):
        chunk_handle = request.chunk_handle
        new_role = request.role

        logger.info(f"UpdateRole called for chunk {chunk_handle} to role {new_role}.")

        with self._get_chunk_lock(chunk_handle):
            self.role[chunk_handle] = new_role
            logger.debug(f"Role updated for chunk {chunk_handle} to {new_role}.")
            return gfs_pb2.RoleUpdateResponse(success=True, message="Role updated")

    def _get_chunk_lock(self, chunk_handle):
        if chunk_handle not in self.locks:
            self.locks[chunk_handle] = threading.Lock()
        return self.locks[chunk_handle]

    def _listen_for_role_updates(self):
        # Placeholder for method to listen for role updates from the master
        pass

def serve():
    if len(sys.argv) != 2:
        print("Usage: python chunk_server.py <port>")
        sys.exit(1)
    port = sys.argv[1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_ChunkServerServicer_to_server(ChunkServer(port), server)
    server.add_insecure_port(f'localhost:{port}')
    server.start()
    logger.info(f"Chunkserver started on port {port}.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        logger.info("Chunkserver stopping...")
        server.stop(0)

if __name__ == '__main__':
    serve()