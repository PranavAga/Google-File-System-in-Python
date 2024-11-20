from concurrent import futures
import threading
import time
from collections import OrderedDict
import random
import uuid

import grpc
import gfs_pb2_grpc
import gfs_pb2

from common import Config as cfg
from common import Status, singleton


class ChunkMetadata(object):
    def __init__(self, chunk_handle, version):
        self.chunk_handle = chunk_handle
        self.version = version
        self.locs = []  # List of chunkservers storing this chunk
        self.primary = None  # Current primary chunkserver
        self.lease_expiration = 0  # Lease expiration time


class FileMetadata(object):
    def __init__(self, file_path):
        self.file_path = file_path
        self.chunks = OrderedDict()  # Map chunk index to ChunkMetadata
        self.delete = False
        self.version = 0  # For managing concurrent modifications


@singleton
class MasterMetadata(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}  # Map file_path to FileMetadata
        self.chunkserver_list = cfg.chunkserver_locs[:]  # List of active chunkservers
        self.chunk_handle_map = {}  # Map chunk_handle to file_path and index

        # Heartbeat management
        self.chunkserver_heartbeat = {}  # Map chunkserver_id to last heartbeat time

    def generate_chunk_handle(self):
        return str(uuid.uuid4())

    def select_chunkservers(self):
        return random.sample(self.chunkserver_list, cfg.replication_factor)

    def assign_primary(self, chunk_md):
        with self.lock:
            # Assign primary to the chunk with a new lease
            chunk_md.primary = random.choice(chunk_md.locs)
            chunk_md.lease_expiration = time.time() + cfg.lease_time

    def check_lease(self, chunk_md):
        return time.time() < chunk_md.lease_expiration

    def update_heartbeat(self, chunkserver_id):
        with self.lock:
            self.chunkserver_heartbeat[chunkserver_id] = time.time()

    def detect_chunkserver_failure(self):
        with self.lock:
            current_time = time.time()
            failed_servers = []
            for cs_id, last_beat in self.chunkserver_heartbeat.items():
                if current_time - last_beat > cfg.heartbeat_interval * 2:
                    failed_servers.append(cs_id)
            return failed_servers


class MasterServer(gfs_pb2_grpc.MasterServerServicer):
    def __init__(self):
        self.meta = MasterMetadata()
        self.failure_detection_thread = threading.Thread(target=self.monitor_chunkservers)
        self.failure_detection_thread.daemon = True
        self.failure_detection_thread.start()

    def monitor_chunkservers(self):
        while True:
            failed_servers = self.meta.detect_chunkserver_failure()
            if failed_servers:
                for cs_id in failed_servers:
                    print(f"Chunkserver {cs_id} failed")
                    self.handle_chunkserver_failure(cs_id)
            time.sleep(cfg.heartbeat_interval)

    def handle_chunkserver_failure(self, chunkserver_id):
        with self.meta.lock:
            # Remove chunkserver from active list
            if chunkserver_id in self.meta.chunkserver_list:
                self.meta.chunkserver_list.remove(chunkserver_id)
            # Remove heartbeat entry
            if chunkserver_id in self.meta.chunkserver_heartbeat:
                del self.meta.chunkserver_heartbeat[chunkserver_id]
            # Update chunk replicas
            for file_md in self.meta.files.values():
                for chunk_md in file_md.chunks.values():
                    if chunkserver_id in chunk_md.locs:
                        chunk_md.locs.remove(chunkserver_id)
                        if chunk_md.primary == chunkserver_id:
                            chunk_md.primary = None
                        # Trigger replication to maintain replication factor
                        self.replicate_chunk(chunk_md)

    def replicate_chunk(self, chunk_md):
        # Logic to replicate chunk to maintain replication factor
        missing_replicas = cfg.replication_factor - len(chunk_md.locs)
        if missing_replicas > 0:
            available_servers = list(set(self.meta.chunkserver_list) - set(chunk_md.locs))
            if available_servers:
                new_locations = random.sample(available_servers, min(missing_replicas, len(available_servers)))
                chunk_md.locs.extend(new_locations)
                # Initiate replication to new chunkservers
                for cs_id in new_locations:
                    # Assuming we have a method to send replication requests
                    threading.Thread(target=self.send_replicate_request, args=(chunk_md, cs_id)).start()

    def send_replicate_request(self, chunk_md, chunkserver_id):
        try:
            channel = grpc.insecure_channel(f'localhost:{chunkserver_id}')
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            chunk_data = gfs_pb2.ChunkData(chunk_handle=chunk_md.chunk_handle, version=chunk_md.version)
            response = stub.ReplicateChunk(chunk_data)
            print(f"Replication to chunkserver {chunkserver_id} successful: {response.st}")
        except Exception as e:
            print(f"Replication to chunkserver {chunkserver_id} failed: {e}")

    # Client RPC implementations
    def ListFiles(self, request, context):
        # Implementation similar to previous code
        pass

    def CreateFile(self, request, context):
        file_path = request.st
        print(f"CreateFile called for {file_path}")
        with self.meta.lock:
            if file_path in self.meta.files:
                return gfs_pb2.String(st="ERROR: File already exists")
            file_md = FileMetadata(file_path)
            self.meta.files[file_path] = file_md
            # Create initial chunk
            chunk_handle = self.meta.generate_chunk_handle()
            chunk_md = ChunkMetadata(chunk_handle, version=1)
            chunk_md.locs = self.meta.select_chunkservers()
            self.meta.assign_primary(chunk_md)
            file_md.chunks[0] = chunk_md
            self.meta.chunk_handle_map[chunk_handle] = (file_path, 0)
            # Instruct chunkservers to create the chunk
            self.create_chunk_on_servers(chunk_handle, chunk_md.locs)
            return gfs_pb2.String(st="SUCCESS")

    def create_chunk_on_servers(self, chunk_handle, chunkservers):
        for cs_id in chunkservers:
            try:
                channel = grpc.insecure_channel(f'localhost:{cs_id}')
                stub = gfs_pb2_grpc.ChunkServerStub(channel)
                chunk_data = gfs_pb2.ChunkData(chunk_handle=chunk_handle, data=b'', version=1)
                response = stub.PushData(chunk_data)
                print(f"Chunk {chunk_handle} created on chunkserver {cs_id}: {response.st}")
            except Exception as e:
                print(f"Failed to create chunk {chunk_handle} on chunkserver {cs_id}: {e}")

    def AppendToFile(self, request, context):
        # Implementation of two-phase commit protocol
        pass

    def ReadFromFile(self, request, context):
        # Implementation for reading data from chunks
        pass

    def DeleteFile(self, request, context):
        # Mark the file as deleted
        pass

    def UndeleteFile(self, request, context):
        # Restore previously deleted file
        pass

    # Chunkserver RPC implementations
    def Heartbeat(self, request, context):
        chunkserver_id = request.chunkserver_id
        print(f"Heartbeat received from Chunkserver {chunkserver_id}")
        self.meta.update_heartbeat(chunkserver_id)
        # Return any invalid chunks (e.g., due to version mismatch)
        invalid_chunks = []  # Placeholder
        return gfs_pb2.HeartbeatResponse(success=True, invalid_chunks=invalid_chunks)

    def ReportChunk(self, request, context):
        # Handle chunk report from chunkservers
        pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_MasterServerServicer_to_server(MasterServer(), server)
    server.add_insecure_port('[::]:{}'.format(cfg.master_loc))
    server.start()
    print(f"Master server started on port {cfg.master_loc}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()