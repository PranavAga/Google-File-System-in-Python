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


@singleton
class MasterMetadata(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}  # Map file_path to FileMetadata
        self.chunkserver_list = cfg.chunkserver_locs[:]  # List of active chunkservers
        self.chunk_handle_map = {}  # Map chunk_handle to file_path and index

        # Heartbeat management
        self.chunkserver_heartbeat = {}  # Map chunkserver_id to last heartbeat time
        self.chunkserver_chunks = {}  # Map chunkserver_id to chunks it holds

        # Change log for sequential operations
        self.change_log = []

    def generate_chunk_handle(self):
        return str(uuid.uuid4())

    def select_chunkservers(self):
        return random.sample(self.chunkserver_list, cfg.replication_factor)

    def assign_primary(self, chunk_md):
        with self.lock:
            # Assign primary to the chunk with a new lease
            if chunk_md.locs:
                chunk_md.primary = random.choice(chunk_md.locs)
                chunk_md.lease_expiration = time.time() + cfg.lease_time

    def check_lease(self, chunk_md):
        return time.time() < chunk_md.lease_expiration

    def update_heartbeat(self, chunkserver_id, chunks, versions):
        with self.lock:
            self.chunkserver_heartbeat[chunkserver_id] = time.time()
            self.chunkserver_chunks[chunkserver_id] = {}
            for chunk_handle, version in zip(chunks, versions):
                self.chunkserver_chunks[chunkserver_id][chunk_handle] = version

    def detect_chunkserver_failure(self):
        with self.lock:
            current_time = time.time()
            failed_servers = []
            for cs_id, last_beat in self.chunkserver_heartbeat.items():
                if current_time - last_beat > cfg.chunkserver_failure_timeout:
                    failed_servers.append(cs_id)
            return failed_servers

    def remove_chunkserver(self, chunkserver_id):
        with self.lock:
            if chunkserver_id in self.chunkserver_list:
                self.chunkserver_list.remove(chunkserver_id)
            if chunkserver_id in self.chunkserver_heartbeat:
                del self.chunkserver_heartbeat[chunkserver_id]
            if chunkserver_id in self.chunkserver_chunks:
                del self.chunkserver_chunks[chunkserver_id]

    def get_file_metadata(self, file_path):
        with self.lock:
            return self.files.get(file_path, None)

    def add_change_log_entry(self, operation):
        with self.lock:
            self.change_log.append(operation)


class MasterServer(gfs_pb2_grpc.MasterServerServicer):
    def __init__(self):
        self.meta = MasterMetadata()
        self.failure_detection_thread = threading.Thread(target=self.monitor_chunkservers)
        self.failure_detection_thread.daemon = True
        self.failure_detection_thread.start()

    def monitor_chunkservers(self):
        while True:
            failed_servers = self.meta.detect_chunkserver_failure()
            for cs_id in failed_servers:
                print(f"[Master] Detected failure of Chunkserver {cs_id}")
                self.handle_chunkserver_failure(cs_id)
            time.sleep(cfg.heartbeat_interval)

    def handle_chunkserver_failure(self, chunkserver_id):
        print(f"[Master] Handling failure of Chunkserver {chunkserver_id}")
        self.meta.remove_chunkserver(chunkserver_id)
        # Update chunk metadata
        with self.meta.lock:
            for file_md in self.meta.files.values():
                for chunk_md in file_md.chunks.values():
                    if chunkserver_id in chunk_md.locs:
                        chunk_md.locs.remove(chunkserver_id)
                        if chunk_md.primary == chunkserver_id:
                            chunk_md.primary = None
                        self.replicate_chunk(chunk_md)

    def replicate_chunk(self, chunk_md):
        # Replicate the chunk to maintain replication factor
        missing_replicas = cfg.replication_factor - len(chunk_md.locs)
        if missing_replicas <= 0:
            return
        available_servers = list(set(self.meta.chunkserver_list) - set(chunk_md.locs))
        if not available_servers:
            print("[Master] No available chunkservers for replication")
            return
        new_locations = random.sample(available_servers, min(missing_replicas, len(available_servers)))
        chunk_md.locs.extend(new_locations)
        # Initiate replication from an existing replica
        source_cs = random.choice(chunk_md.locs)
        for target_cs in new_locations:
            threading.Thread(target=self.send_replicate_request, args=(chunk_md, source_cs, target_cs)).start()

    def send_replicate_request(self, chunk_md, source_cs, target_cs):
        try:
            channel = grpc.insecure_channel(f'localhost:{source_cs}')
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            request = gfs_pb2.MutationRequest(
                chunk_handle=chunk_md.chunk_handle,
                version=chunk_md.version,
                data=b'',  # Empty data for replication request
                offset=0)
            response = stub.ApplyMutation(request)
            if response.success:
                print(f"[Master] Replication from {source_cs} to {target_cs} successful for chunk {chunk_md.chunk_handle}")
            else:
                print(f"[Master] Replication failed: {response.message}")
        except Exception as e:
            print(f"[Master] Replication error: {e}")

    # Client RPC implementations
    def ListFiles(self, request, context):
        file_path = request.st
        with self.meta.lock:
            files = [fp for fp in self.meta.files.keys() if fp.startswith(file_path)]
        return gfs_pb2.String(st="\n".join(files))

    def CreateFile(self, request, context):
        file_path = request.st
        print(f"[Master] CreateFile called for {file_path}")
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
            self.meta.add_change_log_entry(f"CREATE {file_path}")
            return gfs_pb2.String(st="SUCCESS")

    def create_chunk_on_servers(self, chunk_handle, chunkservers):
        for cs_id in chunkservers:
            try:
                channel = grpc.insecure_channel(f'localhost:{cs_id}')
                stub = gfs_pb2_grpc.ChunkServerStub(channel)
                request = gfs_pb2.MutationRequest(
                    chunk_handle=chunk_handle,
                    version=1,
                    data=b'',
                    offset=0)
                response = stub.ApplyMutation(request)
                if response.success:
                    print(f"[Master] Chunk {chunk_handle} created on Chunkserver {cs_id}")
                else:
                    print(f"[Master] Error creating chunk on {cs_id}: {response.message}")
            except Exception as e:
                print(f"[Master] Error contacting Chunkserver {cs_id}: {e}")

    def AppendToFile(self, request, context):
        file_path = request.st
        print(f"[Master] AppendToFile called for {file_path}")
        with self.meta.lock:
            file_md = self.meta.get_file_metadata(file_path)
            if not file_md or file_md.delete:
                return gfs_pb2.ChunkLocations(chunk_handle="", version=0, primary="", replicas=[], message="ERROR: File does not exist")
            # Get the last chunk
            last_chunk_index = len(file_md.chunks) - 1
            chunk_md = file_md.chunks[last_chunk_index]
            if not self.meta.check_lease(chunk_md):
                self.meta.assign_primary(chunk_md)
            return gfs_pb2.ChunkLocations(
                chunk_handle=chunk_md.chunk_handle,
                version=chunk_md.version,
                primary=chunk_md.primary,
                replicas=chunk_md.locs)

    def ReadFromFile(self, request, context):
        file_path = request.file_path
        offset = request.offset
        length = request.length
        print(f"[Master] ReadFromFile called for {file_path} at offset {offset} with length {length}")
        with self.meta.lock:
            file_md = self.meta.get_file_metadata(file_path)
            if not file_md or file_md.delete:
                return gfs_pb2.ChunkLocations(chunk_handle="", version=0, primary="", replicas=[], message="ERROR: File does not exist")
            chunk_index = offset // cfg.chunk_size
            if chunk_index >= len(file_md.chunks):
                return gfs_pb2.ChunkLocations(chunk_handle="", version=0, primary="", replicas=[], message="ERROR: Offset out of bounds")
            chunk_md = file_md.chunks[chunk_index]
            return gfs_pb2.ChunkLocations(
                chunk_handle=chunk_md.chunk_handle,
                version=chunk_md.version,
                primary=chunk_md.primary if self.meta.check_lease(chunk_md) else "",
                replicas=chunk_md.locs)

    def DeleteFile(self, request, context):
        file_path = request.st
        print(f"[Master] DeleteFile called for {file_path}")
        with self.meta.lock:
            file_md = self.meta.get_file_metadata(file_path)
            if not file_md:
                return gfs_pb2.String(st="ERROR: File does not exist")
            if file_md.delete:
                return gfs_pb2.String(st="ERROR: File is already deleted")
            file_md.delete = True
            self.meta.add_change_log_entry(f"DELETE {file_path}")
        return gfs_pb2.String(st="SUCCESS")

    # Chunkserver RPC implementations
    def Heartbeat(self, request, context):
        chunkserver_id = request.chunkserver_id
        chunks = request.chunks
        versions = request.versions
        print(f"[Master] Heartbeat received from Chunkserver {chunkserver_id}")
        self.meta.update_heartbeat(chunkserver_id, chunks, versions)
        # For simplicity, we are not checking for invalid chunks
        return gfs_pb2.HeartbeatResponse(success=True, invalid_chunks=[])

    def ReportChunk(self, request, context):
        # Handle chunk report from chunkservers
        chunk_handle = request.chunk_handle
        version = request.version
        print(f"[Master] ReportChunk received for {chunk_handle} with version {version}")
        # Update the chunk metadata if necessary
        with self.meta.lock:
            if chunk_handle in self.meta.chunk_handle_map:
                file_path, chunk_index = self.meta.chunk_handle_map[chunk_handle]
                file_md = self.meta.files[file_path]
                chunk_md = file_md.chunks[chunk_index]
                if version > chunk_md.version:
                    chunk_md.version = version
        return gfs_pb2.Empty()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_MasterServerServicer_to_server(MasterServer(), server)
    server.add_insecure_port('[::]:{}'.format(cfg.master_loc))
    server.start()
    print(f"[Master] Server started on port {cfg.master_loc}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()