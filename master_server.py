import threading
import time
import uuid
from concurrent import futures

import grpc
import gfs_pb2
import gfs_pb2_grpc

from common import Config, singleton

# Data structures for file and chunk metadata
class ChunkMetadata:
    def __init__(self, chunk_handle, version=1):
        self.chunk_handle = chunk_handle
        self.version = version
        self.locations = []
        self.primary = None
        self.lease_expiration = 0  # Timestamp when lease expires

class FileMetadata:
    def __init__(self, file_path):
        self.file_path = file_path
        self.chunks = []  # List of ChunkMetadata
        self.is_deleted = False

@singleton
class MasterMetadata:
    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}  # Map file path to FileMetadata
        self.chunk_locations = {}  # Map chunk_handle to list of chunkserver_ids
        self.chunkserver_heartbeats = {}  # Map chunkserver_id to last heartbeat time
        self.active_chunkservers = set(Config.chunkserver_ports)

class MasterServer(gfs_pb2_grpc.MasterServerServicer):
    def __init__(self):
        self.metadata = MasterMetadata()
        # Start heartbeat monitoring thread
        threading.Thread(target=self.monitor_heartbeats, daemon=True).start()

    def monitor_heartbeats(self):
        while True:
            time.sleep(Config.heartbeat_interval)
            with self.metadata.lock:
                current_time = time.time()
                to_remove = []
                for cs_id, last_heartbeat in self.metadata.chunkserver_heartbeats.items():
                    if current_time - last_heartbeat > Config.heartbeat_interval * 2:
                        print(f"Chunkserver {cs_id} failed to send heartbeat.")
                        to_remove.append(cs_id)
                for cs_id in to_remove:
                    self.metadata.active_chunkservers.discard(cs_id)
                    del self.metadata.chunkserver_heartbeats[cs_id]
                    # Handle chunkserver failure (e.g., re-replication)
                    self.handle_chunkserver_failure(cs_id)

    def handle_chunkserver_failure(self, chunkserver_id):
        # Iterate over all chunks and remove the failed chunkserver from locations
        for file_md in self.metadata.files.values():
            for chunk_md in file_md.chunks:
                if chunkserver_id in chunk_md.locations:
                    chunk_md.locations.remove(chunkserver_id)
                    # If replication factor is not met, initiate replication
                    if len(chunk_md.locations) < Config.replication_factor:
                        self.replicate_chunk(chunk_md)

    def replicate_chunk(self, chunk_md):
        # Simple replication to maintain replication factor
        available_chunkservers = list(self.metadata.active_chunkservers - set(chunk_md.locations))
        if not available_chunkservers:
            print("No available chunkservers for replication.")
            return

        new_location = available_chunkservers[0]
        chunk_md.locations.append(new_location)
        # Simulate replication process
        threading.Thread(target=self.send_replicate_chunk, args=(chunk_md, new_location), daemon=True).start()

    def send_replicate_chunk(self, chunk_md, chunkserver_id):
        # Fetch chunk data from an existing location
        source_cs_id = chunk_md.locations[0]
        try:
            source_channel = grpc.insecure_channel(f'localhost:{source_cs_id}')
            source_stub = gfs_pb2_grpc.ChunkServerStub(source_channel)
            request = gfs_pb2.StringRequest(message=chunk_md.chunk_handle)
            chunk_data = source_stub.ReadChunk(request)
            # Send chunk data to new chunkserver
            target_channel = grpc.insecure_channel(f'localhost:{chunkserver_id}')
            target_stub = gfs_pb2_grpc.ChunkServerStub(target_channel)
            replicate_request = gfs_pb2.ChunkData(
                chunk_handle=chunk_md.chunk_handle,
                data=chunk_data.data,
                version=chunk_md.version
            )
            target_stub.ReplicateChunk(replicate_request)
            print(f"Chunk {chunk_md.chunk_handle} replicated to chunkserver {chunkserver_id}.")
        except Exception as e:
            print(f"Failed to replicate chunk {chunk_md.chunk_handle}: {e}")

    # Client RPC implementations
    def ListFiles(self, request, context):
        directory = request.message
        with self.metadata.lock:
            files_in_directory = [fp for fp in self.metadata.files if fp.startswith(directory)]
        return gfs_pb2.StringResponse(message='\n'.join(files_in_directory))

    def CreateFile(self, request, context):
        file_path = request.message
        with self.metadata.lock:
            if file_path in self.metadata.files:
                return gfs_pb2.StringResponse(message="ERROR: File already exists.")
            file_md = FileMetadata(file_path)
            self.metadata.files[file_path] = file_md
            # Create initial chunk
            chunk_handle = str(uuid.uuid4())
            chunk_md = ChunkMetadata(chunk_handle)
            file_md.chunks.append(chunk_md)
            # Assign chunkservers
            chunk_md.locations = self.assign_chunkservers()
            # Notify chunkservers to create the chunk
            self.create_chunk_on_servers(chunk_md)
        return gfs_pb2.StringResponse(message="File created successfully.")

    def assign_chunkservers(self):
        with self.metadata.lock:
            available_chunkservers = list(self.metadata.active_chunkservers)
            if len(available_chunkservers) < Config.replication_factor:
                print("Insufficient chunkservers for replication.")
                return available_chunkservers
            return available_chunkservers[:Config.replication_factor]

    def create_chunk_on_servers(self, chunk_md):
        for cs_id in chunk_md.locations:
            try:
                channel = grpc.insecure_channel(f'localhost:{cs_id}')
                stub = gfs_pb2_grpc.ChunkServerStub(channel)
                request = gfs_pb2.ChunkData(
                    chunk_handle=chunk_md.chunk_handle,
                    data=b'',
                    version=chunk_md.version
                )
                stub.PushChunk(request)
                print(f"Chunk {chunk_md.chunk_handle} created on chunkserver {cs_id}.")
            except Exception as e:
                print(f"Failed to create chunk on chunkserver {cs_id}: {e}")

    def AppendToFile(self, request, context):
        try:
            file_path, data = request.message.split('|', 1)
            with self.metadata.lock:
                if file_path not in self.metadata.files:
                    return gfs_pb2.StringResponse(message="ERROR: File does not exist.")
                file_md = self.metadata.files[file_path]
                chunk_md = file_md.chunks[-1]
                # Check if chunk has space (simplified logic)
                # For this example, we'll assume chunks always have space
                # In practice, you'd check the actual size
            # Send data to primary chunkserver
            primary_cs_id = chunk_md.locations[0]
            try:
                channel = grpc.insecure_channel(f'localhost:{primary_cs_id}')
                stub = gfs_pb2_grpc.ChunkServerStub(channel)
                chunk_data = gfs_pb2.ChunkData(
                    chunk_handle=chunk_md.chunk_handle,
                    data=data.encode(),
                    version=chunk_md.version
                )
                stub.PushChunk(chunk_data)
                print(f"Data appended to chunk {chunk_md.chunk_handle} at chunkserver {primary_cs_id}.")
            except Exception as e:
                print(f"Failed to append data to chunkserver {primary_cs_id}: {e}")
                return gfs_pb2.StringResponse(message="ERROR: Failed to append data.")
            return gfs_pb2.StringResponse(message="Data appended successfully.")
        except Exception as e:
            print(f"AppendToFile error: {e}")
            return gfs_pb2.StringResponse(message="ERROR: AppendToFile failed.")

    def ReadFromFile(self, request, context):
        file_path = request.file_path
        offset = request.offset
        num_bytes = request.num_bytes
        with self.metadata.lock:
            if file_path not in self.metadata.files:
                return gfs_pb2.ReadResponse(data=b'')
            file_md = self.metadata.files[file_path]
            # Simplified logic to read from the first chunk
            chunk_md = file_md.chunks[0]
            cs_id = chunk_md.locations[0]
        try:
            channel = grpc.insecure_channel(f'localhost:{cs_id}')
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            read_request = gfs_pb2.StringRequest(message=chunk_md.chunk_handle)
            chunk_data = stub.ReadChunk(read_request)
            data = chunk_data.data[offset:offset+num_bytes]
            return gfs_pb2.ReadResponse(data=data)
        except Exception as e:
            print(f"Failed to read data from chunkserver {cs_id}: {e}")
            return gfs_pb2.ReadResponse(data=b'')

    def DeleteFile(self, request, context):
        file_path = request.message
        with self.metadata.lock:
            if file_path not in self.metadata.files:
                return gfs_pb2.StringResponse(message="ERROR: File does not exist.")
            # Mark file as deleted (or remove from metadata)
            del self.metadata.files[file_path]
            print(f"File {file_path} deleted.")
            # Notify chunkservers to delete chunks (not implemented here)
        return gfs_pb2.StringResponse(message="File deleted successfully.")

    # Chunkserver RPC implementations
    def Heartbeat(self, request, context):
        chunkserver_id = request.chunkserver_id
        with self.metadata.lock:
            self.metadata.chunkserver_heartbeats[chunkserver_id] = time.time()
            self.metadata.active_chunkservers.add(chunkserver_id)
        print(f"Received heartbeat from chunkserver {chunkserver_id}.")
        return gfs_pb2.HeartbeatResponse(success=True, invalid_chunk_handles=[])

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_MasterServerServicer_to_server(MasterServer(), server)
    server.add_insecure_port(f'[::]:{Config.master_port}')
    server.start()
    print(f"Master server started on port {Config.master_port}.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("Master server stopping...")
        server.stop(0)

if __name__ == '__main__':
    serve()