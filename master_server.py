import threading
import time
import uuid
from concurrent import futures

import grpc
import gfs_pb2
import gfs_pb2_grpc

from common import Config, singleton

class ChunkMetadata:
    def __init__(self, chunk_handle, version=1):
        self.chunk_handle = chunk_handle
        self.version = version
        self.locations = []  # list of chunkserver_ids
        self.primary = None  # primary chunkserver_id
        self.lease_expiration = 0  # timestamp

class FileMetadata:
    def __init__(self, file_path):
        self.file_path = file_path
        self.chunks = []  # list of ChunkMetadata

@singleton
class MasterMetadata:
    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}  # file_path -> FileMetadata
        self.chunkservers = {}  # chunkserver_id -> last_heartbeat
        self.chunkserver_chunks = {}  # chunkserver_id -> set of chunk_handles
        self.chunk_handle_map = {}  # chunk_handle -> ChunkMetadata

class MasterServer(gfs_pb2_grpc.MasterServerServicer):
    def __init__(self):
        self.metadata = MasterMetadata()
        threading.Thread(target=self._heartbeat_monitor, daemon=True).start()

    def _heartbeat_monitor(self):
        while True:
            time.sleep(Config.heartbeat_interval)
            with self.metadata.lock:
                for cs_id in list(self.metadata.chunkservers.keys()):
                    last_heartbeat = self.metadata.chunkservers[cs_id]
                    if time.time() - last_heartbeat > Config.heartbeat_interval * 2:
                        self._handle_chunkserver_failure(cs_id)

    def _handle_chunkserver_failure(self, chunkserver_id):
        self.metadata.chunkservers.pop(chunkserver_id, None)
        chunk_handles = self.metadata.chunkserver_chunks.pop(chunkserver_id, set())
        for chunk_handle in chunk_handles:
            chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
            if chunk_meta:
                if chunkserver_id in chunk_meta.locations:
                    chunk_meta.locations.remove(chunkserver_id)
                if chunkserver_id == chunk_meta.primary:
                    chunk_meta.primary = None
                    chunk_meta.lease_expiration = 0
                if len(chunk_meta.locations) < Config.replication_factor:
                    self._replicate_chunk(chunk_handle)

    def _replicate_chunk(self, chunk_handle):
        chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
        if not chunk_meta or not chunk_meta.locations:
            return
        with self.metadata.lock:
            available_servers = [cs_id for cs_id in self.metadata.chunkservers.keys()
                                 if cs_id not in chunk_meta.locations]
            if not available_servers:
                return
            new_cs_id = available_servers[0]
            chunk_meta.locations.append(new_cs_id)
            self.metadata.chunkserver_chunks.setdefault(new_cs_id, set()).add(chunk_handle)
            threading.Thread(target=self._send_replicate_request, args=(chunk_handle, new_cs_id), daemon=True).start()

    def _send_replicate_request(self, chunk_handle, target_cs_id):
        chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
        if not chunk_meta or not chunk_meta.locations:
            return
        source_cs_id = chunk_meta.locations[0]
        try:
            source_channel = grpc.insecure_channel(f'localhost:{source_cs_id}')
            source_stub = gfs_pb2_grpc.ChunkServerStub(source_channel)
            read_response = source_stub.ReadChunk(gfs_pb2.ChunkHandle(handle=chunk_handle))
            write_request = gfs_pb2.WriteRequest(
                chunk_handle=chunk_handle,
                version=chunk_meta.version,
                data=read_response.data,
                client_id="replication"
            )
            target_channel = grpc.insecure_channel(f'localhost:{target_cs_id}')
            target_stub = gfs_pb2_grpc.ChunkServerStub(target_channel)
            target_stub.ReplicateChunk(write_request)
        except Exception as e:
            pass

    def ListFiles(self, request, context):
        directory = request.value
        with self.metadata.lock:
            file_list = [f for f in self.metadata.files.keys() if f.startswith(directory)]
        return gfs_pb2.StringResponse(value='\n'.join(file_list))

    def CreateFile(self, request, context):
        file_path = request.value
        with self.metadata.lock:
            if file_path in self.metadata.files:
                return gfs_pb2.StringResponse(value="ERROR: File already exists")
            file_meta = FileMetadata(file_path)
            self.metadata.files[file_path] = file_meta
            chunk_handle = str(uuid.uuid4())
            chunk_meta = ChunkMetadata(chunk_handle)
            file_meta.chunks.append(chunk_meta)
            self.metadata.chunk_handle_map[chunk_handle] = chunk_meta
            chunk_meta.locations = self._select_chunkservers()
            for cs_id in chunk_meta.locations:
                self.metadata.chunkserver_chunks.setdefault(cs_id, set()).add(chunk_handle)
            chunk_meta.primary = None
        self._initiate_chunk_creation(chunk_meta)
        return gfs_pb2.StringResponse(value="File created successfully")

    def _select_chunkservers(self):
        with self.metadata.lock:
            return list(self.metadata.chunkservers.keys())[:Config.replication_factor]

    def _initiate_chunk_creation(self, chunk_meta):
        def create_on_server(cs_id):
            try:
                channel = grpc.insecure_channel(f'localhost:{cs_id}')
                stub = gfs_pb2_grpc.ChunkServerStub(channel)
                write_request = gfs_pb2.WriteRequest(
                    chunk_handle=chunk_meta.chunk_handle,
                    version=chunk_meta.version,
                    data=b'',
                    client_id="init"
                )
                stub.WriteChunk(write_request)
            except Exception as e:
                pass
        for cs_id in chunk_meta.locations:
            threading.Thread(target=create_on_server, args=(cs_id,), daemon=True).start()

    def AppendToFile(self, request, context):
        file_path = request.file_path
        data = request.data
        with self.metadata.lock:
            file_meta = self.metadata.files.get(file_path)
            if not file_meta:
                return gfs_pb2.AppendResponse(success=False, message="ERROR: File not found")
            chunk_meta = file_meta.chunks[-1]
            if chunk_meta.primary is None or time.time() > chunk_meta.lease_expiration:
                self._assign_new_primary(chunk_meta)
            primary_cs_id = chunk_meta.primary
            secondaries_cs_ids = [cs_id for cs_id in chunk_meta.locations if cs_id != primary_cs_id]
            lease_expiration = chunk_meta.lease_expiration

        client_id = str(uuid.uuid4())

        try:
            # Send write to primary
            primary_channel = grpc.insecure_channel(f'localhost:{primary_cs_id}')
            primary_stub = gfs_pb2_grpc.ChunkServerStub(primary_channel)
            write_request = gfs_pb2.WriteRequest(
                chunk_handle=chunk_meta.chunk_handle,
                version=chunk_meta.version,
                data=data,
                client_id=client_id
            )
            write_response = primary_stub.WriteChunk(write_request)
            if not write_response.success:
                return gfs_pb2.AppendResponse(success=False, message=write_response.message)

            # Send write to secondaries
            for cs_id in secondaries_cs_ids:
                secondary_channel = grpc.insecure_channel(f'localhost:{cs_id}')
                secondary_stub = gfs_pb2_grpc.ChunkServerStub(secondary_channel)
                secondary_stub.WriteChunk(write_request)

            # Commit write on primary
            commit_request = gfs_pb2.WriteCommitRequest(
                chunk_handle=chunk_meta.chunk_handle,
                version=chunk_meta.version,
                client_id=client_id
            )
            commit_response = primary_stub.WriteCommit(commit_request)
            if not commit_response.success:
                return gfs_pb2.AppendResponse(success=False, message=commit_response.message)

            # Commit write on secondaries
            for cs_id in secondaries_cs_ids:
                secondary_channel = grpc.insecure_channel(f'localhost:{cs_id}')
                secondary_stub = gfs_pb2_grpc.ChunkServerStub(secondary_channel)
                secondary_stub.WriteCommit(commit_request)

            with self.metadata.lock:
                chunk_meta.version += 1

            return gfs_pb2.AppendResponse(success=True, message="Append successful")
        except Exception as e:
            return gfs_pb2.AppendResponse(success=False, message=f"ERROR: {e}")

    def _assign_new_primary(self, chunk_meta):
        if chunk_meta.locations:
            chunk_meta.primary = chunk_meta.locations[0]
            chunk_meta.lease_expiration = time.time() + Config.lease_duration

    def ReadFromFile(self, request, context):
        file_path = request.file_path
        offset = request.offset
        length = request.length
        with self.metadata.lock:
            file_meta = self.metadata.files.get(file_path)
            if not file_meta:
                return gfs_pb2.ReadResponse(data=b'')
            chunk_meta = file_meta.chunks[0]
            cs_id = chunk_meta.locations[0] if chunk_meta.locations else None
        if not cs_id:
            return gfs_pb2.ReadResponse(data=b'')
        try:
            channel = grpc.insecure_channel(f'localhost:{cs_id}')
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            read_request = gfs_pb2.ChunkHandle(handle=chunk_meta.chunk_handle)
            read_response = stub.ReadChunk(read_request)
            data = read_response.data[offset:offset+length]
            return gfs_pb2.ReadResponse(data=data)
        except Exception as e:
            return gfs_pb2.ReadResponse(data=b'')

    def DeleteFile(self, request, context):
        file_path = request.value
        with self.metadata.lock:
            file_meta = self.metadata.files.pop(file_path, None)
        if file_meta:
            return gfs_pb2.StringResponse(value="File deleted")
        else:
            return gfs_pb2.StringResponse(value="ERROR: File not found")

    def Heartbeat(self, request, context):
        chunkserver_id = request.chunkserver_id
        with self.metadata.lock:
            self.metadata.chunkservers[chunkserver_id] = time.time()
            if chunkserver_id not in self.metadata.chunkserver_chunks:
                self.metadata.chunkserver_chunks[chunkserver_id] = set()
            for chunk_handle in request.chunk_handles:
                self.metadata.chunkserver_chunks[chunkserver_id].add(chunk_handle)
        return gfs_pb2.HeartbeatResponse(success=True)

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