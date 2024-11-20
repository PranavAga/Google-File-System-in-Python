import threading
import time
import uuid
from concurrent import futures

import grpc
import gfs_pb2
import gfs_pb2_grpc

from common import Config, singleton, setup_logging

logger = setup_logging('MasterServer')

class ChunkMetadata:
    def __init__(self, chunk_handle, version=1):
        self.chunk_handle = chunk_handle
        self.version = version
        self.locations = []
        self.primary = None
        self.secondaries = []
        self.lease_expiration = 0

class FileMetadata:
    def __init__(self, file_path):
        self.file_path = file_path
        self.chunks = []
        self.is_deleted = False

@singleton
class MasterMetadata:
    def __init__(self):
        self.lock = threading.Lock()
        self.files = {}
        self.chunkservers = {}
        self.chunkserver_chunks = {}
        self.chunk_handle_map = {}

class MasterServer(gfs_pb2_grpc.MasterServerServicer):
    def __init__(self):
        self.metadata = MasterMetadata()
        threading.Thread(target=self._heartbeat_monitor, daemon=True).start()
        logger.info('MasterServer initialized.')

    def _heartbeat_monitor(self):
        while True:
            time.sleep(Config.heartbeat_interval)
            with self.metadata.lock:
                current_time = time.time()
                for cs_id in list(self.metadata.chunkservers.keys()):
                    last_heartbeat = self.metadata.chunkservers[cs_id]
                    if current_time - last_heartbeat > Config.heartbeat_interval * 2:
                        logger.warning(f'Chunkserver {cs_id} failed to send heartbeat.')
                        self._handle_chunkserver_failure(cs_id)

    def _handle_chunkserver_failure(self, chunkserver_id):
        logger.error(f'Handling failure of chunkserver {chunkserver_id}.')
        self.metadata.chunkservers.pop(chunkserver_id, None)
        chunk_handles = self.metadata.chunkserver_chunks.pop(chunkserver_id, set())
        for chunk_handle in chunk_handles:
            chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
            if chunk_meta:
                if chunkserver_id in chunk_meta.locations:
                    chunk_meta.locations.remove(chunkserver_id)
                if chunkserver_id == chunk_meta.primary:
                    chunk_meta.primary = None
                if chunkserver_id in chunk_meta.secondaries:
                    chunk_meta.secondaries.remove(chunkserver_id)
                if len(chunk_meta.locations) < Config.replication_factor:
                    self._replicate_chunk(chunk_handle)
                if chunk_meta.primary is None:
                    self._assign_new_primary(chunk_meta)

    def _replicate_chunk(self, chunk_handle):
        chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
        if not chunk_meta:
            return
        with self.metadata.lock:
            available_servers = [cs_id for cs_id in self.metadata.chunkservers.keys()
                                 if cs_id not in chunk_meta.locations]
            if not available_servers:
                logger.error('No available chunkservers for replication.')
                return
            new_cs_id = available_servers[0]
            chunk_meta.locations.append(new_cs_id)
            self.metadata.chunkserver_chunks.setdefault(new_cs_id, set()).add(chunk_handle)
            threading.Thread(target=self._send_replicate_request, args=(chunk_handle, new_cs_id), daemon=True).start()
            logger.info(f'Initiated replication of chunk {chunk_handle} to chunkserver {new_cs_id}.')

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
                data=read_response.data
            )
            target_channel = grpc.insecure_channel(f'localhost:{target_cs_id}')
            target_stub = gfs_pb2_grpc.ChunkServerStub(target_channel)
            target_stub.ReplicateChunk(write_request)
            logger.info(f'Replicated chunk {chunk_handle} to chunkserver {target_cs_id}.')
        except Exception as e:
            logger.error(f'Failed to replicate chunk {chunk_handle} to chunkserver {target_cs_id}: {e}')

    def _assign_new_primary(self, chunk_meta):
        if chunk_meta.locations:
            chunk_meta.primary = chunk_meta.locations[0]
            chunk_meta.secondaries = chunk_meta.locations[1:]
            chunk_meta.lease_expiration = time.time() + Config.lease_duration
            logger.info(f'Assigned new primary {chunk_meta.primary} for chunk {chunk_meta.chunk_handle}.')

    def ListFiles(self, request, context):
        directory = request.value
        with self.metadata.lock:
            file_list = [f for f in self.metadata.files.keys() if f.startswith(directory)]
        logger.info(f'ListFiles called for directory {directory}.')
        return gfs_pb2.StringResponse(value='\n'.join(file_list))

    def CreateFile(self, request, context):
        file_path = request.value
        with self.metadata.lock:
            if file_path in self.metadata.files:
                logger.warning(f'CreateFile: File {file_path} already exists.')
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
            self._assign_new_primary(chunk_meta)
            self._initiate_chunk_creation(chunk_meta)
            logger.info(f'File {file_path} created with initial chunk {chunk_handle}.')
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
                    data=b''
                )
                stub.WriteChunk(write_request)
                logger.info(f'Chunk {chunk_meta.chunk_handle} created on chunkserver {cs_id}.')
            except Exception as e:
                logger.error(f'Failed to create chunk {chunk_meta.chunk_handle} on chunkserver {cs_id}: {e}')
        for cs_id in chunk_meta.locations:
            threading.Thread(target=create_on_server, args=(cs_id,), daemon=True).start()

    def AppendToFile(self, request, context):
        file_path = request.file_path
        data = request.data
        with self.metadata.lock:
            file_meta = self.metadata.files.get(file_path)
            if not file_meta:
                logger.warning(f'AppendToFile: File {file_path} not found.')
                return gfs_pb2.AppendResponse(success=False, message="ERROR: File not found")
            chunk_meta = file_meta.chunks[-1]
            if chunk_meta.primary is None or time.time() > chunk_meta.lease_expiration:
                self._assign_new_primary(chunk_meta)
            primary_cs_id = chunk_meta.primary
            secondaries_cs_ids = chunk_meta.secondaries
            lease_expiration = chunk_meta.lease_expiration
        logger.info(f'Appending to file {file_path} on chunk {chunk_meta.chunk_handle}.')
        try:
            primary_channel = grpc.insecure_channel(f'localhost:{primary_cs_id}')
            primary_stub = gfs_pb2_grpc.ChunkServerStub(primary_channel)
            write_request = gfs_pb2.WriteRequest(
                chunk_handle=chunk_meta.chunk_handle,
                version=chunk_meta.version,
                data=data
            )
            write_response = primary_stub.WriteChunk(write_request)
            if not write_response.success:
                logger.error(f'Primary chunkserver {primary_cs_id} failed to prepare write: {write_response.message}')
                return gfs_pb2.AppendResponse(success=False, message=write_response.message)
            commit_request = gfs_pb2.WriteCommitRequest(
                chunk_handle=chunk_meta.chunk_handle,
                version=chunk_meta.version
            )
            commit_responses = []
            commit_response = primary_stub.WriteCommit(commit_request)
            if not commit_response.success:
                logger.error(f'Primary chunkserver {primary_cs_id} failed to commit write: {commit_response.message}')
                return gfs_pb2.AppendResponse(success=False, message=commit_response.message)
            commit_responses.append(commit_response)
            for cs_id in secondaries_cs_ids:
                secondary_channel = grpc.insecure_channel(f'localhost:{cs_id}')
                secondary_stub = gfs_pb2_grpc.ChunkServerStub(secondary_channel)
                commit_response = secondary_stub.WriteCommit(commit_request)
                if not commit_response.success:
                    logger.error(f'Secondary chunkserver {cs_id} failed to commit write: {commit_response.message}')
                    return gfs_pb2.AppendResponse(success=False, message=commit_response.message)
                commit_responses.append(commit_response)
            with self.metadata.lock:
                chunk_meta.version += 1
                logger.info(f'Chunk {chunk_meta.chunk_handle} version updated to {chunk_meta.version}.')
            logger.info(f'Append to file {file_path} successful.')
            return gfs_pb2.AppendResponse(success=True, message="Append successful")
        except Exception as e:
            logger.error(f'AppendToFile failed: {e}')
            return gfs_pb2.AppendResponse(success=False, message=f"ERROR: {e}")

    def ReadFromFile(self, request, context):
        file_path = request.file_path
        offset = request.offset
        length = request.length
        with self.metadata.lock:
            file_meta = self.metadata.files.get(file_path)
            if not file_meta:
                logger.warning(f'ReadFromFile: File {file_path} not found.')
                return gfs_pb2.ReadResponse(data=b'')
            chunk_meta = file_meta.chunks[0]
            cs_id = chunk_meta.locations[0] if chunk_meta.locations else None
        if not cs_id:
            logger.error(f'ReadFromFile: No chunkserver available for chunk {chunk_meta.chunk_handle}.')
            return gfs_pb2.ReadResponse(data=b'')
        logger.info(f'Reading from file {file_path} chunk {chunk_meta.chunk_handle}.')
        try:
            channel = grpc.insecure_channel(f'localhost:{cs_id}')
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            read_request = gfs_pb2.ChunkHandle(handle=chunk_meta.chunk_handle)
            chunk_data = stub.ReadChunk(read_request)
            data = chunk_data.data[offset:offset+length]
            return gfs_pb2.ReadResponse(data=data)
        except Exception as e:
            logger.error(f'Failed to read data from chunkserver {cs_id}: {e}')
            return gfs_pb2.ReadResponse(data=b'')

    def DeleteFile(self, request, context):
        file_path = request.value
        with self.metadata.lock:
            file_meta = self.metadata.files.pop(file_path, None)
        if file_meta:
            logger.info(f'File {file_path} deleted.')
            return gfs_pb2.StringResponse(value="File deleted")
        else:
            logger.warning(f'DeleteFile: File {file_path} not found.')
            return gfs_pb2.StringResponse(value="ERROR: File not found")

    def Heartbeat(self, request, context):
        chunkserver_id = request.chunkserver_id
        with self.metadata.lock:
            self.metadata.chunkservers[chunkserver_id] = time.time()
            for chunk_info in request.chunks:
                chunk_handle = chunk_info.chunk_handle
                version = chunk_info.version
                chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
                if chunk_meta:
                    if version < chunk_meta.version:
                        logger.warning(f'Chunkserver {chunkserver_id} has outdated version of chunk {chunk_handle}.')
        logger.debug(f'Received heartbeat from chunkserver {chunkserver_id}.')
        return gfs_pb2.HeartbeatResponse(success=True)

    def GetLeaseInfo(self, request, context):
        chunk_handle = request.chunk_handle
        with self.metadata.lock:
            chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
            if not chunk_meta:
                logger.error(f'GetLeaseInfo: Chunk {chunk_handle} not found.')
                return gfs_pb2.LeaseResponse(success=False)
            if chunk_meta.primary is None or time.time() > chunk_meta.lease_expiration:
                self._assign_new_primary(chunk_meta)
            logger.debug(f'Lease info for chunk {chunk_handle} provided.')
            return gfs_pb2.LeaseResponse(
                success=True,
                primary=chunk_meta.primary,
                secondaries=chunk_meta.secondaries,
                lease_expiration=chunk_meta.lease_expiration
            )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_MasterServerServicer_to_server(MasterServer(), server)
    server.add_insecure_port(f'[::]:{Config.master_port}')
    server.start()
    logger.info(f'Master server started on port {Config.master_port}.')
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        logger.info('Master server stopping...')
        server.stop(0)

if __name__ == '__main__':
    serve()