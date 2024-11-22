import threading
import time
import uuid
import logging
from concurrent import futures

import grpc
import gfs_pb2
import gfs_pb2_grpc

from common import Config, singleton

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to logging.DEBUG for more detailed logs
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('gfs_master.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('MasterServer')


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
        self.chunkserver_loads = {}  # chunkserver_id -> load
        self.chunkserver_chunks = {}  # chunkserver_id -> set of chunk_handles
        self.chunk_handle_map = {}  # chunk_handle -> ChunkMetadata
        self.change_log = []  # List of change log entries (operation records)


class MasterServer(gfs_pb2_grpc.MasterServerServicer):
    def __init__(self):
        self.metadata = MasterMetadata()
        threading.Thread(target=self._heartbeat_monitor, daemon=True).start()
        logger.info("MasterServer initialized.")

    def _heartbeat_monitor(self):
        logger.info("Heartbeat monitor thread started.")
        while True:
            time.sleep(Config.heartbeat_interval)
            with self.metadata.lock:
                for cs_id in list(self.metadata.chunkservers.keys()):
                    last_heartbeat = self.metadata.chunkservers[cs_id]
                    if time.time() - last_heartbeat > Config.heartbeat_interval * 2:
                        logger.warning(f"Chunkserver {cs_id} failed to send heartbeat.")
                        self._handle_chunkserver_failure(cs_id)

    def _handle_chunkserver_failure(self, chunkserver_id):
        logger.warning(f"Handling failure of chunkserver {chunkserver_id}.")
        self.metadata.chunkservers.pop(chunkserver_id, None)
        self.metadata.chunkserver_loads.pop(chunkserver_id, None)
        chunk_handles = self.metadata.chunkserver_chunks.pop(chunkserver_id, set())
        for chunk_handle in chunk_handles:
            chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
            if chunk_meta:
                if chunkserver_id in chunk_meta.locations:
                    chunk_meta.locations.remove(chunkserver_id)
                    logger.info(f"Removed chunkserver {chunkserver_id} from locations of chunk {chunk_handle}.")
                if chunkserver_id == chunk_meta.primary:
                    chunk_meta.primary = None
                    chunk_meta.lease_expiration = 0
                    logger.info(f"Chunkserver {chunkserver_id} was primary for chunk {chunk_handle}. Assigning new primary.")
                    self._assign_new_primary(chunk_meta)
                if len(chunk_meta.locations) < Config.replication_factor:
                    self._replicate_chunk(chunk_handle)

    def _replicate_chunk(self, chunk_handle):
        logger.info(f"Starting replication for chunk {chunk_handle}.")
        chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
        if not chunk_meta or not chunk_meta.locations:
            logger.warning(f"No available locations for chunk {chunk_handle} to replicate from.")
            return
        with self.metadata.lock:
            available_servers = [cs_id for cs_id in self.metadata.chunkservers.keys()
                                 if cs_id not in chunk_meta.locations]
            if not available_servers:
                logger.warning("No available chunkservers to replicate chunk.")
                return
            new_cs_id = self._select_chunkserver_for_replication(available_servers)
            chunk_meta.locations.append(new_cs_id)
            self.metadata.chunkserver_chunks.setdefault(new_cs_id, set()).add(chunk_handle)
            logger.info(f"Selected chunkserver {new_cs_id} for replication of chunk {chunk_handle}.")
            threading.Thread(target=self._send_replicate_request, args=(chunk_handle, new_cs_id), daemon=True).start()

    def _select_chunkserver_for_replication(self, available_servers):
        # Implement selection logic, e.g., based on load
        # For now, just select the first one
        return available_servers[0]

    def _send_replicate_request(self, chunk_handle, target_cs_id):
        logger.info(f"Sending replication request for chunk {chunk_handle} to chunkserver {target_cs_id}.")
        chunk_meta = self.metadata.chunk_handle_map.get(chunk_handle)
        if not chunk_meta or not chunk_meta.locations:
            logger.warning(f"Cannot replicate chunk {chunk_handle}: metadata incomplete.")
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
            logger.info(f"Replication of chunk {chunk_handle} to chunkserver {target_cs_id} completed.")
        except Exception as e:
            logger.error(f"Error during replication of chunk {chunk_handle} to chunkserver {target_cs_id}: {e}")

    def ListFiles(self, request, context):
        directory = request.value
        logger.info(f"ListFiles called for directory: {directory}")
        with self.metadata.lock:
            file_list = [f for f in self.metadata.files.keys() if f.startswith(directory)]
        return gfs_pb2.StringResponse(value='\n'.join(file_list))

    def CreateFile(self, request, context):
        file_path = request.value
        logger.info(f"CreateFile called for file_path: {file_path}")
        with self.metadata.lock:
            if file_path in self.metadata.files:
                logger.warning(f"File already exists: {file_path}")
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
                logger.info(f"Assigned chunk {chunk_handle} to chunkserver {cs_id}.")
            chunk_meta.primary = None
            # Log the operation in the change log
            self.metadata.change_log.append({
                'operation': 'CreateFile',
                'file_path': file_path,
                'chunk_handle': chunk_handle,
                'timestamp': time.time()
            })
            logger.debug(f"Change log updated with CreateFile operation for {file_path}.")
        self._initiate_chunk_creation(chunk_meta)
        return gfs_pb2.StringResponse(value="File created successfully")

    def _select_chunkservers(self):
        # with self.metadata.lock:
        # Implement logic to select chunkservers, e.g., based on load
        sorted_chunkservers = sorted(
            self.metadata.chunkservers.keys(),
            key=lambda cs_id: self.metadata.chunkserver_loads.get(cs_id, 0)
        )
        selected_servers = sorted_chunkservers[:Config.replication_factor]
        logger.debug(f"Selected chunkservers for new chunk: {selected_servers}")
        return selected_servers

    def _initiate_chunk_creation(self, chunk_meta):
        logger.info(f"Initiating chunk creation for chunk {chunk_meta.chunk_handle} on locations {chunk_meta.locations}")
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
                logger.info(f"Chunk {chunk_meta.chunk_handle} created on chunkserver {cs_id}.")
            except Exception as e:
                logger.error(f"Error creating chunk {chunk_meta.chunk_handle} on chunkserver {cs_id}: {e}")
        for cs_id in chunk_meta.locations:
            threading.Thread(target=create_on_server, args=(cs_id,), daemon=True).start()

    def AppendToFile(self, request, context):
        file_path = request.file_path
        data = request.data
        logger.info(f"AppendToFile called for file {file_path}")
        with self.metadata.lock:
            file_meta = self.metadata.files.get(file_path)
            if not file_meta:
                logger.error(f"File not found: {file_path}")
                return gfs_pb2.AppendResponse(success=False, message="ERROR: File not found")
            chunk_meta = file_meta.chunks[-1]
            # Check if the chunk is full, for simplicity, we skip this step
            if chunk_meta.primary is None or time.time() > chunk_meta.lease_expiration:
                self._assign_new_primary(chunk_meta)
            primary_cs_id = chunk_meta.primary
            secondaries_cs_ids = [cs_id for cs_id in chunk_meta.locations if cs_id != primary_cs_id]
            lease_expiration = chunk_meta.lease_expiration

        logger.debug(f"Primary for chunk {chunk_meta.chunk_handle} is {primary_cs_id}")
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
                logger.error(f"WriteChunk failed on primary {primary_cs_id}: {write_response.message}")
                return gfs_pb2.AppendResponse(success=False, message=write_response.message)

            # Send write to secondaries
            for cs_id in secondaries_cs_ids:
                try:
                    secondary_channel = grpc.insecure_channel(f'localhost:{cs_id}')
                    secondary_stub = gfs_pb2_grpc.ChunkServerStub(secondary_channel)
                    secondary_stub.WriteChunk(write_request)
                    logger.debug(f"WriteChunk sent to secondary {cs_id}")
                except Exception as e:
                    logger.error(f"Error sending WriteChunk to secondary {cs_id}: {e}")

            # Commit write on primary
            commit_request = gfs_pb2.WriteCommitRequest(
                chunk_handle=chunk_meta.chunk_handle,
                version=chunk_meta.version,
                client_id=client_id
            )
            commit_response = primary_stub.WriteCommit(commit_request)
            if not commit_response.success:
                logger.error(f"WriteCommit failed on primary {primary_cs_id}: {commit_response.message}")
                return gfs_pb2.AppendResponse(success=False, message=commit_response.message)

            # Commit write on secondaries
            for cs_id in secondaries_cs_ids:
                try:
                    secondary_channel = grpc.insecure_channel(f'localhost:{cs_id}')
                    secondary_stub = gfs_pb2_grpc.ChunkServerStub(secondary_channel)
                    secondary_stub.WriteCommit(commit_request)
                    logger.debug(f"WriteCommit sent to secondary {cs_id}")
                except Exception as e:
                    logger.error(f"Error sending WriteCommit to secondary {cs_id}: {e}")

            with self.metadata.lock:
                chunk_meta.version += 1
                # Log the operation in the change log
                self.metadata.change_log.append({
                    'operation': 'AppendToFile',
                    'file_path': file_path,
                    'chunk_handle': chunk_meta.chunk_handle,
                    'version': chunk_meta.version,
                    'data_length': len(data),
                    'timestamp': time.time()
                })
                logger.debug(f"Chunk {chunk_meta.chunk_handle} version incremented to {chunk_meta.version}")
                logger.debug(f"Change log updated with AppendToFile operation for {file_path}.")

            return gfs_pb2.AppendResponse(success=True, message="Append successful")
        except Exception as e:
            logger.error(f"Error during AppendToFile operation: {e}")
            return gfs_pb2.AppendResponse(success=False, message=f"ERROR: {e}")

    def _assign_new_primary(self, chunk_meta):
        # with self.metadata.lock:
        if not chunk_meta.locations:
            logger.error(f"No available locations to assign primary for chunk {chunk_meta.chunk_handle}.")
            return
        # Implement selection logic based on server loads
        available_servers = [(cs_id, self.metadata.chunkserver_loads.get(cs_id, 0)) for cs_id in chunk_meta.locations]
        # Sort by load
        sorted_servers = sorted(available_servers, key=lambda x: x[1])
        chunk_meta.primary = sorted_servers[0][0]
        chunk_meta.lease_expiration = time.time() + Config.lease_duration
        logger.info(f"Assigned new primary {chunk_meta.primary} for chunk {chunk_meta.chunk_handle}")

    def ReadFromFile(self, request, context):
        file_path = request.file_path
        offset = request.offset
        length = request.length
        logger.info(f"ReadFromFile called for file {file_path}, offset {offset}, length {length}")
        with self.metadata.lock:
            file_meta = self.metadata.files.get(file_path)
            if not file_meta:
                logger.error(f"File not found: {file_path}")
                return gfs_pb2.ReadResponse(data=b'')
            chunk_meta = file_meta.chunks[0]  # Simplified for single chunk
            if not chunk_meta.locations:
                logger.error(f"No chunkservers available for chunk {chunk_meta.chunk_handle}")
                return gfs_pb2.ReadResponse(data=b'')
            cs_id = chunk_meta.locations[0]
        try:
            channel = grpc.insecure_channel(f'localhost:{cs_id}')
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            read_request = gfs_pb2.ChunkHandle(handle=chunk_meta.chunk_handle)
            read_response = stub.ReadChunk(read_request)
            data = read_response.data[offset:offset+length]
            logger.info(f"ReadFromFile successful for file {file_path}")
            return gfs_pb2.ReadResponse(data=data)
        except Exception as e:
            logger.error(f"Error during ReadFromFile operation: {e}")
            return gfs_pb2.ReadResponse(data=b'')

    def DeleteFile(self, request, context):
        file_path = request.value
        logger.info(f"DeleteFile called for file {file_path}")
        with self.metadata.lock:
            file_meta = self.metadata.files.pop(file_path, None)
            if file_meta:
                # Remove chunk mappings
                for chunk_meta in file_meta.chunks:
                    self.metadata.chunk_handle_map.pop(chunk_meta.chunk_handle, None)
                    # Notify chunkservers to delete chunks
                    for cs_id in chunk_meta.locations:
                        threading.Thread(target=self._delete_chunk_on_server, args=(chunk_meta.chunk_handle, cs_id), daemon=True).start()
                # Log the operation in the change log
                self.metadata.change_log.append({
                    'operation': 'DeleteFile',
                    'file_path': file_path,
                    'timestamp': time.time()
                })
                logger.debug(f"Change log updated with DeleteFile operation for {file_path}.")
                logger.info(f"File {file_path} deleted.")
                return gfs_pb2.StringResponse(value="File deleted")
            else:
                logger.error(f"File not found: {file_path}")
                return gfs_pb2.StringResponse(value="ERROR: File not found")

    def _delete_chunk_on_server(self, chunk_handle, cs_id):
        logger.info(f"Deleting chunk {chunk_handle} on chunkserver {cs_id}.")
        try:
            channel = grpc.insecure_channel(f'localhost:{cs_id}')
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            delete_request = gfs_pb2.ChunkHandle(handle=chunk_handle)
            # Assuming ChunkServer has DeleteChunk rpc method
            stub.DeleteChunk(delete_request)
            logger.info(f"Chunk {chunk_handle} deleted on chunkserver {cs_id}.")
        except Exception as e:
            logger.error(f"Error deleting chunk {chunk_handle} on chunkserver {cs_id}: {e}")

    def Heartbeat(self, request, context):
        chunkserver_id = request.chunkserver_id
        logger.debug(f"Heartbeat received from chunkserver {chunkserver_id}")
        with self.metadata.lock:
            self.metadata.chunkservers[chunkserver_id] = time.time()
            self.metadata.chunkserver_loads[chunkserver_id] = request.load  # New: storing server load
            if chunkserver_id not in self.metadata.chunkserver_chunks:
                self.metadata.chunkserver_chunks[chunkserver_id] = set()
            for chunk_handle in request.chunk_handles:
                self.metadata.chunkserver_chunks[chunkserver_id].add(chunk_handle)
        return gfs_pb2.HeartbeatResponse(success=True)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_MasterServerServicer_to_server(MasterServer(), server)
    server.add_insecure_port(f'localhost:{Config.master_port}')
    server.start()
    logger.info(f"Master server started on port {Config.master_port}.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        logger.info("Master server stopping...")
        server.stop(0)

if __name__ == '__main__':
    serve()