import os
import time
import grpc
from concurrent import futures
from multiprocessing import Process

import gfs_pb2_grpc
import gfs_pb2

from common import Config as cfg
from common import Status

import logging
from colorama import Fore, Style, init

# Initialize colorama for colored logs
init(autoreset=True)

# Configure logging
class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.MAGENTA
    }

    def format(self, record):
        log_fmt = f'{Fore.WHITE}%(asctime)s{Style.RESET_ALL} - {self.COLORS.get(record.levelname, "")}%(levelname)s{Style.RESET_ALL} - %(message)s'
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

logger = logging.getLogger('ChunkServer')
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(ColoredFormatter())
logger.addHandler(console_handler)

class ChunkServer:
    """
    ChunkServer handles chunk operations such as create, append, read,
    and provides chunk space information.
    """
    def __init__(self, port, root):
        """
        Initialize ChunkServer instance.

        :param port: Port number on which server is running.
        :param root: Root directory for chunk storage.
        """
        self.port = port
        self.root = root
        if not os.path.isdir(root):
            os.makedirs(root)
            logger.info(f"Created root directory at {root} for ChunkServer on port {self.port}")

    def create(self, chunk_handle):
        """
        Create a new chunk file.

        :param chunk_handle: Unique identifier for the chunk.
        :return: Status object indicating success or failure.
        """
        chunk_path = os.path.join(self.root, chunk_handle)
        try:
            with open(chunk_path, 'w'):
                pass
            logger.debug(f"Chunk {chunk_handle} created at {chunk_path}")
            return Status(0, f"Chunk {chunk_handle} created successfully")
        except Exception as e:
            logger.error(f"Failed to create chunk {chunk_handle} at {chunk_path}: {e}")
            return Status(-1, f"ERROR: {e}")

    def get_chunk_space(self, chunk_handle):
        """
        Get available space in chunk.

        :param chunk_handle: Unique identifier for the chunk.
        :return: Tuple of (available_space, Status object)
        """
        chunk_path = os.path.join(self.root, chunk_handle)
        try:
            current_size = os.stat(chunk_path).st_size
            available_space = cfg.CHUNK_SIZE - current_size
            logger.debug(f"Chunk {chunk_handle} has {available_space} bytes of available space")
            return str(available_space), Status(0, "")
        except Exception as e:
            logger.error(f"Failed to get chunk space for {chunk_handle} at {chunk_path}: {e}")
            return None, Status(-1, f"ERROR: {e}")

    def append(self, chunk_handle, data):
        """
        Append data to the chunk.

        :param chunk_handle: Unique identifier for the chunk.
        :param data: Data to append.
        :return: Status object indicating success or failure.
        """
        chunk_path = os.path.join(self.root, chunk_handle)
        try:
            with open(chunk_path, 'a') as f:
                f.write(data)
            logger.debug(f"Appended data to chunk {chunk_handle}: {data}")
            return Status(0, f"Data appended to chunk {chunk_handle} successfully")
        except Exception as e:
            logger.error(f"Failed to append data to chunk {chunk_handle} at {chunk_path}: {e}")
            return Status(-1, f"ERROR: {e}")

    def read(self, chunk_handle, start_offset, numbytes):
        """
        Read data from the chunk.

        :param chunk_handle: Unique identifier for the chunk.
        :param start_offset: Starting byte offset.
        :param numbytes: Number of bytes to read.
        :return: Status object with data or error message.
        """
        chunk_path = os.path.join(self.root, chunk_handle)
        try:
            with open(chunk_path, 'r') as f:
                f.seek(start_offset)
                data = f.read(numbytes)
            logger.debug(f"Read data from chunk {chunk_handle}: {data}")
            return Status(0, data)
        except Exception as e:
            logger.error(f"Failed to read data from chunk {chunk_handle} at {chunk_path}: {e}")
            return Status(-1, f"ERROR: {e}")

class ChunkServerToClientServicer(gfs_pb2_grpc.ChunkServerToClientServicer):
    """
    Provides methods that implement functionality of chunk server's RPC interface.
    """
    def __init__(self, chunk_server):
        """
        Initialize servicer with a ChunkServer instance.

        :param chunk_server: Instance of ChunkServer.
        """
        self.chunk_server = chunk_server
        self.port = self.chunk_server.port

    def Create(self, request, context):
        """
        Handle Create RPC call.

        :param request: gfs_pb2.String containing chunk_handle.
        :param context: RPC context.
        :return: gfs_pb2.String containing status message.
        """
        chunk_handle = request.st
        logger.info(f"Received Create request for chunk {chunk_handle} on port {self.port}")
        status = self.chunk_server.create(chunk_handle)
        return gfs_pb2.String(st=status.e)

    def GetChunkSpace(self, request, context):
        """
        Handle GetChunkSpace RPC call.

        :param request: gfs_pb2.String containing chunk_handle.
        :param context: RPC context.
        :return: gfs_pb2.String containing available space or error message.
        """
        chunk_handle = request.st
        logger.info(f"Received GetChunkSpace request for chunk {chunk_handle} on port {self.port}")
        chunk_space, status = self.chunk_server.get_chunk_space(chunk_handle)
        if status.v != 0:
            return gfs_pb2.String(st=status.e)
        else:
            return gfs_pb2.String(st=chunk_space)

    def Append(self, request, context):
        """
        Handle Append RPC call.

        :param request: gfs_pb2.String containing chunk_handle and data separated by '|'.
        :param context: RPC context.
        :return: gfs_pb2.String containing status message.
        """
        try:
            chunk_handle, data = request.st.split("|", 1)
        except ValueError:
            error_msg = "Invalid append request format"
            logger.error(error_msg)
            return gfs_pb2.String(st=f"ERROR: {error_msg}")

        logger.info(f"Received Append request for chunk {chunk_handle} on port {self.port}")
        status = self.chunk_server.append(chunk_handle, data)
        return gfs_pb2.String(st=status.e)

    def Read(self, request, context):
        """
        Handle Read RPC call.

        :param request: gfs_pb2.String containing chunk_handle, start_offset, numbytes separated by '|'.
        :param context: RPC context.
        :return: gfs_pb2.String containing data read or error message.
        """
        try:
            chunk_handle, start_offset, numbytes = request.st.split("|", 2)
            start_offset = int(start_offset)
            numbytes = int(numbytes)
        except ValueError:
            error_msg = "Invalid read request format"
            logger.error(error_msg)
            return gfs_pb2.String(st=f"ERROR: {error_msg}")

        logger.info(f"Received Read request for chunk {chunk_handle} from offset {start_offset} for {numbytes} bytes on port {self.port}")
        status = self.chunk_server.read(chunk_handle, start_offset, numbytes)
        return gfs_pb2.String(st=status.e)

def start(port):
    """
    Start Chunk Server at the specified port.

    :param port: Port number to start the server on.
    """
    logger.info(f"Starting ChunkServer on port {port}")

    chunk_server = ChunkServer(port=port, root=os.path.join(cfg.CHUNKSERVER_ROOT, port))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    gfs_pb2_grpc.add_ChunkServerToClientServicer_to_server(
        ChunkServerToClientServicer(chunk_server),
        server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"ChunkServer started on port {port}")
    try:
        while True:
            time.sleep(200000)
    except KeyboardInterrupt:
        logger.info(f"ChunkServer on port {port} shutting down")
        server.stop(0)

if __name__ == "__main__":
    processes = []
    for loc in cfg.CHUNKSERVER_LOCS:
        p = Process(target=start, args=(loc,))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("ChunkServers shutting down")
        for p in processes:
            p.terminate()