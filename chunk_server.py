from concurrent import futures
import time
import os
import threading

import grpc
import gfs_pb2_grpc
import gfs_pb2

from common import Config as cfg
from common import Status

class ChunkServer(object):
    def __init__(self, port):
        self.port = port
        self.lock = threading.Lock()
        self.root = os.path.join(cfg.chunkserver_root, self.port)
        os.makedirs(self.root, exist_ok=True)
        self.chunks = {}  # Map chunk_handle to file path

    def get_chunk_path(self, chunk_handle):
        return os.path.join(self.root, chunk_handle)

    def create_chunk(self, chunk_handle):
        chunk_path = self.get_chunk_path(chunk_handle)
        if os.path.exists(chunk_path):
            return Status(-1, "ERROR: Chunk {} already exists".format(chunk_handle))
        with open(chunk_path, 'w') as f:
            pass  # Create an empty file
        with self.lock:
            self.chunks[chunk_handle] = chunk_path
        return Status(0, "SUCCESS: Chunk {} created".format(chunk_handle))

    def get_chunk_space(self, chunk_handle):
        chunk_path = self.get_chunk_path(chunk_handle)
        if not os.path.exists(chunk_path):
            return Status(-1, "ERROR: Chunk {} does not exist".format(chunk_handle))
        chunk_size = os.path.getsize(chunk_path)
        remaining_space = cfg.chunk_size - chunk_size
        if remaining_space < 0:
            remaining_space = 0
        return Status(remaining_space, "SUCCESS: Remaining space is {}".format(remaining_space))

    def append_chunk(self, chunk_handle, data):
        chunk_path = self.get_chunk_path(chunk_handle)
        if not os.path.exists(chunk_path):
            return Status(-1, "ERROR: Chunk {} does not exist".format(chunk_handle))
        with self.lock:
            with open(chunk_path, 'a') as f:
                f.write(data)
        return Status(0, "SUCCESS: Data appended to chunk {}".format(chunk_handle))

    def read_chunk(self, chunk_handle, start_offset, numbytes):
        chunk_path = self.get_chunk_path(chunk_handle)
        if not os.path.exists(chunk_path):
            return Status(-1, "ERROR: Chunk {} does not exist".format(chunk_handle))
        with open(chunk_path, 'r') as f:
            f.seek(start_offset)
            data = f.read(numbytes)
        return Status(0, data)

class ChunkServerToClientServicer(gfs_pb2_grpc.ChunkServerToClientServicer):
    def __init__(self, chunkserver):
        self.chunkserver = chunkserver

    def Create(self, request, context):
        chunk_handle = request.st
        print("Command Create {}".format(chunk_handle))
        status = self.chunkserver.create_chunk(chunk_handle)
        return gfs_pb2.String(st=status.e)

    def GetChunkSpace(self, request, context):
        chunk_handle = request.st
        print("Command GetChunkSpace {}".format(chunk_handle))
        status = self.chunkserver.get_chunk_space(chunk_handle)
        if status.v >= 0:
            return gfs_pb2.String(st=str(status.v))
        else:
            return gfs_pb2.String(st=status.e)

    def Append(self, request, context):
        st = request.st
        chunk_handle, data = st.split("|", 1)
        print("Command Append {} {}".format(chunk_handle, data))
        status = self.chunkserver.append_chunk(chunk_handle, data)
        return gfs_pb2.String(st=status.e)

    def Read(self, request, context):
        st = request.st
        chunk_handle, start_offset, numbytes = st.split("|")
        print("Command Read {} {} {}".format(chunk_handle, start_offset, numbytes))
        status = self.chunkserver.read_chunk(chunk_handle, int(start_offset), int(numbytes))
        if status.v == 0:
            return gfs_pb2.String(st=status.e)
        else:
            return gfs_pb2.String(st=status.e)

def serve():
    # Get the port from configuration or command-line argument
    import sys
    if len(sys.argv) > 1:
        port = sys.argv[1]
    else:
        # Use the first port in the config if not specified
        port = cfg.chunkserver_locs[0]
    chunkserver = ChunkServer(port)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    gfs_pb2_grpc.add_ChunkServerToClientServicer_to_server(
        ChunkServerToClientServicer(chunkserver=chunkserver), server)
    server.add_insecure_port('[::]:{}'.format(port))
    server.start()
    print("Chunk server started on port {}".format(port))
    try:
        while True:
            time.sleep(2000)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()