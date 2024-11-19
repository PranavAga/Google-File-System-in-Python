import asyncio
from concurrent import futures
import grpc
import gfs_pb2
import gfs_pb2_grpc

class MasterServer(gfs_pb2_grpc.MasterServiceServicer):
    def __init__(self):
        self.file_metadata = {}  # {filename: [chunk_handles]}
        self.chunk_locations = {}  # {chunk_handle: [location]}

    def CreateFile(self, request, context):
        filename = request.filename
        if filename in self.file_metadata:
            return gfs_pb2.FileResponse(message="File already exists.")
        self.file_metadata[filename] = []  # Initialize with no chunks
        return gfs_pb2.FileResponse(message="File created successfully.")

    def DeleteFile(self, request, context):
        filename = request.filename
        if filename in self.file_metadata:
            # Remove all associated chunks
            del self.file_metadata[filename]
            return gfs_pb2.FileResponse(message="File deleted successfully.")
        return gfs_pb2.FileResponse(message="File not found.")

    def FindLocations(self, request, context):
        filename = request.filename
        if filename not in self.file_metadata:
            return gfs_pb2.ChunkLocations(locations=[])
        chunk_handles = self.file_metadata[filename]
        locations = [self.chunk_locations[ch] for ch in chunk_handles if ch in self.chunk_locations]
        return gfs_pb2.ChunkLocations(locations=[loc for sublist in locations for loc in sublist])

    def GetFileLength(self, request, context):
        filename = request.filename
        if filename in self.file_metadata:
            return gfs_pb2.FileLength(length=len(self.file_metadata[filename]))
        return gfs_pb2.FileLength(length=0)

async def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_MasterServiceServicer_to_server(MasterServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    
    # Keep the server running until terminated
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        print("Keyboard interrupt received")
    finally:
        print("Shutting down master")
        server.stop(0)

if __name__ == '__main__':
    asyncio.run(serve())