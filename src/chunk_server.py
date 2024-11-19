import asyncio
from concurrent import futures
import grpc
import gfs_pb2
import gfs_pb2_grpc

class ChunkServer(gfs_pb2_grpc.ChunkServerServiceServicer):
    def __init__(self):
        self.chunks = {}  # {chunk_handle: data}

    def ReadChunk(self, request, context):
        chunk_handle = request.chunk_handle
        data = self.chunks.get(chunk_handle, b'')
        return gfs_pb2.ChunkData(data=data)

    def WriteChunk(self, request, context):
        chunk_handle = request.chunk_handle
        self.chunks[chunk_handle] = request.data
        return gfs_pb2.ChunkResponse(message="Write successful.")

async def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_ChunkServerServiceServicer_to_server(ChunkServer(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    
    # Keep the server running until terminated
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        print("Keyboard interrupt received")
    finally:
        print("Shutting down chunk server")
        server.stop(0)

if __name__ == '__main__':
    asyncio.run(serve())