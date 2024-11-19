import grpc
import gfs_pb2
import gfs_pb2_grpc

def run():
    with grpc.insecure_channel('localhost:50051') as master_channel:
        master_stub = gfs_pb2_grpc.MasterServiceStub(master_channel)

        print("Welcome to GFS Client")
        while True:
            command = input("Enter command (create, read, write, delete, exit): ").strip()
            if command == "exit":
                break
            filename = input("Enter filename: ").strip()
            if command == "create":
                response = master_stub.CreateFile(gfs_pb2.FileRequest(filename=filename))
                print(response.message)
            elif command == "delete":
                response = master_stub.DeleteFile(gfs_pb2.FileRequest(filename=filename))
                print(response.message)
            elif command == "read":
                # TODO: Implement read logic
                pass
            elif command == "write":
                # TODO: Implement write logic
                pass
            else:
                print("Unknown command")

if __name__ == '__main__':
    run()