import sys
import grpc
import gfs_pb2_grpc
import gfs_pb2

from common import Config as cfg

def connect_to_master():
    channel = grpc.insecure_channel(f'localhost:{cfg.master_loc}')
    stub = gfs_pb2_grpc.MasterServerStub(channel)
    return stub

def create_file(file_path):
    stub = connect_to_master()
    response = stub.CreateFile(gfs_pb2.String(st=file_path))
    print(response.st)

def append_to_file(file_path, data):
    stub = connect_to_master()
    # Implementation of writing data using two-phase commit
    pass

def read_from_file(file_path):
    stub = connect_to_master()
    # Implementation of reading data from file
    pass

def list_files(directory):
    stub = connect_to_master()
    response = stub.ListFiles(gfs_pb2.String(st=directory))
    print(response.st)

def delete_file(file_path):
    stub = connect_to_master()
    response = stub.DeleteFile(gfs_pb2.String(st=file_path))
    print(response.st)

def undelete_file(file_path):
    stub = connect_to_master()
    response = stub.UndeleteFile(gfs_pb2.String(st=file_path))
    print(response.st)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python client.py <command> <file_path> [data]")
        sys.exit(1)
    command = sys.argv[1]
    file_path = sys.argv[2]
    if command == "create":
        create_file(file_path)
    elif command == "append":
        if len(sys.argv) < 4:
            print("Usage: python client.py append <file_path> <data>")
            sys.exit(1)
        data = sys.argv[3]
        append_to_file(file_path, data)
    elif command == "read":
        read_from_file(file_path)
    elif command == "list":
        list_files(file_path)
    elif command == "delete":
        delete_file(file_path)
    elif command == "undelete":
        undelete_file(file_path)
    else:
        print("Unknown command")