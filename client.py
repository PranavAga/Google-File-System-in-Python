import grpc

import gfs_pb2
import gfs_pb2_grpc

from common import Config

def interactive_client():
    print("Welcome to the GFS Client")
    print("Type 'help' for a list of commands, or 'exit' to quit.")
    master_stub = get_master_stub()
    while True:
        cmd = input(">> ").strip()
        if not cmd:
            continue
        args = cmd.split()
        command = args[0].lower()
        if command == 'help':
            print("""
Available commands:
- create <file_path>
- append <file_path> <data>
- read <file_path> <offset> <length>
- list <directory>
- delete <file_path>
- exit
""")
        elif command == 'exit':
            print("Goodbye!")
            break
        elif command == 'create':
            if len(args) != 2:
                print("Usage: create <file_path>")
                continue
            file_path = args[1]
            response = master_stub.CreateFile(gfs_pb2.StringRequest(value=file_path))
            print(response.value)
        elif command == 'append':
            if len(args) < 3:
                print("Usage: append <file_path> <data>")
                continue
            file_path = args[1]
            data = ' '.join(args[2:]).encode()
            append_request = gfs_pb2.AppendRequest(file_path=file_path, data=data)
            response = master_stub.AppendToFile(append_request)
            print(response.message)
        elif command == 'read':
            if len(args) != 4:
                print("Usage: read <file_path> <offset> <length>")
                continue
            file_path = args[1]
            try:
                offset = int(args[2])
                length = int(args[3])
            except ValueError:
                print("Offset and length must be integers.")
                continue
            read_request = gfs_pb2.ReadRequest(
                file_path=file_path,
                offset=offset,
                length=length
            )
            response = master_stub.ReadFromFile(read_request)
            print("Read data:")
            print(response.data.decode())
        elif command == 'list':
            if len(args) != 2:
                print("Usage: list <directory>")
                continue
            directory = args[1]
            response = master_stub.ListFiles(gfs_pb2.StringRequest(value=directory))
            print("Files:")
            print(response.value)
        elif command == 'delete':
            if len(args) != 2:
                print("Usage: delete <file_path>")
                continue
            file_path = args[1]
            response = master_stub.DeleteFile(gfs_pb2.StringRequest(value=file_path))
            print(response.value)
        else:
            print("Unknown command. Type 'help' for a list of commands.")

def get_master_stub():
    channel = grpc.insecure_channel(f'localhost:{Config.master_port}')
    stub = gfs_pb2_grpc.MasterServerStub(channel)
    return stub

if __name__ == '__main__':
    interactive_client()