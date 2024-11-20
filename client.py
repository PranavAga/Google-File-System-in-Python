import grpc
import gfs_pb2
import gfs_pb2_grpc
import sys

from common import Config as cfg


def connect_to_master():
    channel = grpc.insecure_channel(f'localhost:{cfg.master_loc}')
    stub = gfs_pb2_grpc.MasterServerStub(channel)
    return stub


def interactive_client():
    stub = connect_to_master()
    print("Welcome to the GFS Client!")
    print("Please enter a command. Type 'help' for a list of commands, or 'exit' to quit.")
    
    while True:
        command_input = input(">> ").strip()
        if not command_input:
            continue
        args = command_input.split()
        command = args[0]

        if command == 'help':
            print("""
Available commands:
- create <file_path>               : Create a new file.
- append <file_path> <data>        : Append data to a file.
- read <file_path> <offset> <bytes>: Read data from a file starting at offset.
- list <directory_path>            : List files in a directory.
- delete <file_path>               : Delete a file.
- exit                             : Exit the client.
""")
        elif command == 'create':
            if len(args) != 2:
                print("Usage: create <file_path>")
                continue
            file_path = args[1]
            request = gfs_pb2.StringRequest(message=file_path)
            response = stub.CreateFile(request)
            print(response.message)
        elif command == 'append':
            if len(args) < 3:
                print("Usage: append <file_path> <data>")
                continue
            file_path = args[1]
            data = ' '.join(args[2:])
            request = gfs_pb2.StringRequest(message=f"{file_path}|{data}")
            response = stub.AppendToFile(request)
            print(response.message)
        elif command == 'read':
            if len(args) != 4:
                print("Usage: read <file_path> <offset> <bytes>")
                continue
            file_path = args[1]
            offset = args[2]
            num_bytes = args[3]
            request = gfs_pb2.ReadRequest(
                file_path=file_path,
                offset=int(offset),
                num_bytes=int(num_bytes)
            )
            response = stub.ReadFromFile(request)
            print("Data read:")
            print(response.data)
        elif command == 'list':
            if len(args) != 2:
                print("Usage: list <directory_path>")
                continue
            directory_path = args[1]
            request = gfs_pb2.StringRequest(message=directory_path)
            response = stub.ListFiles(request)
            print("Files:")
            print(response.message)
        elif command == 'delete':
            if len(args) != 2:
                print("Usage: delete <file_path>")
                continue
            file_path = args[1]
            request = gfs_pb2.StringRequest(message=file_path)
            response = stub.DeleteFile(request)
            print(response.message)
        elif command == 'exit':
            print("Exiting the client.")
            break
        else:
            print("Unknown command. Type 'help' for a list of commands.")


if __name__ == '__main__':
    interactive_client()