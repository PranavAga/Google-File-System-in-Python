## README

### Google File System (GFS) Simplified Implementation

This project is a simplified implementation of the Google File System (GFS) using Python and gRPC. The system consists of a master server, multiple chunk servers, and a client to interact with the file system.

### Table of Contents

- [README](#readme)
  - [Google File System (GFS) Simplified Implementation](#google-file-system-gfs-simplified-implementation)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Directory Structure](#directory-structure)
  - [Implementation Details](#implementation-details)
  - [Prerequisites](#prerequisites)
  - [Setup and Compilation](#setup-and-compilation)
  - [Running the System](#running-the-system)
  - [Client Usage](#client-usage)
  - [Example Commands](#example-commands)
  - [Notes](#notes)

### Overview

The system provides basic file system operations such as:

- **Create**: Create a new file in the system.
- **List**: List files in a directory.
- **Append**: Append data to a file.
- **Read**: Read data from a file.
- **Delete**: Mark a file as deleted.
- **Undelete**: Restore a deleted file.

### Directory Structure

```
gfs_project/
├── client.py
├── common.py
├── master_server.py
├── chunk_server.py
├── gfs.proto
├── gfs_pb2.py
├── gfs_pb2_grpc.py
└── root_chunkserver/
    ├── 50052/
    ├── 50053/
    ├── 50054/
    ├── 50055/
    └── 50056/
```

- `client.py`: The client application to interact with the GFS.
- `common.py`: Common configurations and utilities.
- `master_server.py`: The master server managing metadata.
- `chunk_server.py`: The chunk server storing file chunks.
- `gfs.proto`: Protocol buffer definitions.
- `gfs_pb2.py` and `gfs_pb2_grpc.py`: Generated files from `gfs.proto`.
- `root_chunkserver/`: Directory where chunk servers store file chunks.

### Implementation Details

- **Master Server**:
  - Manages metadata about files and chunks.
  - Handles client requests for file operations.
  - Assigns chunk handles and chooses chunk server locations.

- **Chunk Server**:
  - Stores actual file chunks.
  - Responds to client requests for reading and writing data.

- **Client**:
  - Provides a command-line interface for users.
  - Interacts with the master and chunk servers via gRPC.

### Prerequisites

- Python 3.x
- `grpcio` and `grpcio-tools` libraries
- Protocol Buffers compiler (`protoc`)

### Setup and Compilation

1. **Install Required Libraries**:

   ```bash
   pip install grpcio grpcio-tools
   ```

2. **Generate gRPC Code from Protobuf Definitions**:

   Make sure you have the `gfs.proto` file defining the services and messages.

   ```bash
   python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. gfs.proto
   ```

   This will generate `gfs_pb2.py` and `gfs_pb2_grpc.py`.

3. **Create Root Directory for Chunk Servers**:

   ```bash
   mkdir root_chunkserver
   ```

### Running the System

1. **Start the Master Server**:

   ```bash
   python master_server.py
   ```

2. **Start Chunk Servers**:

   Run the following command to start all chunk servers. Each chunk server runs on a different port specified in `Config.chunkserver_locs`.

   ```bash
   python chunk_server.py
   ```

   *Note*: Ensure that the ports specified in `cfg.chunkserver_locs` are available.

### Client Usage

The client provides several commands:

- **Create a File**:

  ```bash
  python client.py create <file_path>
  ```

- **List Files in a Directory**:

  ```bash
  python client.py list <directory_path>
  ```

- **Append Data to a File**:

  ```bash
  python client.py append <file_path> <data>
  ```

- **Read Data from a File**:

  ```bash
  python client.py read <file_path> <byte_offset> <num_bytes>
  ```

- **Delete a File**:

  ```bash
  python client.py delete <file_path>
  ```

- **Undelete a File**:

  ```bash
  python client.py undelete <file_path>
  ```

### Example Commands

1. **Create a New File**:

   ```bash
   python client.py create /myfile
   ```

2. **Append Data to the File**:

   ```bash
   python client.py append /myfile "Hello, GFS!"
   ```

3. **Read Data from the File**:

   ```bash
   python client.py read /myfile 0 10
   ```

4. **List Files in Root Directory**:

   ```bash
   python client.py list /
   ```

5. **Delete the File**:

   ```bash
   python client.py delete /myfile
   ```

6. **Undelete the File**:

   ```bash
   python client.py undelete /myfile
   ```

### Notes

- **Chunk Size**: The chunk size is specified in `common.py` via `Config.chunk_size`. Adjust it as needed.

- **Error Handling**: The system prints error messages for invalid operations or if issues occur during file operations.

- **Data Persistence**: The chunk servers store data in the `root_chunkserver` directory under their respective port numbers.