"""

SDK for DFS

"""

import asyncio
import sys
from pathlib import Path

import httpx


class StorageClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url

    async def upload_file(self, file_path: str) -> dict:
        """Upload a file to the distributed storage"""
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        async with httpx.AsyncClient() as client:
            with open(file_path, "rb") as f:
                files = {"file": (file_path.name, f, "application/octet-stream")}
                response = await client.post(
                    f"{self.base_url}/files/upload", files=files
                )

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"Upload failed: {response.status_code} - {response.text}"
                )

    async def download_file(self, file_id: str, output_path: str = None) -> str:
        """Download a file from the distributed storage"""
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/files/{file_id}")

            if response.status_code == 200:
                # Determine output filename
                if output_path is None:
                    content_disposition = response.headers.get(
                        "content-disposition", ""
                    )
                    if "filename=" in content_disposition:
                        filename = content_disposition.split("filename=")[1].strip('"')
                        output_path = f"downloaded_{filename}"
                    else:
                        output_path = f"downloaded_{file_id}"

                # Write file content
                with open(output_path, "wb") as f:
                    f.write(response.content)

                return output_path
            else:
                raise Exception(
                    f"Download failed: {response.status_code} - {response.text}"
                )

    async def list_files(self) -> dict:
        """List all files in the storage system"""
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/files")

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"List failed: {response.status_code} - {response.text}"
                )

    async def delete_file(self, file_id: str) -> dict:
        """Delete a file from the storage system"""
        async with httpx.AsyncClient() as client:
            response = await client.delete(f"{self.base_url}/files/{file_id}")

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"Delete failed: {response.status_code} - {response.text}"
                )

    async def get_nodes(self) -> dict:
        """Get information about storage nodes"""
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/nodes")

            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(
                    f"Get nodes failed: {response.status_code} - {response.text}"
                )


async def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python client.py upload <file_path>")
        print("  python client.py download <file_id> [output_path]")
        print("  python client.py list")
        print("  python client.py delete <file_id>")
        print("  python client.py nodes")
        return

    client = StorageClient()
    command = sys.argv[1]

    try:
        if command == "upload":
            if len(sys.argv) < 3:
                print("Error: Please provide file path")
                return

            file_path = sys.argv[2]
            result = await client.upload_file(file_path)
            print(f"File uploaded successfully!")
            print(f"File ID: {result['file_id']}")
            print(f"Filename: {result['filename']}")
            print(f"Size: {result['size']} bytes")
            print(f"Stored on nodes: {', '.join(result['nodes'])}")

        elif command == "download":
            if len(sys.argv) < 3:
                print("Error: Please provide file ID")
                return

            file_id = sys.argv[2]
            output_path = sys.argv[3] if len(sys.argv) > 3 else None

            downloaded_path = await client.download_file(file_id, output_path)
            print(f"File downloaded successfully to: {downloaded_path}")

        elif command == "list":
            result = await client.list_files()
            files = result.get("files", [])

            if not files:
                print("No files found in storage")
            else:
                print(f"Found {len(files)} files:")
                print("-" * 80)
                for file_info in files:
                    print(f"ID: {file_info['file_id']}")
                    print(f"Name: {file_info['filename']}")
                    print(f"Size: {file_info['size']} bytes")
                    print(f"Created: {file_info['created_at']}")
                    print(f"Checksum: {file_info['checksum']}")
                    print("-" * 80)

        elif command == "delete":
            if len(sys.argv) < 3:
                print("Error: Please provide file ID")
                return

            file_id = sys.argv[2]
            result = await client.delete_file(file_id)
            print(f"File deleted successfully!")
            print(f"File ID: {result['file_id']}")
            print(f"Nodes cleaned: {', '.join(result['nodes_cleaned'])}")

        elif command == "nodes":
            result = await client.get_nodes()
            nodes = result.get("nodes", [])

            if not nodes:
                print("No storage nodes found")
            else:
                print(f"Found {len(nodes)} storage nodes:")
                print("-" * 80)
                for node in nodes:
                    print(f"Node ID: {node['node_id']}")
                    print(f"URL: {node['url']}")
                    print(f"Capacity: {node['capacity']:,} bytes")
                    print(f"Used: {node['used_space']:,} bytes")
                    print(f"Available: {node['capacity'] - node['used_space']:,} bytes")
                    print(f"Last Heartbeat: {node['last_heartbeat']}")
                    print("-" * 80)

        else:
            print(f"Unknown command: {command}")

    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
