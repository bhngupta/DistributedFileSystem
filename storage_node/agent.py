import asyncio
import hashlib
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

# Basic logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get configuration values - use sensible defaults for development
my_node_id = os.getenv("NODE_ID", "node-001")
controller_endpoint = os.getenv("CONTROLLER_URL", "http://localhost:8000")
data_storage_path = Path(os.getenv("STORAGE_PATH", "/data"))
port_number = int(os.getenv("NODE_PORT", "8001"))
my_node_url = f"http://storage-node-{my_node_id}:{port_number}"

# Make sure we have a place to store files (but handle testing gracefully)
try:
    data_storage_path.mkdir(parents=True, exist_ok=True)
except (OSError, PermissionError):
    # If we can't create the default path (like during testing), use a temp directory
    import tempfile

    temp_dir = Path(tempfile.mkdtemp())
    data_storage_path = temp_dir
    print(
        f"Warning: Using temporary storage path {data_storage_path} due to permission issues"
    )


class StorageAgent:
    def __init__(self):
        self.storage_dir = data_storage_path
        self.controller_url = controller_endpoint
        self.node_id = my_node_id
        self.node_url = my_node_url

    async def register_with_controller(self):
        """Try to register ourselves with the main controller"""
        try:
            async with httpx.AsyncClient() as http_client:
                node_info = {
                    "node_id": self.node_id,
                    "url": self.node_url,
                    "capacity": self.calculate_storage_capacity(),
                }

                resp = await http_client.post(
                    f"{self.controller_url}/nodes/register",
                    json=node_info,
                    timeout=10.0,
                )

                if resp.status_code == 200:
                    logger.info(f"Node {self.node_id} registered successfully!")
                    return True
                else:
                    logger.error(f"Registration failed: {resp.status_code}")
                    return False
        except Exception as ex:
            logger.error(f"Couldn't register with controller: {str(ex)}")
            return False

    def calculate_storage_capacity(self) -> int:
        """Figure out how much storage space we have - capped at 100MB for testing"""
        try:
            # TODO: Can we get this dynamically?
            max_capacity = 100 * 1024 * 1024  # 100MB in bytes

            # Check actual disk space available
            disk_stats = os.statvfs(self.storage_dir)
            actual_bytes = disk_stats.f_frsize * disk_stats.f_blocks

            # Return the smaller of actual capacity or our 100MB limit
            return min(actual_bytes, max_capacity)
        except Exception:
            # Fallback - return 100MB default
            return 100 * 1024 * 1024  # 100MB default

    def calculate_used_space(self) -> int:
        """Calculate how much space we're actually using"""
        try:
            total_used = 0
            for root, dirs, files in os.walk(self.storage_dir):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    total_used += os.path.getsize(file_path)
            return total_used
        except Exception:
            return 0

    async def save_file_locally(self, file_id: str, file_content: bytes) -> bool:
        """Save a file to our local storage"""
        try:
            target_file = self.storage_dir / file_id

            # Write the actual file data
            with open(target_file, "wb") as f:
                f.write(file_content)

            # Also save some metadata about it
            file_metadata = {
                "file_id": file_id,
                "size": len(file_content),
                "checksum": hashlib.sha256(file_content).hexdigest(),
                "local_path": target_file.as_posix(),
            }

            meta_file = self.storage_dir / f"{file_id}.meta"
            with open(meta_file, "w") as f:
                json.dump(file_metadata, f)

            logger.info(f"Saved file {file_id} - {len(file_content)} bytes")
            return True

        except Exception as ex:
            logger.error(f"Failed to store file {file_id}: {str(ex)}")
            return False

    async def load_file_locally(self, file_id: str) -> bytes:
        """Load a file from our local storage"""
        try:
            target_file = self.storage_dir / file_id

            if not target_file.exists():
                raise HTTPException(status_code=404, detail="File not found")

            with open(target_file, "rb") as f:
                file_data = f.read()

            logger.info(f"Loaded file {file_id} - {len(file_data)} bytes")
            return file_data

        except Exception as ex:
            logger.error(f"Couldn't load file {file_id}: {str(ex)}")
            raise HTTPException(
                status_code=500, detail=f"Error loading file: {str(ex)}"
            )

    async def remove_file_locally(self, file_id: str) -> bool:
        """Remove a file from our local storage"""
        try:
            target_file = self.storage_dir / file_id
            meta_file = self.storage_dir / f"{file_id}.meta"

            # Clean up both the file and its metadata
            if target_file.exists():
                target_file.unlink()

            if meta_file.exists():
                meta_file.unlink()

            logger.info(f"Removed file {file_id} from storage")
            return True

        except Exception as ex:
            logger.error(f"Failed to remove file {file_id}: {str(ex)}")
            return False

    async def get_file_list(self) -> list:
        """Get a list of all files we have stored"""
        try:
            stored_files = []
            for meta_file in self.storage_dir.glob("*.meta"):
                try:
                    with open(meta_file, "r") as f:
                        file_info = json.load(f)
                    stored_files.append(file_info)
                except Exception as ex:
                    logger.error(f"Couldn't read metadata from {meta_file}: {str(ex)}")

            return stored_files
        except Exception as ex:
            logger.error(f"Error getting file list: {str(ex)}")
            return []


# Create our storage agent
storage_agent = StorageAgent()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan events"""
    # Startup
    logger.info(f"Storage node {my_node_id} is starting up...")
    await asyncio.sleep(5)  # Give the controller time to start
    await storage_agent.register_with_controller()
    yield
    # Shutdown (if needed)
    logger.info(f"Storage node {my_node_id} is shutting down...")


app = FastAPI(title="Storage Node Agent", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health_status():
    """Simple health check"""
    return {
        "status": "healthy",
        "node_id": my_node_id,
        "storage_path": str(data_storage_path),
        "used_space": storage_agent.calculate_used_space(),
        "capacity": storage_agent.calculate_storage_capacity(),
    }


@app.post("/store/{file_id}")
async def store_file_endpoint(file_id: str, request: Request):
    """Endpoint to store a file on this node"""
    try:
        file_content = await request.body()
        store_success = await storage_agent.save_file_locally(file_id, file_content)

        if store_success:
            return {"status": "stored", "file_id": file_id, "size": len(file_content)}
        else:
            raise HTTPException(status_code=500, detail="Couldn't store the file")
    except Exception as ex:
        logger.error(f"Store operation failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=str(ex))


@app.get("/retrieve/{file_id}")
async def retrieve_file_endpoint(file_id: str):
    """Endpoint to get a file from this node"""
    try:
        file_content = await storage_agent.load_file_locally(file_id)
        return Response(content=file_content, media_type="application/octet-stream")
    except HTTPException:
        raise
    except Exception as ex:
        logger.error(f"Retrieve operation failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=str(ex))


@app.delete("/delete/{file_id}")
async def delete_file_endpoint(file_id: str):
    """Endpoint to delete a file from this node"""
    try:
        delete_success = await storage_agent.remove_file_locally(file_id)

        if delete_success:
            return {"status": "deleted", "file_id": file_id}
        else:
            raise HTTPException(status_code=500, detail="Couldn't delete the file")
    except Exception as ex:
        logger.error(f"Delete operation failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=str(ex))


@app.get("/files")
async def list_files_endpoint():
    """Get a list of all files stored on this node"""
    try:
        file_list = await storage_agent.get_file_list()
        return {"files": file_list, "count": len(file_list)}
    except Exception as ex:
        logger.error(f"File listing failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=str(ex))


@app.get("/stats")
async def node_statistics():
    """Get some stats about this storage node"""
    return {
        "node_id": my_node_id,
        "capacity": storage_agent.calculate_storage_capacity(),
        "used_space": storage_agent.calculate_used_space(),
        "available_space": storage_agent.calculate_storage_capacity()
        - storage_agent.calculate_used_space(),
        "files_count": len(await storage_agent.get_file_list()),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=port_number)
