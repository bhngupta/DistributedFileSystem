import asyncio
import hashlib
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

my_node_id = os.getenv("NODE_ID", "node-001")
controller_endpoint = os.getenv("CONTROLLER_URL", "http://localhost:8000")
data_storage_path = Path(os.getenv("STORAGE_PATH", "/data"))
port_number = int(os.getenv("NODE_PORT", "8001"))
my_node_url = f"http://storage-node-{my_node_id}:{port_number}"

try:
    data_storage_path.mkdir(parents=True, exist_ok=True)
except (OSError, PermissionError):
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

        # Metrics tracking
        self.metrics = {
            "upload_ops_count": 0,
            "download_ops_count": 0,
            "delete_ops_count": 0,
            "response_times": [],
        }

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

    async def send_heartbeat(self):

        try:
            async with httpx.AsyncClient() as http_client:
                heartbeat_data = {
                    "node_id": self.node_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "healthy",
                }

                resp = await http_client.post(
                    f"{self.controller_url}/nodes/heartbeat",
                    json=heartbeat_data,
                    timeout=5.0,
                )

                if resp.status_code == 200:
                    logger.debug(f"Heartbeat sent successfully for node {self.node_id}")
                    return True
                else:
                    logger.warning(f"Heartbeat failed: {resp.status_code}")
                    return False
        except Exception as ex:
            logger.warning(f"Couldn't send heartbeat: {str(ex)}")
            return False

    def calculate_storage_capacity(self) -> int:
        return 100 * 1024 * 1024  # 100MB

    def calculate_used_space(self) -> int:
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
        try:
            target_file = self.storage_dir / file_id
            with open(target_file, "wb") as f:
                f.write(file_content)
            file_metadata = {
                "file_id": file_id,
                "size": len(file_content),
                "checksum": hashlib.sha256(file_content).hexdigest(),
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
        try:
            target_file = self.storage_dir / file_id
            meta_file = self.storage_dir / f"{file_id}.meta"
            if target_file.exists():
                target_file.unlink()
            if meta_file.exists():
                meta_file.unlink()
            logger.info(f"Removed file {file_id}")
            return True
        except Exception as ex:
            logger.error(f"Failed to remove file {file_id}: {str(ex)}")
            return False

    async def get_file_list(self) -> list:
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

    def record_operation(self, operation_type: str, response_time_ms: float = 0):

        if operation_type == "upload":
            self.metrics["upload_ops_count"] += 1
        elif operation_type == "download":
            self.metrics["download_ops_count"] += 1
        elif operation_type == "delete":
            self.metrics["delete_ops_count"] += 1

        if response_time_ms > 0:
            self.metrics["response_times"].append(response_time_ms)

            # only last 100 response times
            if len(self.metrics["response_times"]) > 100:
                self.metrics["response_times"] = self.metrics["response_times"][-100:]

    def get_current_metrics(self) -> dict:

        import psutil

        total_storage = self.calculate_storage_capacity()
        used_storage = self.calculate_used_space()
        available_storage = total_storage - used_storage
        files_count = len(list(self.storage_dir.glob("*.meta")))

        avg_response_time = 0.0
        if self.metrics["response_times"]:
            avg_response_time = sum(self.metrics["response_times"]) / len(
                self.metrics["response_times"]
            )

        return {
            "total_storage_bytes": total_storage,
            "used_storage_bytes": used_storage,
            "available_storage_bytes": available_storage,
            "files_count": files_count,
            "upload_ops_count": self.metrics["upload_ops_count"],
            "download_ops_count": self.metrics["download_ops_count"],
            "delete_ops_count": self.metrics["delete_ops_count"],
            "avg_response_time_ms": avg_response_time,
            "is_healthy": True,
            "cpu_usage_percent": psutil.cpu_percent(),
            "memory_usage_percent": psutil.virtual_memory().percent,
        }

    async def send_metrics_to_controller(self):

        try:
            metrics = self.get_current_metrics()
            async with httpx.AsyncClient() as http_client:
                resp = await http_client.post(
                    f"{self.controller_url}/metrics/nodes/{self.node_id}",
                    json=metrics,
                    timeout=10.0,
                )

                if resp.status_code == 200:
                    logger.debug(f"Metrics sent successfully for node {self.node_id}")
                    return True
                else:
                    logger.warning(f"Failed to send metrics: {resp.status_code}")
                    return False
        except Exception as ex:
            logger.warning(f"Couldn't send metrics: {str(ex)}")
            return False


storage_agent = StorageAgent()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan events"""
    # Startup
    logger.info(f"Storage node {my_node_id} is starting up...")
    await asyncio.sleep(5)  # Give the controller time to start
    await storage_agent.register_with_controller()

    # Start heartbeat task
    heartbeat_task = asyncio.create_task(heartbeat_loop())

    yield

    # Shutdown
    logger.info(f"Storage node {my_node_id} is shutting down...")
    heartbeat_task.cancel()


async def heartbeat_loop():
    """Background task to send regular heartbeats and metrics"""
    while True:
        try:
            await asyncio.sleep(15)  # 15 seconds
            await storage_agent.send_heartbeat()
            await storage_agent.send_metrics_to_controller()  # Send metrics with heartbeat
        except Exception as e:
            logger.error(f"Error in heartbeat loop: {str(e)}")
            await asyncio.sleep(30)  # Wait longer on error


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
    start_time = time.time()
    try:
        file_content = await request.body()
        store_success = await storage_agent.save_file_locally(file_id, file_content)

        if store_success:
            response_time = (time.time() - start_time) * 1000  # Convert to ms
            storage_agent.record_operation("upload", response_time)
            return {"status": "stored", "file_id": file_id, "size": len(file_content)}
        else:
            raise HTTPException(status_code=500, detail="Couldn't store the file")
    except Exception as ex:
        logger.error(f"Store operation failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=str(ex))


@app.get("/retrieve/{file_id}")
async def retrieve_file_endpoint(file_id: str):
    """Endpoint to get a file from this node"""
    start_time = time.time()
    try:
        file_content = await storage_agent.load_file_locally(file_id)
        response_time = (time.time() - start_time) * 1000  # Convert to ms
        storage_agent.record_operation("download", response_time)
        return Response(content=file_content, media_type="application/octet-stream")
    except HTTPException:
        raise
    except Exception as ex:
        logger.error(f"Retrieve operation failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=str(ex))


@app.delete("/delete/{file_id}")
async def delete_file_endpoint(file_id: str):
    """Endpoint to delete a file from this node"""
    start_time = time.time()
    try:
        delete_success = await storage_agent.remove_file_locally(file_id)

        if delete_success:
            response_time = (time.time() - start_time) * 1000  # Convert to ms
            storage_agent.record_operation("delete", response_time)
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
