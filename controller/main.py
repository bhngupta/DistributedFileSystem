import asyncio
import io
import logging
import uuid
from contextlib import asynccontextmanager
from typing import List, Optional

import httpx
import yaml
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from accounting.models import NodeStorageUsage, StorageNode, get_accounting_db

from .config_validation import get_config_service
from .database import FileMetadata, StorageNode, get_db_session, init_database
from .models import FileMetadataModel, StorageNodeModel
from .services import FileService, NodeService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize services
file_svc = FileService()
node_svc = NodeService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up the storage controller...")
    logger.info("Initializing database...")
    init_database()
    logger.info("Database initialized successfully")
    await node_svc.discover_nodes()
    yield
    # Shutdown
    logger.info("Shutting down the storage controller...")


app = FastAPI(
    title="Distributed Storage Controller", version="1.0.0", lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Simple health check"""
    return {"status": "healthy", "service": "controller"}


@app.get("/nodes")
async def get_storage_nodes():
    """List all the storage nodes we know about"""
    active_nodes = await node_svc.get_active_nodes()
    return {"nodes": active_nodes}


@app.post("/files/upload")
async def upload_file_to_storage(uploaded_file: UploadFile = File(...)):
    """Handle file uploads - spread them across our storage nodes"""
    try:
        # Generate a unique ID for this file
        new_file_id = str(uuid.uuid4())
        file_data = await uploaded_file.read()

        # Store the file across multiple nodes for safety
        storage_results = await file_svc.store_file(
            file_id=new_file_id, filename=uploaded_file.filename, content=file_data
        )

        return {
            "file_id": new_file_id,
            "filename": uploaded_file.filename,
            "size": len(file_data),
            "nodes": storage_results["nodes"],
            "status": "uploaded",
        }
    except Exception as ex:
        logger.error(f"Upload went wrong: {str(ex)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(ex)}")


@app.get("/files/{file_id}")
async def download_file_by_id(file_id: str):
    """Download a file using its ID"""
    try:
        file_info = await file_svc.retrieve_file(file_id)

        if not file_info:
            raise HTTPException(status_code=404, detail="File not found")

        # Stream the file back to the client
        return StreamingResponse(
            io.BytesIO(file_info["content"]),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={file_info['filename']}"
            },
        )
    except Exception as ex:
        logger.error(f"Download failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=f"Download failed: {str(ex)}")


@app.get("/files")
async def list_all_files():
    """Get a list of all files in storage"""
    file_list = await file_svc.list_files()
    return {"files": file_list}


@app.delete("/files/{file_id}")
async def remove_file(file_id: str):
    """Delete a file from all storage nodes"""
    try:
        delete_result = await file_svc.delete_file(file_id)
        return {
            "file_id": file_id,
            "status": "deleted",
            "nodes_cleaned": delete_result["nodes_cleaned"],
        }
    except Exception as ex:
        logger.error(f"Delete operation failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(ex)}")


@app.post("/nodes/heartbeat")
async def node_heartbeat(heartbeat_data: dict):
    """Receive heartbeat from storage nodes"""
    node_id = heartbeat_data.get("node_id")
    if not node_id:
        raise HTTPException(status_code=400, detail="node_id required")

    success = await node_svc.update_node_heartbeat(node_id)
    if success:
        return {"status": "heartbeat_received", "node_id": node_id}
    else:
        raise HTTPException(status_code=404, detail="Node not found")


@app.get("/nodes/health")
async def get_nodes_health():
    """Get overall health status of the storage cluster"""
    health_status = await node_svc.check_node_health()
    return health_status


@app.post("/nodes/register")
async def register_storage_node(node_info: dict):
    """Register a new storage node with the controller"""
    node_id = node_info.get("node_id")
    node_url = node_info.get("url")
    node_capacity = node_info.get("capacity", 1024 * 1024 * 1024)  # Default 1GB

    registration_result = await node_svc.register_node(node_id, node_url, node_capacity)
    return {"status": "registered", "node_id": node_id}


@app.post("/metrics")
async def receive_metrics(metrics: dict, db: Session = Depends(get_accounting_db)):
    """Receive and store metrics in the database"""
    try:
        node_id = metrics.get("node_id")
        node = db.query(StorageNode).filter(StorageNode.node_id == node_id).first()
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")

        # Update node metrics
        node.available_capacity = metrics["disk_usage"]["free"]
        node.allocated_capacity = metrics["disk_usage"]["used"]
        node.avg_response_time_ms = metrics.get(
            "response_time_ms", node.avg_response_time_ms
        )
        node.last_health_check = metrics.get("timestamp", node.last_health_check)

        db.commit()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/validate-config")
async def validate_node_config(config_path: str):
    """Endpoint to validate node configuration"""
    service = get_config_service()
    config = service.load_config(config_path)
    service.validate_config(config, expected_quota=1000, expected_replication_factor=3)
    alerts = service.get_alerts()

    if alerts:
        return {"status": "validation_failed", "alerts": alerts}
    return {"status": "validation_success", "message": "All configurations are valid."}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
