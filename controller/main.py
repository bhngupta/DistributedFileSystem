from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
import asyncio
import uuid
from typing import List, Optional
import httpx
import io
from .database import get_db_session
from .models import FileMetadata, StorageNode
from .services import FileService, NodeService
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Distributed Storage Controller", version="1.0.0")

file_service = FileService()
node_service = NodeService()

@app.on_event("startup")
async def startup_event():
    """Initialize the controller and discover storage nodes"""
    logger.info("Starting Distributed Storage Controller")
    await node_service.discover_nodes()

@app.get("/health")
async def health_check():
    
    return {"status": "healthy", "service": "controller"}

@app.get("/nodes")
async def list_nodes():
    
    nodes = await node_service.get_active_nodes()
    return {"nodes": nodes}

@app.post("/files/upload")
async def upload_file(file: UploadFile = File(...)):
    
    try:
        file_id = str(uuid.uuid4())
        file_content = await file.read()
        
        # Store file across multiple nodes for redundancy
        storage_result = await file_service.store_file(
            file_id=file_id,
            filename=file.filename,
            content=file_content
        )
        
        return {
            "file_id": file_id,
            "filename": file.filename,
            "size": len(file_content),
            "nodes": storage_result["nodes"],
            "status": "uploaded"
        }
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/files/{file_id}")
async def download_file(file_id: str):
    
    try:
        file_data = await file_service.retrieve_file(file_id)
        
        if not file_data:
            raise HTTPException(status_code=404, detail="File not found")
        
        return StreamingResponse(
            io.BytesIO(file_data["content"]),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={file_data['filename']}"}
        )
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

@app.get("/files")
async def list_files():
    
    files = await file_service.list_files()
    return {"files": files}

@app.delete("/files/{file_id}")
async def delete_file(file_id: str):
    
    try:
        result = await file_service.delete_file(file_id)
        return {"file_id": file_id, "status": "deleted", "nodes_cleaned": result["nodes_cleaned"]}
    except Exception as e:
        logger.error(f"Delete failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")

@app.post("/nodes/register")
async def register_node(node_data: dict):
    
    node_id = node_data.get("node_id")
    node_url = node_data.get("url")
    capacity = node_data.get("capacity", 1024*1024*1024)  # Default 1GB
    
    result = await node_service.register_node(node_id, node_url, capacity)
    return {"status": "registered", "node_id": node_id}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
