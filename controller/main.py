import asyncio
import io
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from .database import init_database
from .services import FileService, NodeService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

file_svc = FileService()
node_svc = NodeService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting controller...")
    init_database()
    await node_svc.discover_nodes()
    yield
    logger.info("Shutting down controller...")


app = FastAPI(title="Storage Controller", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/nodes")
async def get_storage_nodes():
    active_nodes = await node_svc.get_active_nodes()
    return {"nodes": active_nodes}


@app.post("/files/upload")
async def upload_file(uploaded_file: UploadFile = File(...)):
    try:
        file_id = str(uuid.uuid4())
        file_data = await uploaded_file.read()
        storage_results = await file_svc.store_file(
            file_id, uploaded_file.filename, file_data
        )
        return {
            "file_id": file_id,
            "filename": uploaded_file.filename,
            "size": len(file_data),
            "nodes": storage_results["nodes"],
            "status": "uploaded",
        }
    except Exception as ex:
        logger.error(f"Upload failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(ex)}")


@app.get("/files/{file_id}")
async def download_file(file_id: str):
    try:
        file_info = await file_svc.retrieve_file(file_id)
        if not file_info:
            raise HTTPException(status_code=404, detail="File not found")
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
async def list_files():
    file_list = await file_svc.list_files()
    return {"files": file_list}


@app.delete("/files/{file_id}")
async def delete_file(file_id: str):
    try:
        delete_result = await file_svc.delete_file(file_id)
        return {
            "file_id": file_id,
            "status": "deleted",
            "nodes_cleaned": delete_result["nodes_cleaned"],
        }
    except Exception as ex:
        logger.error(f"Delete failed: {str(ex)}")
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(ex)}")


@app.post("/nodes/heartbeat")
async def node_heartbeat(heartbeat_data: dict):
    node_id = heartbeat_data.get("node_id")
    if not node_id:
        raise HTTPException(status_code=400, detail="node_id required")
    success = await node_svc.update_node_heartbeat(node_id)
    if success:
        return {"status": "ok", "node_id": node_id}
    else:
        raise HTTPException(status_code=404, detail="Node not found")


@app.get("/nodes/health")
async def get_nodes_health():
    return await node_svc.check_node_health()


@app.post("/nodes/register")
async def register_node(node_info: dict):
    node_id = node_info.get("node_id")
    node_url = node_info.get("url")
    node_capacity = node_info.get("capacity", 1024 * 1024 * 1024)
    await node_svc.register_node(node_id, node_url, node_capacity)
    return {"status": "registered", "node_id": node_id}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
