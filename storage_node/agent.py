from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
import os
import asyncio
import httpx
import hashlib
import logging
from pathlib import Path
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Storage Node Agent", version="1.0.0")

# Configuration
NODE_ID = os.getenv("NODE_ID", "node-1")
CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://localhost:8000")
STORAGE_PATH = Path(os.getenv("STORAGE_PATH", "/data"))
NODE_PORT = int(os.getenv("NODE_PORT", "8001"))
NODE_URL = f"http://storage-node-{NODE_ID}:{NODE_PORT}"

# Ensure storage directory exists
STORAGE_PATH.mkdir(parents=True, exist_ok=True)

class StorageAgent:
    def __init__(self):
        self.storage_path = STORAGE_PATH
        self.controller_url = CONTROLLER_URL
        self.node_id = NODE_ID
        self.node_url = NODE_URL

    async def register_with_controller(self):
        """Register this node with the controller"""
        try:
            async with httpx.AsyncClient() as client:
                registration_data = {
                    "node_id": self.node_id,
                    "url": self.node_url,
                    "capacity": self.get_storage_capacity()
                }
                
                response = await client.post(
                    f"{self.controller_url}/nodes/register",
                    json=registration_data,
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    logger.info(f"Successfully registered node {self.node_id} with controller")
                    return True
                else:
                    logger.error(f"Failed to register with controller: {response.status_code}")
                    return False
        except Exception as e:
            logger.error(f"Error registering with controller: {str(e)}")
            return False

    def get_storage_capacity(self) -> int:
        """Get storage capacity in bytes"""
        try:
            # Get available disk space
            statvfs = os.statvfs(self.storage_path)
            return statvfs.f_frsize * statvfs.f_blocks
        except Exception:
            return 1024 * 1024 * 1024  # Default 1GB

    def get_used_space(self) -> int:
        """Get used storage space in bytes"""
        try:
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(self.storage_path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    total_size += os.path.getsize(filepath)
            return total_size
        except Exception:
            return 0

    async def store_file(self, file_id: str, content: bytes) -> bool:
        """Store file content locally"""
        try:
            file_path = self.storage_path / file_id
            
            # Write file content
            with open(file_path, 'wb') as f:
                f.write(content)
            
            # Store metadata
            metadata = {
                "file_id": file_id,
                "size": len(content),
                "checksum": hashlib.sha256(content).hexdigest(),
                "stored_at": file_path.as_posix()
            }
            
            metadata_path = self.storage_path / f"{file_id}.meta"
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f)
            
            logger.info(f"Stored file {file_id} ({len(content)} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"Error storing file {file_id}: {str(e)}")
            return False

    async def retrieve_file(self, file_id: str) -> bytes:
        """Retrieve file content"""
        try:
            file_path = self.storage_path / file_id
            
            if not file_path.exists():
                raise HTTPException(status_code=404, detail="File not found")
            
            with open(file_path, 'rb') as f:
                content = f.read()
            
            logger.info(f"Retrieved file {file_id} ({len(content)} bytes)")
            return content
            
        except Exception as e:
            logger.error(f"Error retrieving file {file_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error retrieving file: {str(e)}")

    async def delete_file(self, file_id: str) -> bool:
        """Delete file from storage"""
        try:
            file_path = self.storage_path / file_id
            metadata_path = self.storage_path / f"{file_id}.meta"
            
            # Delete file and metadata
            if file_path.exists():
                file_path.unlink()
            
            if metadata_path.exists():
                metadata_path.unlink()
            
            logger.info(f"Deleted file {file_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting file {file_id}: {str(e)}")
            return False

    async def list_files(self) -> list:
        """List all stored files"""
        try:
            files = []
            for file_path in self.storage_path.glob("*.meta"):
                try:
                    with open(file_path, 'r') as f:
                        metadata = json.load(f)
                    files.append(metadata)
                except Exception as e:
                    logger.error(f"Error reading metadata for {file_path}: {str(e)}")
            
            return files
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return []

# Initialize storage agent
storage_agent = StorageAgent()

@app.on_event("startup")
async def startup_event():
    """Register with controller on startup"""
    logger.info(f"Starting Storage Node {NODE_ID}")
    await asyncio.sleep(5)  # Wait for controller to be ready
    await storage_agent.register_with_controller()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "node_id": NODE_ID,
        "storage_path": str(STORAGE_PATH),
        "used_space": storage_agent.get_used_space(),
        "capacity": storage_agent.get_storage_capacity()
    }

@app.post("/store/{file_id}")
async def store_file(file_id: str, request: Request):
    """Store a file"""
    try:
        content = await request.body()
        success = await storage_agent.store_file(file_id, content)
        
        if success:
            return {"status": "stored", "file_id": file_id, "size": len(content)}
        else:
            raise HTTPException(status_code=500, detail="Failed to store file")
    except Exception as e:
        logger.error(f"Store endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/retrieve/{file_id}")
async def retrieve_file(file_id: str):
    """Retrieve a file"""
    try:
        content = await storage_agent.retrieve_file(file_id)
        return Response(content=content, media_type="application/octet-stream")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Retrieve endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete/{file_id}")
async def delete_file(file_id: str):
    """Delete a file"""
    try:
        success = await storage_agent.delete_file(file_id)
        
        if success:
            return {"status": "deleted", "file_id": file_id}
        else:
            raise HTTPException(status_code=500, detail="Failed to delete file")
    except Exception as e:
        logger.error(f"Delete endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files")
async def list_files():
    """List all stored files"""
    try:
        files = await storage_agent.list_files()
        return {"files": files, "count": len(files)}
    except Exception as e:
        logger.error(f"List files error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Get node statistics"""
    return {
        "node_id": NODE_ID,
        "capacity": storage_agent.get_storage_capacity(),
        "used_space": storage_agent.get_used_space(),
        "available_space": storage_agent.get_storage_capacity() - storage_agent.get_used_space(),
        "files_count": len(await storage_agent.list_files())
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
