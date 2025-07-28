from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class FileMetadataModel(BaseModel):
    file_id: str
    filename: str
    size: int
    checksum: str
    created_at: datetime
    updated_at: datetime
    is_deleted: bool


class StorageNodeModel(BaseModel):
    node_id: str
    url: str
    capacity: int
    used_space: int
    is_active: bool
    last_heartbeat: datetime


class FileLocationModel(BaseModel):
    id: int
    file_id: str
    node_id: str
    created_at: datetime


class UploadRequest(BaseModel):
    filename: str
    content: bytes


class UploadResponse(BaseModel):
    file_id: str
    filename: str
    size: int
    nodes: List[str]
    status: str


class NodeRegistration(BaseModel):
    node_id: str
    url: str
    capacity: int
