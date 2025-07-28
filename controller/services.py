import asyncio
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Optional

import httpx
from sqlalchemy.orm import Session

from .database import FileLocation, FileMetadata, StorageNode, get_db_session

logger = logging.getLogger(__name__)


class FileService:
    def __init__(self):
        # How many copies of each file should we keep?
        self.num_replicas = 2

    async def store_file(self, file_id: str, filename: str, content: bytes) -> Dict:
        """Take a file and spread it across our storage nodes"""
        file_hash = hashlib.sha256(content).hexdigest()
        content_size = len(content)

        # Find nodes that are currently active
        node_svc = NodeService()
        nodes_available = await node_svc.get_active_nodes()

        if len(nodes_available) < self.num_replicas:
            raise Exception(
                f"Need {self.num_replicas} nodes but only have {len(nodes_available)}"
            )

        # Pick some nodes to store the file on
        target_nodes = nodes_available[: self.num_replicas]

        # Try to store the file on each selected node
        successful_stores = []
        for node_info in target_nodes:
            try:
                store_worked = await self._put_file_on_node(
                    node_info["url"], file_id, content
                )
                if store_worked:
                    successful_stores.append(node_info["node_id"])
            except Exception as ex:
                logger.error(
                    f"Couldn't store on node {node_info['node_id']}: {str(ex)}"
                )

        if not successful_stores:
            raise Exception("Couldn't store file anywhere!")

        # Record what we did in the database
        db_session = next(get_db_session())
        try:
            # Save the file metadata
            file_record = FileMetadata(
                file_id=file_id,
                filename=filename,
                size=content_size,
                checksum=file_hash,
            )
            db_session.add(file_record)

            # Record where we stored it
            for node_id in successful_stores:
                location_record = FileLocation(file_id=file_id, node_id=node_id)
                db_session.add(location_record)

            db_session.commit()
        finally:
            db_session.close()

        return {"nodes": successful_stores, "checksum": file_hash}

    async def retrieve_file(self, file_id: str) -> Optional[Dict]:
        """Get a file back from storage"""
        db_session = next(get_db_session())
        try:
            # Look up the file info
            file_info = (
                db_session.query(FileMetadata)
                .filter(
                    FileMetadata.file_id == file_id, FileMetadata.is_deleted == False
                )
                .first()
            )

            if not file_info:
                return None

            # Find out where it's stored
            storage_locations = (
                db_session.query(FileLocation)
                .filter(FileLocation.file_id == file_id)
                .all()
            )

            if not storage_locations:
                return None

            # Try each location until we get the file
            for location in storage_locations:
                try:
                    storage_node = (
                        db_session.query(StorageNode)
                        .filter(
                            StorageNode.node_id == location.node_id,
                            StorageNode.is_active == True,
                        )
                        .first()
                    )

                    if node:
                        content = await self._retrieve_file_from_node(node.url, file_id)
                        if content:
                            return {
                                "filename": file_metadata.filename,
                                "content": content,
                                "size": file_metadata.size,
                                "checksum": file_metadata.checksum,
                            }
                except Exception as e:
                    logger.error(
                        f"Failed to retrieve file from node {location.node_id}: {str(e)}"
                    )
                    continue

            return None
        finally:
            db.close()

    async def delete_file(self, file_id: str) -> Dict:
        """Delete file from all storage nodes"""
        db = next(get_db_session())
        try:
            # Mark file as deleted in metadata
            file_metadata = (
                db.query(FileMetadata).filter(FileMetadata.file_id == file_id).first()
            )

            if not file_metadata:
                raise Exception("File not found")

            file_metadata.is_deleted = True

            # Get file locations
            file_locations = (
                db.query(FileLocation).filter(FileLocation.file_id == file_id).all()
            )

            # Delete from nodes
            deleted_nodes = []
            for location in file_locations:
                try:
                    node = (
                        db.query(StorageNode)
                        .filter(
                            StorageNode.node_id == location.node_id,
                            StorageNode.is_active == True,
                        )
                        .first()
                    )

                    if node:
                        success = await self._delete_file_from_node(node.url, file_id)
                        if success:
                            deleted_nodes.append(location.node_id)
                except Exception as e:
                    logger.error(
                        f"Failed to delete file from node {location.node_id}: {str(e)}"
                    )

            db.commit()
            return {"nodes_cleaned": deleted_nodes}
        finally:
            db.close()

    async def list_files(self) -> List[Dict]:
        """List all files in the storage system"""
        db = next(get_db_session())
        try:
            files = (
                db.query(FileMetadata).filter(FileMetadata.is_deleted == False).all()
            )

            return [
                {
                    "file_id": f.file_id,
                    "filename": f.filename,
                    "size": f.size,
                    "created_at": f.created_at.isoformat(),
                    "checksum": f.checksum,
                }
                for f in files
            ]
        finally:
            db.close()

    async def _store_file_on_node(
        self, node_url: str, file_id: str, content: bytes
    ) -> bool:
        """Store file on a specific storage node"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{node_url}/store/{file_id}",
                    content=content,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=30.0,
                )
                return response.status_code == 200
            except Exception as e:
                logger.error(f"Error storing file on node {node_url}: {str(e)}")
                return False

    async def _retrieve_file_from_node(
        self, node_url: str, file_id: str
    ) -> Optional[bytes]:
        """Retrieve file from a specific storage node"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{node_url}/retrieve/{file_id}", timeout=30.0
                )
                if response.status_code == 200:
                    return response.content
                return None
            except Exception as e:
                logger.error(f"Error retrieving file from node {node_url}: {str(e)}")
                return None

    async def _delete_file_from_node(self, node_url: str, file_id: str) -> bool:
        """Delete file from a specific storage node"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.delete(
                    f"{node_url}/delete/{file_id}", timeout=30.0
                )
                return response.status_code == 200
            except Exception as e:
                logger.error(f"Error deleting file from node {node_url}: {str(e)}")
                return False


class NodeService:
    async def get_active_nodes(self) -> List[Dict]:
        """Get list of active storage nodes"""
        db = next(get_db_session())
        try:
            nodes = db.query(StorageNode).filter(StorageNode.is_active == True).all()

            return [
                {
                    "node_id": node.node_id,
                    "url": node.url,
                    "capacity": node.capacity,
                    "used_space": node.used_space,
                    "last_heartbeat": node.last_heartbeat.isoformat(),
                }
                for node in nodes
            ]
        finally:
            db.close()

    async def register_node(self, node_id: str, url: str, capacity: int) -> bool:
        """Register a new storage node"""
        db = next(get_db_session())
        try:
            # Check if node already exists
            existing_node = (
                db.query(StorageNode).filter(StorageNode.node_id == node_id).first()
            )

            if existing_node:
                # Update existing node
                existing_node.url = url
                existing_node.capacity = capacity
                existing_node.is_active = True
                existing_node.last_heartbeat = datetime.utcnow()
            else:
                # Create new node
                new_node = StorageNode(
                    node_id=node_id, url=url, capacity=capacity, is_active=True
                )
                db.add(new_node)

            db.commit()
            return True
        except Exception as e:
            logger.error(f"Error registering node {node_id}: {str(e)}")
            db.rollback()
            return False
        finally:
            db.close()

    async def discover_nodes(self):
        """Discover and register storage nodes"""
        # In a real implementation, this would discover nodes automatically
        # For now, nodes register themselves
        logger.info("Node discovery started - waiting for nodes to register")
