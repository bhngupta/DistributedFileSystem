import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import and_, desc, func
from sqlalchemy.orm import Session

from .models import (
    AccountingLog,
    IOOperation,
    NodeStorageUsage,
    OperationType,
    StorageNode,
    User,
    UserStorageUsage,
    get_accounting_db,
)


class UserAccountingService:
    def __init__(self, db: Session):
        self.db = db

    async def create_user(self, username: str, email: str) -> User:
        user = User(username=username, email=email)
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        self.db.add(UserStorageUsage(user_id=user.id))
        self.db.commit()
        await self._log_event(
            user_id=user.id,
            event_type="user_created",
            description=f"User {username} created",
        )
        return user

    async def update_usage_after_operation(
        self, user_id: int, operation_type: OperationType, file_size: int
    ):
        usage = (
            self.db.query(UserStorageUsage)
            .filter(UserStorageUsage.user_id == user_id)
            .first()
        )
        if not usage:
            usage = UserStorageUsage(user_id=user_id)
            self.db.add(usage)
        if operation_type == OperationType.UPLOAD:
            usage.total_files += 1
            usage.total_bytes_used += file_size
        elif operation_type == OperationType.DELETE:
            usage.total_files = max(0, usage.total_files - 1)
            usage.total_bytes_used = max(0, usage.total_bytes_used - file_size)
        usage.last_calculated = datetime.utcnow()
        self.db.commit()

    async def _log_event(
        self,
        user_id: int = None,
        node_id: int = None,
        event_type: str = "",
        description: str = "",
        severity: str = "info",
        event_data: Dict = None,
    ):
        self.db.add(
            AccountingLog(
                user_id=user_id,
                node_id=node_id,
                event_type=event_type,
                event_description=description,
                severity=severity,
                event_data=json.dumps(event_data) if event_data else None,
            )
        )
        self.db.commit()


class NodeAccountingService:
    def __init__(self, db: Session):
        self.db = db

    async def register_node(
        self, node_id: str, hostname: str, total_capacity: int, **kwargs
    ) -> StorageNode:
        """Register a new storage node"""

        node = StorageNode(
            node_id=node_id,
            hostname=hostname,
            total_capacity=total_capacity,
            available_capacity=total_capacity,
            **kwargs,
        )

        self.db.add(node)
        self.db.commit()
        self.db.refresh(node)

        # Create initial usage record
        usage = NodeStorageUsage(node_id=node.id)
        self.db.add(usage)
        self.db.commit()

        return node

    async def update_node_health(
        self, node_id: str, response_time_ms: float, is_healthy: bool = True
    ):
        """Update node health metrics"""
        node = self.db.query(StorageNode).filter(StorageNode.node_id == node_id).first()
        if not node:
            return

        # Update response time (rolling average)
        if node.avg_response_time_ms == 0:
            node.avg_response_time_ms = response_time_ms
        else:
            # Simple exponential moving average
            node.avg_response_time_ms = (node.avg_response_time_ms * 0.8) + (
                response_time_ms * 0.2
            )

        node.last_health_check = datetime.utcnow()
        node.is_active = is_healthy

        self.db.commit()

    async def update_node_usage(
        self, node_id: str, bytes_change: int, files_change: int = 0
    ):
        """Update node usage statistics"""
        node = self.db.query(StorageNode).filter(StorageNode.node_id == node_id).first()
        usage = (
            self.db.query(NodeStorageUsage)
            .filter(NodeStorageUsage.node_id == node.id)
            .first()
        )

        if not node or not usage:
            return

        # Update node capacity
        node.allocated_capacity += bytes_change
        node.available_capacity = max(0, node.total_capacity - node.allocated_capacity)

        # Update usage stats
        usage.total_files_stored += files_change
        usage.total_bytes_used += bytes_change
        usage.last_calculated = datetime.utcnow()

        self.db.commit()

    def get_node_recommendations(self, required_capacity: int) -> List[StorageNode]:
        return (
            self.db.query(StorageNode)
            .filter(
                and_(
                    StorageNode.is_active == True,
                    StorageNode.is_accepting_writes == True,
                    StorageNode.available_capacity >= required_capacity,
                )
            )
            .order_by(
                StorageNode.available_capacity.desc(),
                StorageNode.avg_response_time_ms.asc(),
            )
            .limit(10)
            .all()
        )


class IOOperationService:
    """Service for logging and analyzing I/O operations"""

    def __init__(self, db: Session):
        self.db = db

    async def log_operation(
        self,
        user_id: int,
        node_id: int,
        operation_type: OperationType,
        file_id: str,
        file_size: int,
        filename: str = None,
        response_time_ms: int = None,
        was_successful: bool = True,
        error_message: str = None,
        client_ip: str = None,
    ) -> IOOperation:
        """Log an I/O operation for analytics"""

        operation = IOOperation(
            user_id=user_id,
            node_id=node_id,
            operation_type=operation_type,
            file_id=file_id,
            filename=filename,
            file_size=file_size,
            response_time_ms=response_time_ms,
            was_successful=was_successful,
            error_message=error_message,
            client_ip=client_ip,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow() if was_successful else None,
        )

        self.db.add(operation)
        self.db.commit()
        self.db.refresh(operation)

        return operation

    def get_user_operation_stats(self, user_id: int, days: int = 30) -> Dict:
        since_date = datetime.utcnow() - timedelta(days=days)

        stats = (
            self.db.query(
                IOOperation.operation_type,
                func.count(IOOperation.id).label("count"),
                func.sum(IOOperation.file_size).label("total_bytes"),
                func.avg(IOOperation.response_time_ms).label("avg_response_time"),
            )
            .filter(
                and_(
                    IOOperation.user_id == user_id,
                    IOOperation.started_at >= since_date,
                    IOOperation.was_successful == True,
                )
            )
            .group_by(IOOperation.operation_type)
            .all()
        )

        return {
            stat.operation_type: {
                "count": stat.count,
                "total_bytes": stat.total_bytes or 0,
                "avg_response_time_ms": float(stat.avg_response_time or 0),
            }
            for stat in stats
        }


# Factory functions for service instances
def get_user_service(db: Session = None) -> UserAccountingService:
    """Get user accounting service instance"""
    if db is None:
        db = next(get_accounting_db())
    return UserAccountingService(db)


def get_node_service(db: Session = None) -> NodeAccountingService:
    """Get node accounting service instance"""
    if db is None:
        db = next(get_accounting_db())
    return NodeAccountingService(db)


def get_io_service(db: Session = None) -> IOOperationService:
    """Get I/O operation service instance"""
    if db is None:
        db = next(get_accounting_db())
    return IOOperationService(db)
