import os
from datetime import datetime
from enum import Enum

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Database configuration
ACCOUNTING_DATABASE_URL = os.getenv(
    "ACCOUNTING_DATABASE_URL",
    "postgresql://storage_user:storage_pass@localhost:5432/storage_accounting",
)

accounting_engine = create_engine(ACCOUNTING_DATABASE_URL)
AccountingSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=accounting_engine
)
AccountingBase = declarative_base()


class OperationType(str, Enum):
    UPLOAD = "upload"
    DOWNLOAD = "download"
    DELETE = "delete"
    REPLICATE = "replicate"


class User(AccountingBase):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    storage_usage = relationship("UserStorageUsage", back_populates="user")
    io_operations = relationship("IOOperation", back_populates="user")


class StorageNode(AccountingBase):
    __tablename__ = "storage_nodes"

    id = Column(Integer, primary_key=True)
    node_id = Column(String(100), unique=True, nullable=False)
    hostname = Column(String(255), nullable=False)
    total_capacity = Column(BigInteger, nullable=False)
    allocated_capacity = Column(BigInteger, default=0)
    available_capacity = Column(BigInteger, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    storage_usage = relationship("NodeStorageUsage", back_populates="node")
    io_operations = relationship("IOOperation", back_populates="node")


class UserStorageUsage(AccountingBase):
    __tablename__ = "user_storage_usage"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    total_files = Column(Integer, default=0)
    total_bytes_used = Column(BigInteger, default=0)
    last_calculated = Column(DateTime, default=datetime.utcnow)
    user = relationship("User", back_populates="storage_usage")


class NodeStorageUsage(AccountingBase):
    __tablename__ = "node_storage_usage"

    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey("storage_nodes.id"), nullable=False)
    total_files_stored = Column(Integer, default=0)
    total_bytes_used = Column(BigInteger, default=0)
    last_calculated = Column(DateTime, default=datetime.utcnow)
    node = relationship("StorageNode", back_populates="storage_usage")


class IOOperation(AccountingBase):
    __tablename__ = "io_operations"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    node_id = Column(Integer, ForeignKey("storage_nodes.id"), nullable=False)
    operation_type = Column(String(20), nullable=False)
    file_id = Column(String(255), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    user = relationship("User", back_populates="io_operations")
    node = relationship("StorageNode", back_populates="io_operations")


class AccountingLog(AccountingBase):
    __tablename__ = "accounting_logs"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    node_id = Column(Integer, ForeignKey("storage_nodes.id"))
    event_type = Column(String(50), nullable=False)
    event_description = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


def create_accounting_tables():
    AccountingBase.metadata.create_all(bind=accounting_engine)


def get_accounting_db():
    db = AccountingSessionLocal()
    try:
        yield db
    finally:
        db.close()


# Useful queries and helper functions
def get_user_storage_summary(db, user_id: int):
    user = db.query(User).filter(User.id == user_id).first()
    usage = (
        db.query(UserStorageUsage).filter(UserStorageUsage.user_id == user_id).first()
    )

    if not user or not usage:
        return None

    return {
        "user_id": user_id,
        "username": user.username,
        "storage_used": usage.total_bytes_used,
        "files_count": usage.total_files,
        "last_calculated": usage.last_calculated,
    }


def get_node_health_summary(db, node_id: str):
    node = db.query(StorageNode).filter(StorageNode.node_id == node_id).first()
    usage = (
        db.query(NodeStorageUsage).filter(NodeStorageUsage.node_id == node.id).first()
    )

    if not node:
        return None

    return {
        "node_id": node_id,
        "hostname": node.hostname,
        "is_healthy": node.is_active and node.is_accepting_writes,
        "capacity_used": usage.total_bytes_used if usage else 0,
        "capacity_total": node.total_capacity,
        "capacity_percentage": (
            (usage.total_bytes_used if usage else 0) / node.total_capacity
        )
        * 100,
        "uptime_percentage": float(node.uptime_percentage),
        "avg_response_time": float(node.avg_response_time_ms),
        "files_stored": usage.total_files_stored if usage else 0,
        "operations_24h": usage.total_operations_24h if usage else 0,
        "last_health_check": node.last_health_check,
    }


if __name__ == "__main__":
    print("Creating accounting database tables...")
    create_accounting_tables()
    print("Accounting tables created successfully!")
