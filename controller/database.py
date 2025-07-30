import os
import time
from datetime import datetime

from sqlalchemy import (
    DDL,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    create_engine,
    event,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://admin:admin@postgres_container:5432/dfs_test",
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class FileMetadata(Base):
    __tablename__ = "files"

    file_id = Column(String, primary_key=True)
    filename = Column(String, nullable=False)
    size = Column(BigInteger, nullable=False)  # Changed to BIGINT for large files
    checksum = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_deleted = Column(Boolean, default=False, index=True)


class StorageNode(Base):
    __tablename__ = "storage_nodes"

    node_id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    capacity = Column(BigInteger, nullable=False)  # BIGINT for large storage
    used_space = Column(BigInteger, default=0)
    is_active = Column(Boolean, default=True, index=True)
    last_heartbeat = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


class FileLocation(Base):
    __tablename__ = "file_locations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(String, ForeignKey("files.file_id"), nullable=False, index=True)
    node_id = Column(
        String, ForeignKey("storage_nodes.node_id"), nullable=False, index=True
    )
    created_at = Column(DateTime, default=datetime.utcnow)


class NodeMetrics(Base):
    __tablename__ = "node_metrics"

    id = Column(Integer, primary_key=True, index=True)
    node_id = Column(String, index=True, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)

    # Storage metrics
    total_storage_bytes = Column(BigInteger, default=0)
    used_storage_bytes = Column(BigInteger, default=0)
    available_storage_bytes = Column(BigInteger, default=0)
    files_count = Column(Integer, default=0)

    # Performance metrics
    upload_ops_count = Column(Integer, default=0)
    download_ops_count = Column(Integer, default=0)
    delete_ops_count = Column(Integer, default=0)
    avg_response_time_ms = Column(Float, default=0.0)

    # Health metrics
    is_healthy = Column(Boolean, default=True)
    cpu_usage_percent = Column(Float, default=0.0)
    memory_usage_percent = Column(Float, default=0.0)
    last_heartbeat = Column(DateTime, default=datetime.utcnow)


# trigger function and trigger for automatic updated_at updates
update_timestamp_trigger = DDL(
    """
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';

    DROP TRIGGER IF EXISTS update_files_updated_at ON files;
    CREATE TRIGGER update_files_updated_at
        BEFORE UPDATE ON files
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
"""
)

# Attach the trigger to the FileMetadata table
event.listen(FileMetadata.__table__, "after_create", update_timestamp_trigger)


def init_database():
    """Initialize database tables with retry logic"""
    max_retries = 30
    retry_interval = 2

    for attempt in range(max_retries):
        try:
            # Try to create tables
            Base.metadata.create_all(bind=engine)
            print("Database tables created successfully")
            return
        except OperationalError as e:
            if attempt < max_retries - 1:
                print(
                    f"Database not ready, attempt {attempt + 1}/{max_retries}. Retrying in {retry_interval}s..."
                )
                time.sleep(retry_interval)
            else:
                print(f"Failed to connect to database after {max_retries} attempts")
                raise e


def get_db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class DatabaseSession:
    """Context manager for database sessions"""

    def __init__(self):
        self.db = None

    def __enter__(self):
        self.db = SessionLocal()
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db:
            self.db.close()
