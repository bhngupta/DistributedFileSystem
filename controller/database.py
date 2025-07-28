import os
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, create_engine
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
    size = Column(Integer, nullable=False)
    checksum = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_deleted = Column(Boolean, default=False)


class StorageNode(Base):
    __tablename__ = "storage_nodes"

    node_id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    capacity = Column(Integer, nullable=False)
    used_space = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    last_heartbeat = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


class FileLocation(Base):
    __tablename__ = "file_locations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(String, nullable=False)
    node_id = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


# Create tables
Base.metadata.create_all(bind=engine)


def get_db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
