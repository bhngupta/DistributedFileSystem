# Distributed File Storage System

## Components

- **Controller**: FastAPI application managing file metadata and node orchestration
- **Storage Nodes**: Python agents handling file storage and retrieval
- **PostgreSQL**: Metadata database storing file locations and node information
- **Docker**: Containerization for storage nodes

## Quick Start

1. Start the database: `docker-compose up -d postgres`
2. Start the controller: `python -m controller.main`
3. Start storage nodes: `docker-compose up -d storage-nodes`
4. Access API at: `http://localhost:8000`
