# AutoProvisioner

A Flask-based backend service for provisioning and managing DB clusters with Docker orchestration

## Features

- **Multi-Database Support**: PostgreSQL, MySQL, MongoDB
- **High Availability**: Configurable primary-replica setups
- **Docker Orchestration**: Containerized database clusters
- **REST API**: Complete cluster lifecycle management
- **Environment Configuration**: Flexible configuration management

## Quick Start

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd AutoProvisioner
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Start the application:
```bash
python run.py
```

The application will be available at `http://localhost:5000`

## API Endpoints

### Health Check
- `GET /health` - Service health status

### Cluster Management
- `POST /api/v1/provision` - Provision a new database cluster
- `GET /api/v1/status/<cluster_name>` - Get cluster status
- `GET /api/v1/admin/<cluster_name>` - Get admin connection info
- `DELETE /api/v1/delete/<cluster_name>` - Delete a cluster
- `GET /api/v1/list` - List all clusters

## Usage Examples

### Provision a PostgreSQL Cluster

```bash
curl -X POST http://localhost:5000/api/v1/provision \
  -H "Content-Type: application/json" \
  -d '{
    "type": "postgresql",
    "name": "my-postgres-cluster",
    "config": {
      "database": "myapp",
      "username": "appuser",
      "password": "securepassword",
      "ha_enabled": true
    }
  }'
```

### Check Cluster Status

```bash
curl http://localhost:5000/api/v1/status/my-postgres-cluster
```

### Get Admin Connection Info

```bash
curl http://localhost:5000/api/v1/admin/my-postgres-cluster
```

### List All Clusters

```bash
curl http://localhost:5000/api/v1/list
```

### Delete a Cluster

```bash
curl -X DELETE http://localhost:5000/api/v1/delete/my-postgres-cluster
```

## Configuration

The application supports multiple environments through configuration classes:

- `DevelopmentConfig`: For local development
- `ProductionConfig`: For production deployment
- `TestingConfig`: For running tests

Set the `FLASK_ENV` environment variable to choose the configuration:

```bash
export FLASK_ENV=production  # or development, testing
```
