-- Initialize distributed storage database

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS distributed_storage;

-- Use the database
\c distributed_storage;

-- Create files table
CREATE TABLE IF NOT EXISTS files (
    file_id VARCHAR(255) PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    size INTEGER NOT NULL,
    checksum VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE
);

-- Create storage_nodes table
CREATE TABLE IF NOT EXISTS storage_nodes (
    node_id VARCHAR(255) PRIMARY KEY,
    url VARCHAR(255) NOT NULL,
    capacity BIGINT NOT NULL,
    used_space BIGINT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create file_locations table
CREATE TABLE IF NOT EXISTS file_locations (
    id SERIAL PRIMARY KEY,
    file_id VARCHAR(255) NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (file_id) REFERENCES files(file_id),
    FOREIGN KEY (node_id) REFERENCES storage_nodes(node_id)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_files_created_at ON files(created_at);
CREATE INDEX IF NOT EXISTS idx_files_is_deleted ON files(is_deleted);
CREATE INDEX IF NOT EXISTS idx_storage_nodes_is_active ON storage_nodes(is_active);
CREATE INDEX IF NOT EXISTS idx_file_locations_file_id ON file_locations(file_id);
CREATE INDEX IF NOT EXISTS idx_file_locations_node_id ON file_locations(node_id);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_files_updated_at 
    BEFORE UPDATE ON files 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();
