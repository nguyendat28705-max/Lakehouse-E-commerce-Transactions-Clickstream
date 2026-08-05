-- MySQL initialization script for Hive Metastore

-- Create hive_metastore database if not exists
CREATE DATABASE IF NOT EXISTS hive_metastore;

-- Grant permissions to hive user
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hive'@'%';

-- Create additional user for root access
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;

-- Flush privileges
FLUSH PRIVILEGES;