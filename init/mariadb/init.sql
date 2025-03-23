-- Create metastore database if it doesn't exist
CREATE DATABASE IF NOT EXISTS metastore_db;

-- Grant privileges to the hive user
GRANT ALL PRIVILEGES ON metastore_db.* TO 'hive'@'%';
FLUSH PRIVILEGES;

-- Switch to the metastore database
USE metastore_db;

-- This script initializes the database required by Hive Metastore
-- The actual schema creation will be handled by Hive's schematool when 
-- the hive-metastore container starts up
