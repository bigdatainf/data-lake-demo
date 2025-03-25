# Data Lake Implementation with Docker

This repository contains a simplified, practical implementation of a Data Lake using Docker containers. It's designed as an educational tool for understanding data lake zones and basic operations.

## Overview

This Data Lake implementation includes:

- **MinIO**: S3-compatible object storage as the storage layer
- **Trino**: SQL query engine for data analysis
- **Python Client**: Processing tools for data transformation

The data is organized in zones following standard Data Lake architecture:
- **Raw Zone**: Original unprocessed data
- **Trusted Zone**: Cleansed and validated data
- **Refined Zone**: Transformed and enriched data
- **Consumption Zone**: Business-ready datasets

## Prerequisites

- Docker and Docker Compose installed
- At least 2GB of RAM available for Docker
- Basic knowledge of SQL and Python

## Deployment Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/data-lake-demo.git
cd data-lake-demo
```

### 2. Create Directory Structure

Create the necessary directories for the deployment:

```bash
mkdir -p config/trino/{catalog,} data/{raw-zone,trusted-zone,refined-zone,consumption-zone} scripts
```

### 3. Create Configuration Files

#### Trino Configuration

Create `config/trino/config.properties`:

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://trino:8080
```

Create `config/trino/node.properties`:

```properties
node.environment=production
node.data-dir=/data/trino
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
```

Create `config/trino/jvm.config`:

```
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
```

Create `config/trino/catalog/minio.properties`:

```properties
connector.name=hive
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
hive.non-managed-table-writes-enabled=true
hive.metastore=file
hive.metastore.catalog.dir=s3://raw-zone/
```

### 4. Create Python Scripts

Copy the provided Python scripts to the scripts directory:
- `utils.py`: Common utilities for interacting with MinIO and Trino
- `01_ingest_data.py`: Data ingestion into raw zone
- `02_transform_data.py`: Data transformation into trusted zone
- `03_query_data.py`: Data analysis using pandas

### 5. Create Docker Compose File

Create `docker-compose.yml` with the content provided.

### 6. Start the Environment

Launch the Docker containers:

```bash
docker-compose up -d
```

Verify that all services are running:

```bash
docker-compose ps
```

### 7. Run the Sample Workflow

Execute the data ingestion script:

```bash
docker exec -it python-client python /scripts/01_ingest_data.py
```

Transform the data:

```bash
docker exec -it python-client python /scripts/02_transform_data.py
```

Analyze the data:

```bash
docker exec -it python-client python /scripts/03_query_data.py
```

## Exploring the Data Lake

### Access MinIO Console

Open your browser and navigate to:
- URL: http://localhost:9001
- Login with: minioadmin / minioadmin

You can explore the different buckets and view the data files.

### Using Trino for SQL Queries

Connect to the Trino CLI:

```bash
docker exec -it trino trino
```

Run some example queries:

```sql
-- Show available catalogs
SHOW CATALOGS;

-- Show available schemas
SHOW SCHEMAS FROM minio;

-- Query the data (if you have created a table)
SELECT * FROM minio.default.your_table LIMIT 10;
```

## Adding Your Own Data

You can add custom data to the Data Lake using these steps:

1. Prepare your data in CSV format
2. Use the Python client to ingest it into the raw zone:

```bash
docker cp your-data.csv python-client:/data/
docker exec -it python-client python -c "
from utils import upload_file_to_minio
upload_file_to_minio('/data/your-data.csv', 'raw-zone', 'custom/your-data.csv')
"
```

3. Write a transformation script similar to the example
4. Analyze the data using pandas or Trino

## Data Lake Architecture Explanation

This simplified implementation demonstrates the key concepts of a Data Lake:

### 1. Zones-based Organization

- **Raw Zone**: Stores original, unmodified data exactly as it was received
- **Trusted Zone**: Contains validated and cleansed data with consistent formats
- **Refined Zone**: Holds transformed and enriched data ready for analysis
- **Consumption Zone**: Provides business-ready datasets tailored for specific use cases

### 2. Schema-on-Read Philosophy

Unlike traditional databases, data lakes follow a "schema-on-read" approach:
- Data is stored without predefined schema requirements
- Schema is applied only when the data is read/queried
- This allows for more flexibility in data storage and usage

### 3. Separation of Storage and Compute

- MinIO provides the storage layer
- Trino and Python handle the compute layer
- This separation allows for independent scaling of each layer

## Cleanup

When you're done with the environment:

```bash
# Stop the containers
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v
```

## Next Steps and Extensions

After mastering the basic implementation, consider:

1. Adding data quality checks
2. Creating dimension tables and fact tables
3. Implementing simple workflow orchestration
4. Building a visualization layer with tools like Metabase
5. Adding Hive Metastore for metadata management (optional)

## References

- [MinIO Documentation](https://docs.min.io/)
- [Trino Documentation](https://trino.io/docs/current/)
- [Data Lake Design Patterns](https://docs.aws.amazon.com/prescriptive-guidance/latest/defining-data-lake-architecture/welcome.html)