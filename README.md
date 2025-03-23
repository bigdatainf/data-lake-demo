# Data Lake Implementation with Docker

This repository contains a practical implementation of a Data Lake using Docker containers. It is designed as an educational tool for understanding the concepts, architecture, and operations of a zone-based Data Lake.

## Overview

This Data Lake implementation includes:

- **MinIO**: S3-compatible object storage as the storage layer
- **Hive Metastore**: Metadata management for data organization
- **Trino (formerly PrestoSQL)**: SQL query engine for data analysis
- **Python Client**: Processing tools for data transformation

The data is organized in zones following the standard Data Lake architecture:
- **Raw Zone**: Original unprocessed data
- **Trusted Zone**: Cleansed and validated data
- **Refined Zone**: Transformed and enriched data
- **Consumption Zone**: Business-ready datasets

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB of RAM available for Docker
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
mkdir -p config/hive config/trino/catalog data/{raw-zone,trusted-zone,refined-zone,consumption-zone} init/mariadb scripts
```

### 3. Create Configuration Files

#### Hive Configuration

Create `config/hive/hive-site.xml`:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<configuration>
  <property>
    <n>hive.metastore.warehouse.dir</n>
    <value>s3a://trusted-zone/warehouse</value>
  </property>
  <property>
    <n>javax.jdo.option.ConnectionURL</n>
    <value>jdbc:mysql://mariadb:3306/metastore_db</value>
  </property>
  <property>
    <n>javax.jdo.option.ConnectionDriverName</n>
    <value>com.mysql.jdbc.Driver</value>
  </property>
  <property>
    <n>javax.jdo.option.ConnectionUserName</n>
    <value>hive</value>
  </property>
  <property>
    <n>javax.jdo.option.ConnectionPassword</n>
    <value>hive</value>
  </property>
  <property>
    <n>fs.s3a.endpoint</n>
    <value>http://minio:9000</value>
  </property>
  <property>
    <n>fs.s3a.access.key</n>
    <value>minioadmin</value>
  </property>
  <property>
    <n>fs.s3a.secret.key</n>
    <value>minioadmin</value>
  </property>
  <property>
    <n>fs.s3a.path.style.access</n>
    <value>true</value>
  </property>
  <property>
    <n>fs.s3a.impl</n>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
</configuration>
```

#### Trino Configuration

Create `config/trino/config.properties`:

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://trino:8080
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

Create `config/trino/catalog/hive.properties`:

```properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.path-style-access=true
hive.non-managed-table-writes-enabled=true
hive.storage-format=PARQUET
hive.allow-drop-table=true
```

Create `config/trino/catalog/minio.properties`:

```properties
connector.name=hive
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
hive.metastore=file
hive.metastore.catalog.dir=s3://raw-zone/
```

#### MariaDB Initialization

Create `init/mariadb/init.sql`:

```sql
CREATE DATABASE IF NOT EXISTS metastore_db;
GRANT ALL PRIVILEGES ON metastore_db.* TO 'hive'@'%';
```

### 4. Create Python Scripts

#### Utils Script

Create `scripts/utils.py`:

```python
from minio import Minio
import pandas as pd
import io
import trino
import os

def get_minio_client():
    """Create and return a MinIO client."""
    return Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

def get_trino_connection():
    """Create and return a Trino connection."""
    return trino.dbapi.connect(
        host="trino",
        port=8080,
        user="trino",
        catalog="hive",
        schema="default",
    )

def upload_file_to_minio(file_path, bucket_name, object_name=None):
    """Upload a file to MinIO."""
    if object_name is None:
        object_name = os.path.basename(file_path)
    
    client = get_minio_client()
    
    # Make sure the bucket exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    # Upload the file
    client.fput_object(bucket_name, object_name, file_path)
    print(f"File {file_path} uploaded to {bucket_name}/{object_name}")

def download_file_from_minio(bucket_name, object_name, file_path=None):
    """Download a file from MinIO."""
    if file_path is None:
        file_path = object_name
    
    client = get_minio_client()
    client.fget_object(bucket_name, object_name, file_path)
    print(f"File {bucket_name}/{object_name} downloaded to {file_path}")

def upload_dataframe_to_minio(df, bucket_name, object_name, format='csv'):
    """Upload a pandas DataFrame to MinIO."""
    client = get_minio_client()
    
    # Make sure the bucket exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    # Convert DataFrame to bytes in the specified format
    if format.lower() == 'csv':
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        client.put_object(
            bucket_name, object_name, buffer, length=buffer.getbuffer().nbytes, content_type='text/csv'
        )
    elif format.lower() == 'parquet':
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        client.put_object(
            bucket_name, object_name, buffer, length=buffer.getbuffer().nbytes, content_type='application/octet-stream'
        )
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    print(f"DataFrame uploaded to {bucket_name}/{object_name}")

def download_dataframe_from_minio(bucket_name, object_name, format='csv'):
    """Download a file from MinIO into a pandas DataFrame."""
    client = get_minio_client()
    
    # Get the object
    response = client.get_object(bucket_name, object_name)
    
    # Convert to DataFrame based on format
    if format.lower() == 'csv':
        return pd.read_csv(response)
    elif format.lower() == 'parquet':
        return pd.read_parquet(io.BytesIO(response.read()))
    else:
        raise ValueError(f"Unsupported format: {format}")

def execute_trino_query(query):
    """Execute a query in Trino and return the results as a DataFrame."""
    conn = get_trino_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    
    # Get column names
    if cursor.description:
        columns = [desc[0] for desc in cursor.description]
        # Fetch all results
        data = cursor.fetchall()
        # Create DataFrame
        return pd.DataFrame(data, columns=columns)
    else:
        return pd.DataFrame()
```

#### Data Ingestion Script

Create `scripts/01_ingest_data.py`:

```python
"""
This script demonstrates data ingestion into the raw zone of the data lake.
It uploads sample data to MinIO's raw-zone bucket.
"""
import pandas as pd
import os
from utils import upload_dataframe_to_minio, get_minio_client

def create_sample_data():
    """Create a sample dataset for demonstration purposes."""
    # Create a sample customer transactions dataset
    data = {
        'transaction_id': range(1, 1001),
        'customer_id': [f'CUST{i:04d}' for i in range(1, 101)] * 10,
        'transaction_date': pd.date_range(start='2024-01-01', periods=1000).tolist(),
        'product_id': [f'PROD{i:03d}' for i in range(1, 51)] * 20,
        'amount': [round(10 + 90 * i/1000, 2) for i in range(1, 1001)],
        'payment_method': ['credit_card', 'debit_card', 'cash', 'digital_wallet'] * 250
    }
    return pd.DataFrame(data)

def main():
    # Create sample data
    print("Creating sample transaction data...")
    df = create_sample_data()
    
    # Save sample data to a local CSV file
    sample_data_path = '/data/raw-zone/sample-data.csv'
    os.makedirs(os.path.dirname(sample_data_path), exist_ok=True)
    df.to_csv(sample_data_path, index=False)
    print(f"Sample data saved to {sample_data_path}")
    
    # Upload data to MinIO raw zone
    print("Uploading data to raw zone...")
    upload_dataframe_to_minio(df, 'raw-zone', 'transactions/transactions.csv')
    
    # Verify that the file was uploaded
    client = get_minio_client()
    objects = list(client.list_objects('raw-zone', prefix='transactions/'))
    if objects:
        print(f"Successfully uploaded to raw-zone. Objects: {[obj.object_name for obj in objects]}")
    else:
        print("Upload failed or no objects found")

if __name__ == "__main__":
    main()
```

#### Data Transformation Script

Create `scripts/02_transform_data.py`:

```python
"""
This script demonstrates data transformation and movement between zones in the data lake.
It reads data from the raw zone, performs transformations, and writes to the trusted zone.
"""
from utils import (
    download_dataframe_from_minio, 
    upload_dataframe_to_minio,
    execute_trino_query
)
import pandas as pd

def transform_transaction_data(df):
    """Perform transformations on the transaction data."""
    # Convert date string to datetime object
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    
    # Extract date components
    df['year'] = df['transaction_date'].dt.year
    df['month'] = df['transaction_date'].dt.month
    df['day'] = df['transaction_date'].dt.day
    
    # Categorize transactions by amount
    def categorize_amount(amount):
        if amount < 20:
            return 'low'
        elif amount < 50:
            return 'medium'
        else:
            return 'high'
    
    df['amount_category'] = df['amount'].apply(categorize_amount)
    
    # Convert to Parquet for better performance (columnar storage)
    return df

def main():
    # Download data from raw zone
    print("Downloading data from raw zone...")
    try:
        df = download_dataframe_from_minio('raw-zone', 'transactions/transactions.csv')
        print(f"Downloaded {len(df)} records")
    except Exception as e:
        print(f"Error downloading data: {e}")
        return
    
    # Transform data
    print("Transforming data...")
    transformed_df = transform_transaction_data(df)
    
    # Upload to trusted zone
    print("Uploading transformed data to trusted zone...")
    upload_dataframe_to_minio(
        transformed_df, 
        'trusted-zone', 
        'transactions/transactions.parquet', 
        format='parquet'
    )
    
    # Create Hive table via Trino
    print("Creating Hive table for the data...")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS hive.default.transactions (
        transaction_id BIGINT,
        customer_id VARCHAR,
        transaction_date TIMESTAMP,
        product_id VARCHAR,
        amount DOUBLE,
        payment_method VARCHAR,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        amount_category VARCHAR
    )
    WITH (
        external_location = 's3a://trusted-zone/transactions/',
        format = 'PARQUET'
    )
    """
    
    try:
        execute_trino_query(create_table_query)
        print("Hive table created successfully")
    except Exception as e:
        print(f"Error creating Hive table: {e}")

if __name__ == "__main__":
    main()
```

#### Data Query Script

Create `scripts/03_query_data.py`:

```python
"""
This script demonstrates querying and analyzing data from the data lake.
It uses Trino to run SQL queries against the Hive tables.
"""
from utils import execute_trino_query
import pandas as pd

def main():
    # Query 1: Get transaction counts by payment method
    print("Running Query 1: Transaction counts by payment method")
    query1 = """
    SELECT payment_method, COUNT(*) as transaction_count
    FROM hive.default.transactions
    GROUP BY payment_method
    ORDER BY transaction_count DESC
    """
    
    try:
        result1 = execute_trino_query(query1)
        print("\nTransaction counts by payment method:")
        print(result1)
    except Exception as e:
        print(f"Error executing Query 1: {e}")
    
    # Query 2: Transaction statistics by month
    print("\nRunning Query 2: Monthly transaction statistics")
    query2 = """
    SELECT 
        year, 
        month, 
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount,
        MIN(amount) as min_amount,
        MAX(amount) as max_amount
    FROM hive.default.transactions
    GROUP BY year, month
    ORDER BY year, month
    """
    
    try:
        result2 = execute_trino_query(query2)
        print("\nMonthly transaction statistics:")
        print(result2)
    except Exception as e:
        print(f"Error executing Query 2: {e}")
    
    # Query 3: Top customers by spending
    print("\nRunning Query 3: Top 10 customers by spending")
    query3 = """
    SELECT 
        customer_id, 
        COUNT(*) as transaction_count,
        SUM(amount) as total_spent
    FROM hive.default.transactions
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 10
    """
    
    try:
        result3 = execute_trino_query(query3)
        print("\nTop 10 customers by spending:")
        print(result3)
    except Exception as e:
        print(f"Error executing Query 3: {e}")

if __name__ == "__main__":
    main()
```

### 5. Create Docker Compose File

Create `docker-compose.yml`:

```yaml
version: '3'

services:
  # MinIO - S3 Compatible Object Storage (Storage Layer)
  minio:
    image: minio/minio:RELEASE.2023-03-20T20-16-18Z
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3

  # Create initial buckets in MinIO
  mc:
    image: minio/mc
    depends_on:
      - minio
    entrypoint: >
      /bin/sh -c "
      sleep 10;
      /usr/bin/mc config host add minio http://minio:9000 minioadmin minioadmin;
      /usr/bin/mc mb minio/raw-zone;
      /usr/bin/mc mb minio/trusted-zone;
      /usr/bin/mc mb minio/refined-zone;
      /usr/bin/mc mb minio/consumption-zone;
      exit 0;
      "

  # Mariadb for Hive Metastore
  mariadb:
    image: mariadb:10.6
    container_name: mariadb
    environment:
      MYSQL_ROOT_PASSWORD: admin
      MYSQL_USER: hive
      MYSQL_PASSWORD: hive
      MYSQL_DATABASE: metastore_db
    volumes:
      - mariadb_data:/var/lib/mysql
      - ./init/mariadb/init.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "3306:3306"

  # Hive Metastore - Metadata Management
  hive-metastore:
    image: apache/hive:3.1.3
    container_name: hive-metastore
    depends_on:
      - mariadb
    environment:
      DB_DRIVER: mysql
      DB_HOST: mariadb
      DB_NAME: metastore_db
      DB_USER: hive
      DB_PASSWORD: hive
      METASTORE_JDBC_URL: jdbc:mysql://mariadb:3306/metastore_db
      HADOOP_HOME: /opt/hadoop
    ports:
      - "9083:9083"
    volumes:
      - ./config/hive/hive-site.xml:/opt/hive/conf/hive-site.xml
      - ./data:/data

  # Trino (formerly PrestoSQL) - SQL Query Engine
  trino:
    image: trinodb/trino:403
    container_name: trino
    ports:
      - "8080:8080"
    volumes:
      - ./config/trino/:/etc/trino/
    depends_on:
      - hive-metastore
      - minio

  # Python client for interacting with Data Lake
  python-client:
    image: python:3.9-slim
    container_name: python-client
    volumes:
      - ./scripts:/scripts
      - ./data:/data
    working_dir: /scripts
    command: >
      bash -c "pip install minio pandas pyarrow requests trino && tail -f /dev/null"
    depends_on:
      - trino
      - minio

volumes:
  minio_data:
  mariadb_data:
```

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

Transform the data and create a Hive table:

```bash
docker exec -it python-client python /scripts/02_transform_data.py
```

Run example queries:

```bash
docker exec -it python-client python /scripts/03_query_data.py
```

## Exploring the Data Lake

### Access MinIO Console

Open your browser and navigate to:
- URL: http://localhost:9001
- Login with: minioadmin / minioadmin

You can explore the different buckets and view the data files.

### Query Data with Trino

Connect to the Trino CLI:

```bash
docker exec -it trino trino
```

Run some example queries:

```sql
-- Show available catalogs
SHOW CATALOGS;

-- Show schemas in the Hive catalog
SHOW SCHEMAS FROM hive;

-- Show tables in the default schema
SHOW TABLES FROM hive.default;

-- Query the transactions table
SELECT * FROM hive.default.transactions LIMIT 10;

-- Run an aggregation query
SELECT 
    payment_method, 
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM 
    hive.default.transactions
GROUP BY 
    payment_method
ORDER BY 
    total_amount DESC;
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
4. Create a Hive table via Trino to make it queryable

## Cleanup

When you're done with the environment:

```bash
# Stop the containers
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v
```

## Common Issues and Solutions

### Connection Issues to MinIO

If you're having trouble connecting to MinIO, check that:
- The MinIO container is running
- The access key and secret key are correct (minioadmin/minioadmin)
- You're using the correct endpoint (http://minio:9000 inside Docker, http://localhost:9000 from host)

### Trino Query Failures

If Trino queries fail:
- Ensure the Hive table was created correctly
- Verify the data exists in the expected location in MinIO
- Check Trino logs: `docker-compose logs trino`

### Python Client Errors

If Python scripts fail:
- Ensure all required packages are installed
- Check file permissions in the mounted volumes
- Verify the connection parameters in utils.py

## Next Steps and Extensions

After mastering the basic implementation, consider:

1. Adding data quality checks
2. Creating dimension tables and fact tables
3. Implementing a simple workflow orchestration
4. Building a visualization layer with tools like Metabase
5. Adding data governance and metadata tracking

## References

- [MinIO Documentation](https://docs.min.io/)
- [Trino Documentation](https://trino.io/docs/current/)
- [Apache Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-Metastore)
- [Data Lake Design Patterns](https://docs.aws.amazon.com/prescriptive-guidance/latest/defining-data-lake-architecture/welcome.html)
