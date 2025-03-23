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