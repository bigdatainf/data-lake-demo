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