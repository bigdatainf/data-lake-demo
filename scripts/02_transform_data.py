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