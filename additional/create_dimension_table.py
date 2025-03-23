# Example 2: Creating a dimension table in the Data Lake
# create_dimension_table.py

from utils import execute_trino_query, upload_dataframe_to_minio
import pandas as pd

def create_product_dimension():
    """Create a product dimension table in the data lake."""
    # Sample product data
    products_data = {
        'product_id': [f'PROD{i:03d}' for i in range(1, 51)],
        'product_name': [f'Product {i}' for i in range(1, 51)],
        'category': ['Electronics'] * 10 + ['Clothing'] * 15 + ['Home'] * 10 + ['Food'] * 10 + ['Other'] * 5,
        'price': [round(10 + i * 5, 2) for i in range(1, 51)],
        'in_stock': [True] * 40 + [False] * 10
    }

    products_df = pd.DataFrame(products_data)

    # Upload to trusted zone
    upload_dataframe_to_minio(
        products_df,
        'trusted-zone',
        'dimensions/products.parquet',
        format='parquet'
    )

    # Create Hive table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS hive.default.dim_products (
        product_id VARCHAR,
        product_name VARCHAR,
        category VARCHAR,
        price DOUBLE,
        in_stock BOOLEAN
    )
    WITH (
        external_location = 's3a://trusted-zone/dimensions/',
        format = 'PARQUET'
    )
    """

    execute_trino_query(create_table_query)
    print("Product dimension table created")