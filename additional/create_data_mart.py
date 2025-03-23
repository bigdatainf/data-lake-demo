# Example 4: Creating a Data Mart in Consumption Zone
# create_data_mart.py

def create_sales_data_mart():
    """Create a sales data mart in the consumption zone."""
    # SQL to create a sales summary table
    create_sales_mart_query = """
    CREATE TABLE IF NOT EXISTS hive.default.sales_mart
    WITH (
        format = 'PARQUET',
        external_location = 's3a://consumption-zone/sales_mart/'
    )
    AS
    SELECT 
        t.year,
        t.month,
        p.category,
        COUNT(*) as transaction_count,
        SUM(t.amount) as total_sales,
        AVG(t.amount) as avg_sale_value,
        COUNT(DISTINCT t.customer_id) as unique_customers
    FROM 
        hive.default.transactions t
    JOIN 
        hive.default.dim_products p ON t.product_id = p.product_id
    GROUP BY 
        t.year, t.month, p.category
    """

    execute_trino_query(create_sales_mart_query)
    print("Sales data mart created in consumption zone")