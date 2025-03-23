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