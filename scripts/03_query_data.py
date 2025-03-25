"""
This script demonstrates querying the data lake using Python and pandas.
"""
from utils import download_dataframe_from_minio
import pandas as pd

def main():
    # Query 1: Get transaction counts by payment method
    print("Analyzing transaction data by payment method...")

    try:
        # Load data from trusted zone
        df = download_dataframe_from_minio('trusted-zone', 'transactions/transactions.parquet', format='parquet')

        # Analysis 1: Payment method distribution
        payment_method_counts = df.groupby('payment_method').size().reset_index(name='transaction_count')
        payment_method_counts['percentage'] = payment_method_counts['transaction_count'] / len(df) * 100

        print("\nTransaction counts by payment method:")
        print(payment_method_counts)

        # Analysis 2: Monthly transaction statistics
        monthly_stats = df.groupby(['year', 'month']).agg({
            'transaction_id': 'count',
            'amount': ['sum', 'mean', 'min', 'max']
        }).reset_index()

        monthly_stats.columns = ['year', 'month', 'transaction_count', 'total_amount', 'avg_amount', 'min_amount', 'max_amount']

        print("\nMonthly transaction statistics:")
        print(monthly_stats)

        # Analysis 3: Top customers by spending
        top_customers = df.groupby('customer_id').agg({
            'transaction_id': 'count',
            'amount': 'sum'
        }).reset_index()

        top_customers.columns = ['customer_id', 'transaction_count', 'total_spent']
        top_customers = top_customers.sort_values('total_spent', ascending=False).head(10)

        print("\nTop 10 customers by spending:")
        print(top_customers)

    except Exception as e:
        print(f"Error analyzing data: {e}")

if __name__ == "__main__":
    main()