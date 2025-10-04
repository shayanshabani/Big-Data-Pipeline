# data-generator/generate_parquet.py

import sys
import argparse
from datetime import datetime, timedelta
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

class SalesDataGenerator:
    """
    Generates sample sales data in Parquet format with Snappy compression.
    Data includes transactions with products, customers, and payment information.
    """
    
    CATEGORIES = [
        'Electronics', 'Clothing', 'Books', 'Home & Garden', 
        'Sports', 'Toys', 'Food & Beverage', 'Health & Beauty'
    ]
    
    COUNTRIES = [
        'USA', 'UK', 'Germany', 'France', 'Canada', 
        'Australia', 'Japan', 'India', 'Brazil', 'Spain'
    ]
    
    PAYMENT_METHODS = [
        'Credit Card', 'Debit Card', 'PayPal', 
        'Bank Transfer', 'Cash', 'Crypto'
    ]
    
    PRODUCTS = {
        'Electronics': [
            ('ELEC001', 'Laptop Pro 15"', 999.99),
            ('ELEC002', 'Wireless Headphones', 149.99),
            ('ELEC003', 'Smartphone X', 799.99),
            ('ELEC004', 'Smart Watch', 299.99),
            ('ELEC005', 'Tablet 10"', 449.99),
        ],
        'Clothing': [
            ('CLTH001', 'Designer Jeans', 89.99),
            ('CLTH002', 'Cotton T-Shirt', 24.99),
            ('CLTH003', 'Winter Jacket', 149.99),
            ('CLTH004', 'Running Shoes', 119.99),
            ('CLTH005', 'Leather Boots', 199.99),
        ],
        'Books': [
            ('BOOK001', 'Data Engineering Handbook', 49.99),
            ('BOOK002', 'Python for Beginners', 34.99),
            ('BOOK003', 'Spark in Action', 59.99),
            ('BOOK004', 'Machine Learning Basics', 44.99),
            ('BOOK005', 'Cloud Architecture', 54.99),
        ],
        'Home & Garden': [
            ('HOME001', 'Coffee Maker', 79.99),
            ('HOME002', 'Garden Tools Set', 129.99),
            ('HOME003', 'Bed Sheets Set', 59.99),
            ('HOME004', 'Wall Clock', 39.99),
            ('HOME005', 'Indoor Plant', 24.99),
        ],
        'Sports': [
            ('SPRT001', 'Yoga Mat', 29.99),
            ('SPRT002', 'Dumbbell Set', 149.99),
            ('SPRT003', 'Tennis Racket', 89.99),
            ('SPRT004', 'Basketball', 34.99),
            ('SPRT005', 'Cycling Helmet', 69.99),
        ],
        'Toys': [
            ('TOYS001', 'Building Blocks Set', 49.99),
            ('TOYS002', 'Remote Control Car', 79.99),
            ('TOYS003', 'Board Game', 34.99),
            ('TOYS004', 'Puzzle 1000 pieces', 24.99),
            ('TOYS005', 'Action Figure', 19.99),
        ],
        'Food & Beverage': [
            ('FOOD001', 'Coffee Beans 1kg', 24.99),
            ('FOOD002', 'Green Tea Box', 14.99),
            ('FOOD003', 'Chocolate Box', 19.99),
            ('FOOD004', 'Olive Oil 1L', 29.99),
            ('FOOD005', 'Organic Honey', 17.99),
        ],
        'Health & Beauty': [
            ('HLTH001', 'Face Cream', 39.99),
            ('HLTH002', 'Shampoo & Conditioner Set', 29.99),
            ('HLTH003', 'Vitamin Supplements', 34.99),
            ('HLTH004', 'Perfume 50ml', 89.99),
            ('HLTH005', 'Makeup Kit', 79.99),
        ],
    }
    
    def __init__(self, num_records=1000, start_time=None):
        """
        Initialize the data generator.
        
        Args:
            num_records: Number of transaction records to generate
            start_time: Starting timestamp for transactions (default: now - 1 hour)
        """
        self.num_records = num_records
        self.start_time = start_time or (datetime.now() - timedelta(hours=1))
        
    def generate_transaction_id(self, index):
        """Generate a unique transaction ID."""
        timestamp = int(datetime.now().timestamp() * 1000)
        return f"TXN{timestamp}{index:06d}"
    
    def generate_customer_id(self):
        """Generate a random customer ID."""
        return f"CUST{random.randint(10000, 99999)}"
    
    def generate_timestamp(self, index):
        """
        Generate a timestamp for the transaction.
        Distributes transactions over time with some clustering.
        """
        # Add some randomness but keep general time progression
        seconds_offset = (index / self.num_records) * 3600  # Spread over 1 hour
        random_jitter = random.uniform(-300, 300)  # +/- 5 minutes jitter
        
        return self.start_time + timedelta(seconds=seconds_offset + random_jitter)
    
    def select_product(self):
        """Randomly select a product with its details."""
        category = random.choice(self.CATEGORIES)
        product_id, product_name, base_price = random.choice(self.PRODUCTS[category])
        
        # Add some price variation (±10%)
        price = base_price * random.uniform(0.9, 1.1)
        
        return {
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'price': round(price, 2)
        }
    
    def generate_record(self, index):
        """Generate a single sales transaction record."""
        product = self.select_product()
        
        return {
            'transaction_id': self.generate_transaction_id(index),
            'timestamp': self.generate_timestamp(index),
            'product_id': product['product_id'],
            'product_name': product['product_name'],
            'category': product['category'],
            'quantity': random.randint(1, 10),
            'price': product['price'],
            'customer_id': self.generate_customer_id(),
            'customer_country': random.choice(self.COUNTRIES),
            'payment_method': random.choice(self.PAYMENT_METHODS)
        }
    
    def generate_data(self):
        """Generate all records and return as DataFrame."""
        print(f"Generating {self.num_records} sales transactions...")
        
        records = [self.generate_record(i) for i in range(self.num_records)]
        df = pd.DataFrame(records)
        
        print(f"Generated {len(df)} records")
        print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"\nSample records:")
        print(df.head())
        print(f"\nData summary:")
        print(df.describe())
        
        return df
    
    def save_to_parquet(self, df, output_file):
        """
        Save DataFrame to Parquet file with Snappy compression.
        
        Args:
            df: pandas DataFrame to save
            output_file: Output file path
        """
        print(f"\nSaving to Parquet file: {output_file}")
        
        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Define schema with proper types
        schema = pa.schema([
            ('transaction_id', pa.string()),
            ('timestamp', pa.timestamp('us')),
            ('product_id', pa.string()),
            ('product_name', pa.string()),
            ('category', pa.string()),
            ('quantity', pa.int32()),
            ('price', pa.float64()),
            ('customer_id', pa.string()),
            ('customer_country', pa.string()),
            ('payment_method', pa.string())
        ])
        
        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df, schema=schema)
        
        # Write with Snappy compression
        pq.write_table(
            table,
            output_file,
            compression='snappy',
            use_dictionary=True,
            use_deprecated_int96_timestamps=False
        )
        
        # Print file info
        file_size = output_path.stat().st_size
        print(f"File saved successfully!")
        print(f"File size: {file_size / 1024:.2f} KB ({file_size / (1024*1024):.2f} MB)")
        
        # Verify the file
        self.verify_parquet(output_file)
    
    def verify_parquet(self, file_path):
        """Verify the generated Parquet file."""
        print(f"\nVerifying Parquet file...")
        
        parquet_file = pq.ParquetFile(file_path)
        
        print(f"Number of row groups: {parquet_file.num_row_groups}")
        print(f"Schema: {parquet_file.schema}")
        print(f"Metadata: {parquet_file.metadata}")
        
        # Read back and display sample
        df_read = pd.read_parquet(file_path)
        print(f"\nRead {len(df_read)} records from file")
        print("Sample data:")
        print(df_read.head(3))


def main():
    parser = argparse.ArgumentParser(
        description='Generate sample sales data in Snappy-compressed Parquet format'
    )
    
    parser.add_argument(
        'filename',
        help='Output filename (e.g., sales_2024_10_04_001.snappy.parquet)'
    )
    
    parser.add_argument(
        '-n', '--num-records',
        type=int,
        default=1000,
        help='Number of records to generate (default: 1000)'
    )
    
    parser.add_argument(
        '-s', '--start-time',
        help='Start time for transactions (format: YYYY-MM-DD HH:MM:SS)'
    )
    
    args = parser.parse_args()
    
    # Parse start time if provided
    start_time = None
    if args.start_time:
        try:
            start_time = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f"Error: Invalid date format. Use YYYY-MM-DD HH:MM:SS")
            sys.exit(1)
    
    # Ensure filename has correct extension
    filename = args.filename
    if not filename.endswith('.parquet'):
        filename += '.snappy.parquet'
    elif not filename.endswith('.snappy.parquet'):
        filename = filename.replace('.parquet', '.snappy.parquet')
    
    # Generate and save data
    try:
        generator = SalesDataGenerator(
            num_records=args.num_records,
            start_time=start_time
        )
        
        df = generator.generate_data()
        generator.save_to_parquet(df, filename)
        
        print(f"\n✓ Successfully generated {filename}")
        
    except Exception as e:
        print(f"\n✗ Error generating data: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
