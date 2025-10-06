# data-generator/generate_parquet.py

import sys
import argparse
from datetime import datetime, timedelta
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path



def create_subset_file(input_path, output_path, num_rows=10):
    """
    Reads a large Parquet file and creates a new Parquet file
    containing only the first N rows.
    """
    try:
        print(f"--- Reading source file: {input_path} ---")
        df = pd.read_parquet(input_path, engine='pyarrow')
        print(f"✅ Source file read successfully. Total rows: {len(df)}")

        # Create a subset of the DataFrame containing the first N rows
        print(f"\n--- Selecting the first {num_rows} rows ---")
        df_subset = df.sample(num_rows)
        print(f"✅ Subset created with {len(df_subset)} rows.")

        # Write the subset to a new Snappy-compressed Parquet file
        print(f"\n--- Writing to new file: {output_path} ---")
        df_subset.to_parquet(
            output_path,
            engine='pyarrow',
            compression='snappy', # Explicitly specify Snappy compression
            index=False,           # Recommended: Do not write the pandas index as a column
            use_deprecated_int96_timestamps=True
        )
        print(f"\n🎉 Success! New file '{output_path}' has been created.")

    except FileNotFoundError:
        print(f"\n❌ ERROR: The input file '{input_path}' was not found.", file=sys.stderr)
        print("Please make sure the file exists in the same directory as the script.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)



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
        default=10,
        help='Number of records to generate (default: 10)'
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

    input_path = "data.snappy.parquet"
    
    # Generate and save data
    try:
        create_subset_file(
            input_path=input_path,
            output_path=filename,
            num_rows=args.num_records
        )
        print(f"\n✓ Successfully generated {filename}")
        
    except Exception as e:
        print(f"\n✗ Error generating data: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
