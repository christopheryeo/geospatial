import pandas as pd
import pyarrow.parquet as pq

def read_parquet_and_print(filepath):
  """
  Reads a parquet file and prints the first 10 records.

  Args:
      filepath (str): Path to the parquet file.
  """
  try:
    # Read the parquet file
    table = pq.read_table(filepath)
    # Convert table to pandas DataFrame
    df = table.to_pandas()
    print(df.info())
    
    # Print the first 10 records
    print(df.head(10))
  except FileNotFoundError:
    print(f"Error: File not found: {filepath}")

if __name__ == "__main__":
  # Replace 'path/to/your/file.parquet' with the actual path to your file
  filepath = 'sample-telco-location/day=20180701'
  read_parquet_and_print(filepath)
