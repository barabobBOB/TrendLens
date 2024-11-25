import pandas as pd

try:
    df = pd.read_parquet("2024-11-25_52e4fc75-972a-439f-8641-732dff11d131.parquet")
    print(df)
except Exception as e:
    print("Error reading Parquet file:", e)
