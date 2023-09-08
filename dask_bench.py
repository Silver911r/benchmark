import time
import random
import sqlite3
import pandas as pd
import dask.dataframe as dd

# Initialize Dask DataFrame
n = 1000  # Number of records
df = dd.from_pandas(pd.DataFrame({'key': [f'key_{i}' for i in range(n)], 'value': [f'value_{i}' for i in range(n)]}), npartitions=5)

start_time = time.time()
for i in range(n):
    new_row = dd.from_pandas(pd.DataFrame({'key': [f'key_{n + i}'], 'value': [f'value_{n + i}']}), npartitions=1)
    df = dd.concat([df, new_row])
end_time = time.time()
print(f"Dask Insert Time: {end_time - start_time}")

start_time = time.time()
for _ in range(100):
    key = f'key_{random.randint(0, n-1)}'
    _ = df[df['key'] == key].compute()
end_time = time.time()
print(f"Dask Search Time: {end_time - start_time}")

# Dask doesn't support in-place deletion, so we're just timing a filter operation here
start_time = time.time()
for _ in range(100):
    key = f'key_{random.randint(0, n-1)}'
    df = df[df['key'] != key]
end_time = time.time()
print(f"Dask Delete Time (via filtering): {end_time - start_time}")
