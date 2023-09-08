import time
import random
import sqlite3
import pandas as pd
import dask.dataframe as dd

# Initialize dictionary
table_dict = {}
n = 1000  # Number of records

start_time = time.time()
for i in range(n):
    key = f'key_{i}'
    value = f'value_{i}'
    table_dict[key] = value
end_time = time.time()
print(f"Dictionary Insert Time: {end_time - start_time}")

start_time = time.time()
for _ in range(100):
    key = random.choice(list(table_dict.keys()))
    _ = table_dict.get(key)
end_time = time.time()
print(f"Dictionary Search Time: {end_time - start_time}")

start_time = time.time()
for _ in range(100):
    key = random.choice(list(table_dict.keys()))
    _ = table_dict.pop(key, None)
end_time = time.time()
print(f"Dictionary Delete Time: {end_time - start_time}")
