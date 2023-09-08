import time
import random
import sqlite3
import pandas as pd
import dask.dataframe as dd

# Initialize SQLite database
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()
cursor.execute("CREATE TABLE table_db (key TEXT, value TEXT)")

start_time = time.time()
for i in range(n):
    key = f'key_{i}'
    value = f'value_{i}'
    cursor.execute("INSERT INTO table_db VALUES (?, ?)", (key, value))
conn.commit()
end_time = time.time()
print(f"SQLite Insert Time: {end_time - start_time}")

start_time = time.time()
for _ in range(100):
    key = f'key_{random.randint(0, n-1)}'
    cursor.execute("SELECT value FROM table_db WHERE key=?", (key,))
    _ = cursor.fetchone()
end_time = time.time()
print(f"SQLite Search Time: {end_time - start_time}")

start_time = time.time()
for _ in range(100):
    key = f'key_{random.randint(0, n-1)}'
    cursor.execute("DELETE FROM table_db WHERE key=?", (key,))
conn.commit()
end_time = time.time()
print(f"SQLite Delete Time: {end_time - start_time}")

conn.close()
