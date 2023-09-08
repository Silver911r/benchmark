import time
import random
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client

def main():
    client = Client()

    n = 1000  # Number of records
    df = dd.from_pandas(pd.DataFrame({'key': [f'key_{i}' for i in range(n)], 'value': [f'value_{i}' for i in range(n)]}), npartitions=5)

    # Benchmarking Insert Operation
    start_time = time.time()
    
    batch_size = 100
    batch = []
    for i in range(n):
        new_row = pd.DataFrame({'key': [f'key_{n + i}'], 'value': [f'value_{n + i}']})
        batch.append(new_row)
        if (i+1) % batch_size == 0:
            df = dd.concat([df, dd.from_pandas(pd.concat(batch, ignore_index=True), npartitions=1)])
            batch = []
            
    if batch:  # In case there are any remaining items in the batch
        df = dd.concat([df, dd.from_pandas(pd.concat(batch, ignore_index=True), npartitions=1)])
        
    end_time = time.time()
    print(f"Dask Insert Time: {end_time - start_time}")

    # Benchmarking Search Operation
    start_time = time.time()
    for _ in range(100):
        key = f'key_{random.randint(0, 2 * n - 1)}'
        result = df[df['key'] == key].compute()
    end_time = time.time()
    print(f"Dask Search Time: {end_time - start_time}")

    # Benchmarking Delete Operation (via filtering)
    start_time = time.time()
    for _ in range(100):
        key = f'key_{random.randint(0, 2 * n - 1)}'
        df = df[df['key'] != key]
    end_time = time.time()
    print(f"Dask Delete Time (via filtering): {end_time - start_time}")

if __name__ == '__main__':
    main()
