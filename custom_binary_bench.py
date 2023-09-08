
import os
import random
import time

HEADER_SIZE = 4  # Bytes
COLUMN_META_SIZE = 2  # Bytes for our simple example
ROW_SIZE = 24  # Bytes

def write_row(f, row):
    f.write(row[0].to_bytes(4, 'little'))
    f.write(row[1].ljust(20).encode('utf-8'))

def write_custom_file(filename, n):
    with open(filename, 'wb') as f:
        f.write((1).to_bytes(4, 'little'))  # Version number
        for i in range(n):
            write_row(f, (i, f'str_{i}'))

def read_random_rows(filename, row_numbers):
    rows = []
    with open(filename, 'rb') as f:
        for row_number in row_numbers:
            f.seek(HEADER_SIZE + row_number * ROW_SIZE)
            row_bytes = f.read(ROW_SIZE)
            row_int = int.from_bytes(row_bytes[:4], 'little')
            row_str = row_bytes[4:].decode('utf-8').strip()
            rows.append((row_int, row_str))
    return rows

def main():
    filename = 'custom_file.dat'
    n = 31000000  # Number of records

    # Write benchmark
    start_time = time.time()
    write_custom_file(filename, n)
    end_time = time.time()
    print(f"Custom File Write Time: {end_time - start_time}")

    # Read benchmark
    random_row_numbers = [random.randint(0, n-1) for _ in range(100)]
    start_time = time.time()
    rows = read_random_rows(filename, random_row_numbers)
    end_time = time.time()
    print(f"Custom File Read Time: {end_time - start_time}")

    # Clean up
    os.remove(filename)

if __name__ == '__main__':
    main()
