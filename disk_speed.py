import os
import time

def write_speed_test(filename='speed_test_file', block_size=1024, num_blocks=1000):
    with open(filename, 'wb') as f:
        start_time = time.time()
        for _ in range(num_blocks):
            f.write(os.urandom(block_size))
        end_time = time.time()
    os.remove(filename)
    write_speed = (block_size * num_blocks) / (end_time - start_time) / 1024 / 1024  # in MB/s
    return write_speed

def read_speed_test(filename='speed_test_file', block_size=1024, num_blocks=1000):
    with open(filename, 'wb') as f:
        for _ in range(num_blocks):
            f.write(os.urandom(block_size))

    with open(filename, 'rb') as f:
        start_time = time.time()
        while f.read(block_size):
            pass
        end_time = time.time()
    os.remove(filename)
    read_speed = (block_size * num_blocks) / (end_time - start_time) / 1024 / 1024  # in MB/s
    return read_speed

if __name__ == '__main__':
    print(f"Measuring write speed...")
    write_speed = write_speed_test()
    print(f"Write Speed: {write_speed:.2f} MB/s")

    print(f"Measuring read speed...")
    read_speed = read_speed_test()
    print(f"Read Speed: {read_speed:.2f} MB/s")
