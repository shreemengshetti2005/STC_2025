import random
import time
import multiprocessing
import threading
import psutil
import os
from functools import reduce
import heapq

# -----------------------------
# CPU Monitor
# -----------------------------
def monitor_cpu(stop_event, usage_list):
    """
    Runs in a separate thread.
    Monitors CPU usage per core every 0.5 seconds.
    Does not use significant CPU itself; just observes usage.
    """
    while not stop_event.is_set():
        usage = psutil.cpu_percent(percpu=True)
        usage_list.append(usage)
        time.sleep(0.5)

# -----------------------------
# Merge Function (k-way merge)
# -----------------------------
def merge_k_sorted(lists):
    """
    Merges multiple sorted lists into one sorted list.
    CPU is actively used here, but this is sequential in your current code.
    """
    return list(heapq.merge(*lists))

# -----------------------------
# Sequential Merge Sort
# -----------------------------
def merge_sort(arr):
    """
    Classic recursive merge sort.
    This function runs **entirely on the main CPU core** during sequential sort.
    CPU is fully utilized by this function but only on a single core.
    """
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = merge_sort(arr[:mid])   # CPU-intensive recursive call
    right = merge_sort(arr[mid:])  # CPU-intensive recursive call
    return list(heapq.merge(left, right))  # CPU-intensive merge

# -----------------------------
# Parallel Merge Sort
# -----------------------------
def parallel_merge_sort(arr):
    nprocs = multiprocessing.cpu_count()  # Number of CPU cores available

    # -----------------------------
    # Split array into chunks for each core
    # -----------------------------
    chunk_size = len(arr) // nprocs
    chunks = [arr[i*chunk_size : (i+1)*chunk_size] for i in range(nprocs)]
    if len(arr) % nprocs:  # Handle remainder elements
        chunks[-1].extend(arr[nprocs*chunk_size:])

    # -----------------------------
    # Sort each chunk in parallel
    # -----------------------------
    # At this point, each CPU core gets one chunk to sort.
    # CPU cores are now fully utilized independently.
    # Each core runs merge_sort() on its chunk in parallel.
    with multiprocessing.Pool(processes=nprocs) as pool:
        sorted_chunks = pool.map(merge_sort, chunks)  # CPU-intensive on multiple cores

    # -----------------------------
    # Merge all sorted chunks together
    # -----------------------------
    # This merge is currently **sequential** on a single core.
    # For very large arrays, this can become a bottleneck.
    return merge_k_sorted(sorted_chunks)

# -----------------------------
# Main Execution
# -----------------------------
if __name__ == "__main__":  # Fix: double underscores
    # Generate 1 million random numbers
    N = 10**6
    arr = [random.randint(0, 10**6) for _ in range(N)]  # CPU used minimally
    arr_copy = arr.copy()  # CPU used minimally

    # -----------------------------
    # Sequential Merge Sort
    # -----------------------------
    seq_usage = []
    seq_stop = threading.Event()
    seq_thread = threading.Thread(target=monitor_cpu, args=(seq_stop, seq_usage))
    seq_thread.start()  # CPU monitoring runs on a separate thread

    start_time = time.time()
    sorted_seq = merge_sort(arr)  # CPU-intensive work on a single core
    seq_time = time.time() - start_time

    seq_stop.set()
    seq_thread.join()
    print(f"Sequential Merge Sort took {seq_time:.2f} seconds")
    print("CPU Usage during Sequential Merge Sort (sample):", seq_usage[:10])

    # -----------------------------
    # Parallel Merge Sort
    # -----------------------------
    par_usage = []
    par_stop = threading.Event()
    par_thread = threading.Thread(target=monitor_cpu, args=(par_stop, par_usage))
    par_thread.start()  # CPU monitoring thread runs separately

    start_time = time.time()
    sorted_par = parallel_merge_sort(arr_copy)  
    # CPU-intensive: multiple cores actively sorting their chunks in parallel
    # Final merge uses CPU sequentially on one core
    par_time = time.time() - start_time

    par_stop.set()
    par_thread.join()
    print(f"Parallel Merge Sort took {par_time:.2f} seconds")
    print("CPU Usage during Parallel Merge Sort (sample):", par_usage[:10])

    # Verify both results are same
    assert sorted_seq == sorted_par, "Sorting results do not match!"
    print("Sorting results match!")
