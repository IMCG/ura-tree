----------------- DATA FORMAT -------------------------------
Type:
    - Describes the dataset being used in this test
    - "Skew" means a generally non-ordered dataset. Keys are distributed in no particular order in each file.
    - "Sorted" means that keys are sorted in each file.

Threads:
    - The number of threads involved in this test case
    - Ranges from 1 - 32 in powers of 2

Total-Time:
    - Measured time in seconds from when the code begins to execute until indexing is finished
    - A very general indicator of performance.

Thread-Id:
    - The particular thread whose stats are being recorded
    - Note that all threads collect stats individually

Thread-Time:
    - Measured time in seconds from when a thread begins to execute until indexing is finished
    - Another general indicator of performance

Root-Read-Wait:
    - The amount of time in nanoseconds that read operations spend trying to get a read lock on root node
    - Typically increases with contention

Root-Write-Wait:
    - The amount of time in nanoseconds that write operations spend trying to get a write lock on root node
    - Typically increases with contention

Readlock-Wait:
    - The amount of time in nanoseconds that read operations spend trying to get all read locks
    - Typically increases with contention

Writelock-Wait:
    - The amount of time in nanoseconds that write operations spend trying got get all write locks
    - Typically increases with contention

Readlock-Acquired:
    - The number of read locks acquired by this thread

Writelock-Acquired:
    - The number of write locks acquired by this thread

Readlock-Failed:
    - The number of times this thread has failed to get a read lock
    - Thread could fail multiple times trying to get one lock

Writelock-Failed:
    - The number of times this thread has failed to get a write lock
    - Thread could fail multiple times trying to get one lock

LowFence-Overwrites:
    - The number of times the lower fence key has been overwritten
    - Should always be 0

Optimistic-Successes:
    - The number of times the optimistic search strategy has succeeded

Optimistic-Failures:
    - The number of times the optimistic search strategy has failed
    - Counted at beginning of fallback search strategy, so should be high in non-optimistic search cases


------------ ANALYSIS IN EXCEL SO FAR -----------------------

Readlock_Wait_Ratio:
    - The ratio of wait time of regular search strategy vs optimistic search strategy for read locks
    - Higher is better

Writelock_Wait_Ratio:
    - The ratio of wait time of regular search strategy vs optimistic search strategy for write locks
    - Higher is better

Readlock_Acquired_Ratio:
    - The ratio of successfully acquired read locks in regular search strategy vs optimistic search strategy
    - Higher is better

Writelock_Acquired_Ratio:
    - The ratio of successfully acquired write locks in regular search strategy vs optimistic search strategy
    - Higher is better

Read_Regular_Latency:
    - How long it takes, on average, for a read lock to be successfully acquired in nanoseconds
    - Using regular search strategy
    - Lower is better

Write_Regular_Latency:
    - How long it takes, on average, for a write lock to be successfully acquired in nanoseconds
    - Using regular search strategy
    - Lower is better

Read_Optimistic_Latency:
    - How long it takes, on average, for a read lock to be successfully acquired in nanoseconds
    - Using optimistic search strategy
    - Lower is better

Write_Optimistic_Latency:
    - How long it takes, on average, for a write lock to be successfully acquired in nanoseconds
    - Using optimistic search strategy
    - Lower is better

Regular_Throughput:
    - The number of write locks successfully acquired per second of thread execution
    - Uses regular search strategy
    - Higher is better

Optimistic_Throughput:
    - The number of write locks successfully acquired per second of thread execution
    - Uses optimistic search strategy
    - Higher is better


--------------- TEST DESCRIPTIONS ---------------------------

Test01:
    - Works on skew datasets
    - No control over which core each thread uses
    - Uses read and write locks
    - No information on per-thread execution time

Test02:
    - Works on skew datasets
    - No control over which core each thread uses
    - Uses read and write locks
    - Per-thread execution time included

Test03:
    - Works on skew datasets
    - Groups threads to nearby cores if possible
    - Uses read and write locks
    - Per-thread execution time included

Test04 (Scrapped):
    - Works on skew datasets
    - Groups threads to nearby cores if possible
    - Uses only write locks
        - Note: Much larger percentage of CPU time taken by system calls
    - Per-thread execution time included

    Result:
    	- Cut off when first index took >4.5 hours

Test05:
    - Works on larger randomized dataset with at most ten repetitions of each key
    - Groups threads to nearby cores if possible
    - Usesread and write locks
    - Per-thread execution time included

Test06 (Planned):
    - 
