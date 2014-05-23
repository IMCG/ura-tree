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

Test0x (Scrapped):
    - Works on skew datasets
    - Groups threads to nearby cores if possible
    - Uses only write locks
        - Note: Much larger percentage of CPU time taken by system calls
    - Per-thread execution time included
	- Cut off when first index took >4.5 hours

Test04 (Planned):
    - Works on larger randomized dataset with at most three repetitions of each key
    - Groups threads to nearby cores if possible
    - Uses only write locks
    - Per-thread execution time included
