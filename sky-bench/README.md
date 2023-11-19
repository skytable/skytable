# Skytable Benchmark Tool

`sky-bench` is Skytable's benchmarking tool. Unlike most other benchmarking tools, Skytable's benchmark
tool doesn't do anything "fancy" to make benchmarks appear better than they are. As it happens, the benchmark tool might show Skytable to be slower!

Here's how the benchmark tool works (it's dead simple):

1. We start up some threads with each having a thread local connection to the database
2. Each thread attempts to keep running queries until the target number of queries is reached.
   - This sort of simulates a real-world scenario where these threads are like your application servers sending requests to the database
   - Also there is no ideal distribution and the number of queries each worker runs is unspecified (but owing to low latencies from the database, that should be even)
   - We do this to ensure that the distribution of queries executed by each "server" is skewed as it would be in the real world.
3. Once the target number of queries are reached, the workers notify that the task is complete. Each worker keeps track of how long it spent processing queries and this is also notified to the benchmark engine
4. The benchmark engine then computes relevant statistics
