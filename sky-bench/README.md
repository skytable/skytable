# Skytable Benchmark Tool

`sky-bench` is Skytable's benchmarking tool. Unlike most other benchmarking tools, Skytable's benchmark
tool doesn't do anything "fancy" to make benchmarks appear better than they are. As it happens, the benchmark tool might show Skytable to be slower!

Here's how the benchmark tool works (it's dead simple):

1. Depending on the configuration it launches "network pools" which are just thread pools where each worker
   holds a persistent connection to the database (something like a connection pool)
2. A collection of unique, random keys are generated using a PRNG provided by the `rand` library that is
   seeded using the OS' source of randomness. The values are allowed to repeat
3. The [Skytable Rust driver](https://github.com/skytable/client-rust) is used to generate _raw query packets_. To put it simply, the keys and values are turned into `Query` objects and then into the raw bytes that will be sent over the network. (This just makes it simpler to design the network pool interface)
4. For every type of benchmark (GET,SET,...) we use the network pool to send all the bytes and wait until we receive the expected response. We time how long it takes to send and receive the response for all the queries for the given test (aggregate)
5. We repeat this for all the remaining tests
6. We repeat the entire set of tests 5 times (by default, this can be changed).
7. We do the calculations and output the results.

## License

All files in this directory are distributed under the [AGPL-3.0 License](../LICENSE).
