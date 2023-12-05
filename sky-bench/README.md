# Skytable Benchmark Tool

`sky-bench` is Skytable's benchmarking tool. Unlike most other benchmarking tools, Skytable's benchmark
tool doesn't do anything "fancy" to make benchmarks appear better than they are. As it happens, the benchmark tool might show Skytable to be slower!

> **The benchmarking engine is currently experimental!** You can indirectly compare it to the working of `redis-benchmark` but one
> important note: **Skytable has a full fledged query language.** *Even then, you will probably enjoy the benchmarks!*
>
> Other tools like `memtier_benchmark` are far more sophisticated and use several strategies that can hugely affect benchmark 
> numbers.
>
> We will upgrade the benchmark engine from time to time to improve the reporting of statistics. For example, right now the 
> engine only outputs the slowest and fastest query speeds **in nanoseconds** but we plan to provide a overall distribution of 
> latencies.

## Working

Here's how the benchmark tool works (it's dead simple):

1. We spawn up multiple client tasks (or what you can call "green threads") that can each handle tasks. These tasks are run on a threadpool which has multiple worker threads, kind of simulating multiple "application server instances"
   - You can use `--connections` to set the number of client connections
   - You can use `--threads` to set the number of threads to be used
2. An overall target is sent to these tasks and all tasks start executing queries until the overall target is reached
   > The distribution of tasks across clients is generally unspecified, but because of Skytable's extremely low average latencies, in most common scenarios, the distribution is even.
3. Once the overall target is reached, each task relays its local execution statistics to the monitoring task
4. The monitoring task then prepares the final results, and this is returned

### Engines

There are two benchmark engines:

- `fury`: this is the new experimental engine, but also set as the default engine. It is generally more efficient and tracks statistics more effectively. At the same time, it is capable of generating larger consistent loads without crashing or blowing up CPU usage
- `rookie`: this is the old engine that's still available but is not used by default. it still uses lesser memory than prior versions (which used a very inefficient and memory hungry algorithm) but is not as resource efficient as the `fury` engine.
