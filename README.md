# dlp_mpi - Data-level parallelism with mpi for python

Task parallelism fits better as name

tmpi?

<table>
<tr>
<th>
Run an serial algorithm on multiple examples
</th>
<th>
Use dlp_mpi to run the loop body in parallel
</th>
<th>
Use dlp_mpi to run a function in parallel
</th>
</tr>
<tr>
<td>

```python
# python script.py

import time


examples = list(range(10))
results = []







for example in examples:

    # Some heavy workload:
    # CPU or IO
    time.sleep(0.2)
    result = example

    # Remember the results
    results.append(result)










# Summarize your experiment
print(sum(results))
```
</td>
<td>

```python
# mpiexec -np 8 python script.py

import time
import dlp_mpi

examples = list(range(10))
results = []







for example in dlp_mpi.split_managed(
        examples):
    # Some heavy workload:
    # CPU or IO
    time.sleep(0.2)
    result = example

    # Remember the results
    results.append(result)

results = dlp_mpi.gather(results)

if dlp_mpi.IS_MASTER:
    results = [
        result
        for worker_results in results
        for result in worker_results
    ]
    
    # Summarize your experiment
    print(results)
```
</td>
<td>

```python
# mpiexec -np 8 python script.py

import time
import dlp_mpi

examples = list(range(10))
results = []

def work_load(example):
    # Some heavy workload:
    # CPU or IO
    time.sleep(0.2)
    result = example

for result in dlp_mpi.map_unordered(
        work_load, examples):





    # Remember the results
    results.append(result)









if dlp_mpi.IS_MASTER:
    # Summarize your experiment
    print(results)
```
</td>
</tr>
</table>

This package uses `mpi4py` to provide utilities to parallize algorithms that are applied to multiple examples.

The core idea is: Start `N` processes and each process works on a subset of all examples.
To start the processes `mpiexec` can be used. Most HPC systems support MPI to scatter the workload across multiple hosts. For the command, look in the documentation for your HPC system and search for MPI launches.

Since each process should operate on different examples, MPI provides the variables `RANK` and `SIZE`, where `SIZE` is the number of workers and `RANK` is a unique identifier from `0` to `SIZE - 1`.
The simplest way to improve the execution time is to process `examples[RANK::SIZE]` on each worker.
This is a round robin load balancing (`dlp_mpi.split_round_robin`).
An more advanced load balaning is `dlp_mpi.split_managed`, where one process manages the load and assigns a new task to a worker, once he finishes the last task.

When in the end of a program all results should be summariesd or written in a single file, comunication between all processes is nessesary.
For this purpose `dlp_mpi.gather` (`mpi4py.MPI.COMM_WORLD.gather`) can be used. This function sends all data to the root process (For serialisation is `pickle` used).

As alternative to splitting the data, this package also provides a `map` style parallelization (see example in the beginning):
The function `dlp_mpi.map_unordered` calls `work_load` in parallel and executes the `for` body in serial.
The communication between the processes is only the `result` and the index to get the `i`th example from the examples. i.e.: The example aren't transferred between the processes.

# Runtime

Without this package your code runs serial.
The execution time of the following code snippets will demonstrated, how it runs with this package.
Regarding the color: The `examples = ...` is the setup code.
Therefore it is blue in the code and the block that represents the execution time is also blue.

![(Serial Worker)](doc/tikz_split_managed_serial.svg)

This simples way to paralize the workload (dark orange) is to do an round robin assignment of the load:
`for example in dlp_mpi.split_round_robin(examples)`.
This function call is equivalent to `for example in examples[dlp_mpi.RANK::dlp_mpi.SIZE]`.
So there is zero comunications between the workers.
Only when it is nessesary to do some final work on the results of all data (e.g. calculating average metrics) a comunication is nessesary.
This is done with the `gather` function.
This functions returns the worker results in an list on the master process and the worker process gets a `None` return value.
Depending on the workload the round robin assingment can be suboptimal.
See the example block diagramm.
Worker 1 got tasks that are relative long.
So this worker used much more time than the others.

![(Round Robin)](doc/tikz_split_managed_rr.svg)

To overcome the limitations of the round robin assingment, this package propose to use a manager to assign the work to the workers.
This optimizes the utilisation of the workers.
Once a worker finished an example, he request a new one from the manager and gets one assigned.
Note: The comunication is only which example should be processed (i.e. the index of the example) not the example itself.

![(Managed Split)](doc/tikz_split_managed_split.svg)

An alternative to splitting the iterator is to use a `map` function.
The function is the executed on the workers and the return value is send back to the manager.
Be carefull, that the loop body is fast enough, otherwise it can be a bottleneck.
When a worker sends a task to the manager, the manager sends back a new task and enter the for loop body. 
While the manager is in the loop body, he cannot react on requests of other workers, see the block diagramm:

![(Managed Map)](doc/tikz_split_managed_map.svg)
