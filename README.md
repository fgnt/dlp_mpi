# dlp_mpi - Data-level parallelism with mpi for python

[![PyPI](https://img.shields.io/pypi/v/dlp_mpi.svg)](https://pypi.org/project/dlp-mpi)
[![PyPI](https://img.shields.io/pypi/dm/dlp_mpi)](https://pypi.org/project/dlp-mpi)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/fgnt/dlp_mpi/master/LICENSE)

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
    return result

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

This package uses `mpi4py` to provide utilities to parallelize algorithms that are applied to multiple examples.

The core idea is: Start `N` processes and each process works on a subset of all examples.
To start the processes `mpiexec` can be used. Most HPC systems support MPI to scatter the workload across multiple hosts. For the command, look in the documentation for your HPC system and search for MPI launches.

Since each process should operate on different examples, MPI provides the variables `RANK` and `SIZE`, where `SIZE` is the number of workers and `RANK` is a unique identifier from `0` to `SIZE - 1`.
The easiest way to improve the execution time is to process `examples[RANK::SIZE]` on each worker.
This is a round-robin load balancing (`dlp_mpi.split_round_robin`).
A more advanced load balancing is `dlp_mpi.split_managed`, where one process manages the load and assigns a new task to a worker once he finishes the last task.

When in the end of a program all results should be summarized or written in a single file, communication between all processes is nessesary.
For this purpose `dlp_mpi.gather` (`mpi4py.MPI.COMM_WORLD.gather`) can be used. This function sends all data to the root process (Here, `pickle` is used for serialization).

As an alternative to splitting the data, this package also provides a `map` style parallelization (see example in the beginning):
The function `dlp_mpi.map_unordered` calls `work_load` in parallel and executes the `for` body in serial.
The communication between the processes is only the `result` and the index to get the `i`th example from the examples, i.e., the example aren't transferred between the processes.

# Availabel utilities and functions

Note: `dlp_mpi` has dummy implementations, when `mpi4py` is not installed and the environment indicates that no MPI is used (Useful for running on a laptop).

 - `dlp_mpi.RANK` or `mpi4py.MPI.COMM_WORLD.rank`: The rank of the process. To avoid programming errors, `if dlp_mpi.RANK: ...` will fail.
 - `dlp_mpi.SIZE` or `mpi4py.MPI.COMM_WORLD.size`: The number of processes.
 - `dlp_mpi.IS_MASTER`: A flag that indicates whether the process is the default master/controller/root.
 - `dlp_mpi.bcast(...)` or `mpi4py.MPI.COMM_WORLD.bcast(...)`: Broadcast the data from the root to all workers.
 - `dlp_mpi.gather(...)` or `mpi4py.MPI.COMM_WORLD.gather(...)`: Send data from all workers to the root.
 - `dlp_mpi.barrier()` or `mpi4py.MPI.COMM_WORLD.Barrier()`: Sync all prosesses.

The advanced functions that are provided in this package are

 - `split_round_robin(examples)`: Zero communication split of the data. The default is identical to `examples[dlp_mpi.RANK::dlp_mpi.SIZE]`.
 - `split_managed(examples)`: The master process manages the load balance while the others do the work. Note: The master process does not distribute the examples. It is assumed that examples have the same order on each worker.
 - `map_unordered(work_load, examples)`: The master process manages the load balance, while the others execute the `work_load` function. The result is sent back to the master process.


# Runtime

Without this package, your code runs in serial.
The execution time of the following code snippets will be demonstrated by running it with this package.
Regarding the color: The `examples = ...` is the setup code.
Therefore, the code and the corresponding block representing the execution time it is blue in the code.

![(Serial Worker)](doc/tikz_split_managed_serial.svg)

This easiest way to parallelize the workload (dark orange) is to do a round-robin assignment of the load:
`for example in dlp_mpi.split_round_robin(examples)`.
This function call is equivalent to `for example in examples[dlp_mpi.RANK::dlp_mpi.SIZE]`.
Thus, there is zero comunications between the workers.
Only when it is nessesary to do some final work on the results of all data (e.g. calculating average metrics) a communication is necessary.
This is done with the `gather` function.
This function returns the worker results in a list on the master process and the worker process gets a `None` return value.
Depending on the workload the round-robin assingment can be suboptimal.
See the example block diagram.
Worker 1 got tasks that are relatively long.
So this worker used much more time than the others.

![(Round Robin)](doc/tikz_split_managed_rr.svg)

To overcome the limitations of the round robin assignment, this package helps to use a manager to assign the work to the workers.
This optimizes the utilization of the workers.
Once a worker finished an example, it requests a new one from the manager and gets one assigned.
Note: The communication is only which example should be processed (i.e. the index of the example) not the example itself.

![(Managed Split)](doc/tikz_split_managed_split.svg)

An alternative to splitting the iterator is to use a `map` function.
The function is then executed on a worker and the return value is sent back to the manager.
Be carefull, that the loop body is fast enough, otherwise it can be a bottleneck.
You should use the loop body only for book keeping, not for actual workload.
When a worker sends a task to the manager, the manager sends back a new task and enters the for loop body. 
While the manager is in the loop body, he cannot react on requests of other workers, see the block diagram:

![(Managed Map)](doc/tikz_split_managed_map.svg)


# Installation

You can install this package from pypi:
```bash
pip install mpi4py
pip install dlp_mpi
```
where `mpi4py` is a backend for this package.
You can skip the installation of `mpi4py` when you
want to use the internal backend, it is called `ame`
and is part of this package (`dlp_mpi.ame`).

To check if the installation was successful, try the following command:
```bash 
$ ameexec -np 4 python -m dlp_mpi  # mpiexec -np 4 python -m dlp_mpi
MPI backend: dlp_mpi.ame.MPI
dlp_mpi.ame init info from the root process:
  Using AME for getting host, port, rank, size:
    Host: sa, Port: 51297
    Rank: 0, Size: 4
  Available methods:
    AME: dlp_mpi.ame.core._init.get_init.get_ame_host_rank_size
    None: dlp_mpi.ame.core._init.get_init.get_fallback_host_rank_size
Hello from rank 3 of 4!
Hello from rank 1 of 4!
Hello from rank 2 of 4!
Hello from rank 0 of 4!
```
The command should print four times `Hello from rank X of 4!` where X is 0, 1, 2 and 3.
The order is random.
When it prints 4 times `Hello from rank 0 of 1!`, something went wrong.
When that line prints 4 times a zero, something went wrong.
You can try different launchers
 - `ameexec -np 4 python -m dlp_mpi`  # Simple launcher, that supports a subset of mpiexec
 - `mpiexec -np 4 python -m dlp_mpi`  # Supports multi-node execution
 - `srun -N 1 -n 1 -c 10 -p cpu --gpus 1 srun python -m dlp_mpi`  # recommended in HPC systems. Probably you have to adapt the arguments to the SLURM installation.
and you can switch between backends, via environment variables
 - `export DLP_MPI_BACKEND=ame`
 - `export DLP_MPI_BACKEND=mpi4py`

If you installed mpi4py, it sometimes happens, that the used mpi doesn't
match the compiletime mpi version of mpi4py, e.g., it was missing.
In a Debian-based Linux you can install it with `sudo apt install libopenmpi-dev`.
When you do not have the rights to install something with `apt`, you could also install `mpi4py` with `conda`.
The above `pip install` will install `mpi4py` from `pypi`.
Be careful, that the installation from `conda` may conflict with your locally installed `mpi`. 
Especially in High Performance Computing (HPC) environments this can cause troubles.

What should be used?
 - CB: I stopped using mpi4py and use ame either with mpiexec (or ameexec, if mpiexec is not installed) or slurm. For me it just works.

# AME Backend

The `ame` backend can be activated by setting the environment variable `DLP_MPI_BACKEND` to `ame`:

```bash
export DLP_MPI_BACKEND=ame
```

It implements the interface of `mpi4py` that is used by `dlp_mpi`.

It has the following properties:

 - Pure python implementation with sockets:
   - No issues with binaries: The actual motivation for `ame`
   - Most likely slower than `mpi4py`: `MPI` has many optimizations that are not implemented in `ame`
 - Communication only between root and workers, i.e., no communication between workers. So you cannot change the root in any function of `dlp_mpi`. But it is also unlikely that you need this feature. At least, I never needed it.
 - Assumes a trusted environment: The communication is not encrypted. So do not use it in an untrusted environment (Maybe the same as in mpi?).
 - Supported launchers (mpiexec and srun):
   - mpiexec build with PMI (uses PMI to setup the environment)
   - mpiexec build with PMIx (use file based setup)
   - slurm/srun with PMIx (use file based setup)

# FAQ

**Q**: Can I run a script that uses `dlp_mpi` on my laptop, that has no running MPI (i.e. broken installation)?

**A**: Yes, when you uninstall `mpi4py` (i.e. `pip uninstall mpi4py`) after installing this package. When `MPI` is working or missing, code written with `dlp_mpi` should work.
