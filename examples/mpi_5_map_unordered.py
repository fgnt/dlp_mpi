"""
Run on single CPU:
python paderbox/examples/mpi_5_map_unordered.py

Run on multiple CPUs:
mpiexec -np 5 python paderbox/examples/mpi_5_map_unordered.py
"""

from paderbox.utils.mpi import COMM, RANK, SIZE, MASTER, IS_MASTER, map_unordered
import time
import numpy as np


def fn(example_id):
    time.sleep(np.random.uniform(0, 1))
    example = 'hello'
    print(RANK, example_id, example[example_id])
    return example[example_id]


if __name__ == '__main__':
    if IS_MASTER:
        print('### Unordered map scattered around processes:')

    result = list(map_unordered(fn, range(5)))

    if IS_MASTER:
        print(result)

    if IS_MASTER:
        print('### Map function run only on master:')
        print(list(map(fn, range(5))))
