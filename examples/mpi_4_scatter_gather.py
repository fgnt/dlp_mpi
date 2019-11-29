"""Call instructions:

# When you do not have MPI:
python mpi_4_scatter_gather.py

# When you have MPI:
mpiexec -np 3 python mpi_4_scatter_gather.py
"""
from itertools import islice
from dlp_mpi import COMM, RANK, SIZE, MASTER, IS_MASTER

if __name__ == '__main__':
    workload = [10, 11, 12, 13]

    result = list()
    for data in islice(workload, RANK, None, SIZE):
        print(f'rank={RANK}, size={SIZE}, data={data!r}')
        result.append(2 * data)

    total = COMM.gather(result, root=MASTER)

    if IS_MASTER:
        print('job splits:', total)
        print('flat result: ', [x for sublist in total for x in sublist])
