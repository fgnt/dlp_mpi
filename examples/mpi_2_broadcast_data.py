"""Call instructions:

# When you do not have MPI:
python mpi.py

# When you have MPI:
mpiexec -np 3 python mpi.py
"""

from dlp_mpi import COMM, RANK, SIZE, MASTER, IS_MASTER

if __name__ == '__main__':
    if IS_MASTER:
        data = {
            'key1': [7, 2.72, 2+3j],
            'key2': ('abc', 'xyz')
        }
    else:
        data = None

    data = COMM.bcast(data, root=MASTER)

    print(f'rank={RANK}, size={SIZE}, data={data!r}')

