"""Call instructions:

# When you do not have MPI:
python mpi.py

# When you have MPI:
mpiexec -np 3 python mpi.py
"""

from nt.utils.mpi import COMM, RANK, SIZE, MASTER, IS_MASTER

if __name__ == '__main__':
    if RANK == 0:
        data = 'hello'
    elif RANK == 1:
        data = 'world'
    else:
        data = '!'

    data = COMM.gather(data, root=MASTER)

    print(f'rank={RANK}, size={SIZE}, data={data!r}')
