"""Call instructions:

# When you do not have MPI:
python mpi.py

# When you have MPI:
mpiexec -np 3 python mpi.py
""""

from nt.utils.mpi import COMM, RANK, SIZE

if __name__ == '__main__':
    print(f'rank={RANK}, size={SIZE}')
