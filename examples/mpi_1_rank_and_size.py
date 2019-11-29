"""Call instructions:

# When you do not have MPI:
python mpi_1_rank_and_size.py

# When you have MPI:
mpiexec -np 3 python mpi_1_rank_and_size.py
"""

from dlp_mpi import COMM, RANK, SIZE

if __name__ == '__main__':
    print(f'rank={RANK}, size={SIZE}')
