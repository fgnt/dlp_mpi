import dlp_mpi
from dlp_mpi import COMM, SIZE


def test_relaunch_with_mpi(mpi_size=3):
    assert dlp_mpi.mpi._mpi_available, dlp_mpi.mpi._mpi_available
    if dlp_mpi.SIZE == 1:
        assert mpi_size == 3, mpi_size

        import subprocess
        import sys

        cmd = ['mpiexec', '-np', '3', sys.executable, sys.argv[0]]

        print(' '.join(['mpiexec', '-np', '3', sys.executable, sys.argv[0]]))
        p = subprocess.run(cmd)

        sys.exit(p.returncode)
