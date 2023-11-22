"""
Wraps imports for mpi4py to allow code to run on non MPI machines, too.

http://mpi4py.readthedocs.io/en/latest/tutorial.html:
Communication of generic Python objects:
    You have to use all-lowercase ...
Communication of buffer-like objects:
    You have to use method names starting with an upper-case ...

If you want to implement Round-Robin execution, you can try this::
    for example in iterator[RANK::SIZE]:
        pass
"""
import os
import sys

import dlp_mpi
from dlp_mpi.util import ensure_single_thread_numeric, maybe_warn_if_slurm


__all__ = [
    'RANK',
    'SIZE',
    'MASTER',
    'IS_MASTER',
    'barrier',
    'bcast',
    'gather',
    'MPI',
    'COMM',
    'call_on_root_and_broadcast',
]


try:

    from mpi4py import MPI
    if MPI.COMM_WORLD.size > 1:
        if False:
            # Usually data-level parallelism is better than low-level
            # parallelism, i.e. openmp
            # => force numpy (and other libs) to work on a single core
            pass
            # Tensorflow may read the environment variables. And since it is
            # difficult to force TF to a single thread, allow higher ncpus on
            # the HPC sytsem PC2 for Tensorflow. Furthermore, TF has better
            # concepts to use multicores.
        else:
            ensure_single_thread_numeric()
    maybe_warn_if_slurm()
    _mpi_available = True
except ImportError:
    _mpi_available = False

    if 'PC2SYSNAME' in os.environ:
        # No fallback to single core, when the code is executed on our HPC
        # system (PC2).
        # PC2SYSNAME is 'OCULUS' or 'Noctua'
        raise

    if int(os.environ.get('OMPI_COMM_WORLD_SIZE', '1')) != 1:
        print(
            f'WARNING: Something is wrong with your mpi4py installation.\n'
            f'Environment size: {os.environ["OMPI_COMM_WORLD_SIZE"]}\n'
            f'mpi4py size: {os.environ["OMPI_COMM_WORLD_SIZE"]}\n'
        )
        raise

    if int(os.environ.get('PMI_SIZE', '1')) != 1:
        # MPICH_INTERFACE_HOSTNAME=ntws25
        # PMI_RANK=0
        # PMI_FD=6
        # PMI_SIZE=1
        print(
            f'WARNING: Something is wrong with your mpi4py installation.\n'
            f'You use intel mpi while we usually use openmpi.\n'
            f'This is usually caused from "conda install mpi4py" instead of '
            f'"pip install mpi4py".\n'
            f'Try to uninstall mpi4py and install it with "pip install mpi4py"'
        )
        raise

if not _mpi_available:
    # Fallback to a dummy mpi implementation to run on platforms that do not
    # support mpi or computers that do not need mpi, e.g. on a development
    # notebook mpi may not be installed.

    class DUMMY_COMM_WORLD:
        size = 1
        rank = 0
        Barrier = lambda self: None
        bcast = lambda self, data, *args, **kwargs: data
        gather = lambda self, data, *args, **kwargs: [data]
        Clone = lambda self: self

    class _dummy_MPI:
        COMM_WORLD = DUMMY_COMM_WORLD()

    MPI = _dummy_MPI()


class RankInt(int):
    def __bool__(self):
        raise NotImplementedError(
            'Bool is disabled for rank. '
            'It is likely that you want to use IS_MASTER.'
        )


COMM = MPI.COMM_WORLD
RANK = RankInt(COMM.rank)
SIZE = COMM.size

# MASTER is deprecated, and all usages in dlp_mpi should be replaced by ROOT.
# Here, we plan to keep it for external code.
MASTER = RankInt(0)
IS_MASTER = (RANK == MASTER)

ROOT = RankInt(0)
IS_ROOT = (RANK == ROOT)


def barrier():
    """
    Blocks all processes until all processes reach this barrier.
    So this is a sync point.
    """
    COMM.Barrier()


def bcast(obj, root: int = ROOT):
    """
    Pickles the obj and send it from the root to all processes (i.e. broadcast)
    """
    try:
        return COMM.bcast(obj, root)
    except OverflowError:
        raise ValueError(
            'A typical cause for '
            '''"OverflowError: integer XXXXXXXXXX does not fit in 'int'"\n'''
            'is a too large object.\n'
            'See '
            'https://bitbucket.org/mpi4py/mpi4py/issues/57/overflowerror-integer-2768896564-does-not'
        )


def gather(obj, root: int = ROOT):
    """
    Pickles the obj on each process and send them to the root process.
    Returns a list on the master process that contains all objects.
    """
    return COMM.gather(obj, root=root)


def call_on_root_and_broadcast(func, *args, **kwargs):
    if IS_MASTER:
        result = func(*args, **kwargs)
    else:
        result = None
    return bcast(result)
