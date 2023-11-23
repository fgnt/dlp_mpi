import os
import logging
import contextlib


LOG = logging.getLogger('dlp_mpi')


@contextlib.contextmanager
def progress_bar(
        sequence,
        display_progress_bar,
):
    try:
        length = len(sequence)
    except TypeError:
        length = None

    if display_progress_bar:
        try:
            from tqdm import tqdm
        except ImportError:
            LOG.warning('Can not import tqdm. Disable the progress bar.')
        else:
            # Smoothing has problems with a huge amount of workers (e.g. 200)
            with tqdm(
                    total=length,
                    # disable=not display_progress_bar,
                    mininterval=2,
                    smoothing=None,
            ) as pbar:
                yield pbar
    else:
        class DummyPBar:
            def set_description(self, *args, **kwargs):
                pass

            def update(self, *args, **kwargs):
                pass

        yield DummyPBar()


def ensure_single_thread_numeric():
    """
    When you parallelize your input pipeline you often want each worker to work
    on a single thread.

    These variables are all candidates to be set to 1, but the ones checked in
    this function are mandatory as far as we know.

    GOMP_NUM_THREADS
    OMP_NUM_THREADS
    OPENBLAS_NUM_THREADS
    MKL_NUM_THREADS
    VECLIB_MAXIMUM_THREADS
    NUMEXPR_NUM_THREADS
    """
    candidates = [
        'OMP_NUM_THREADS',
        'MKL_NUM_THREADS',
    ]

    for key in candidates:
        if not os.environ.get(key) == '1':
            raise EnvironmentError(
                'Make sure to set the following environment variables to '
                'ensure that each worker works on a single thread:\n'
                'export OMP_NUM_THREADS=1\n'
                'export MKL_NUM_THREADS=1\n\n'
                f'But you use: {key}={os.environ.get(key)}'
            )


def maybe_warn_if_slurm():
    SLURM_NTASKS = int(os.environ.get('SLURM_NTASKS', 1))
    if SLURM_NTASKS > 1:
        from mpi4py import MPI
        SIZE = MPI.COMM_WORLD.size
        if SIZE == 1:
            import warnings
            warnings.warn(
                f'dlp_mpi: Your SLURM job can use up to {SLURM_NTASKS} '
                f'tasks/MPI processes,\n'
                f'but MPI SIZE is only 1. Maybe you forgot srun?'
            )
