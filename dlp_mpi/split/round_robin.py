from itertools import islice

import dlp_mpi
from dlp_mpi import RANK, SIZE

__all__ = [
    'split_round_robin',
]


def split_round_robin(
        sequence,
        progress_bar=False,
        indexable=True,
):
    """
    Splits the work between all processes in round robin fashion.
    Requires zero communication between the processes.

    The progress_bar displays an approximation of the progress:
        Display the progress of the master process and assume the workers have
        the same execution time.

    >> assert dlp_mpi.SIZE == 2  # mpi size is 2
    >> if rank==0:
    ..     print(list(split_round_robin(range(5))))
    [0, 2, 4]
    >> if rank==1:
    ..     print(list(split_round_robin(range(5))))
    [1, 3]

    """

    if dlp_mpi.IS_MASTER and progress_bar:
        from tqdm import tqdm

        if indexable:
            def gen():
                with tqdm(total=len(sequence), desc='MasterPbar') as pbar:
                    for ele in sequence[RANK:SIZE]:
                        yield ele
                        pbar.update(SIZE)
        else:
            def gen():
                with tqdm(total=len(sequence), desc='MasterPbar') as pbar:
                    for ele in islice(sequence, RANK, None, SIZE):
                        yield ele
                        pbar.update(SIZE)
        return gen()

    else:
        if indexable:
            return sequence[RANK::SIZE]
        else:
            return islice(sequence, RANK, None, SIZE)
