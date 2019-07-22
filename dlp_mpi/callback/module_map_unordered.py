import dlp_mpi
from dlp_mpi import MPI, COMM

__all__ = [
    'map_unordered',
]


def map_unordered(
        func,
        sequence,
        *,
        progress_bar=False,
        indexable=True,

):
    """
    Similar to the buildin function map, but the function is executed on the
    workers and the result is send to the master process.

    A master process push tasks to the workers and receives the result.
    Pushing tasks mean, send an index to the worker and the worker processes
    the corresponding entry in the sequence.

    By default it is assumed that the sequence is indexable
    (i.e. sequence[3] is allowed). When this is not the case, the argument
    indexable can be set to False and the worker iterates through the
    sequence to get the correct item. In this case it is recomented, that
    iterating through the sequence is fast.

    Required at least 2 mpi processes, but to produce a speedup 3 are required.
    Only rank 0 get the results.
    This map is lazy. When the master process blocks in the for loop, the
    master process can not submit new tasks to the workers.

    Assume function body is fast.

    Parallel: The execution of func.
    """
    from tqdm import tqdm
    from enum import IntEnum, auto

    if dlp_mpi.SIZE == 1:
        if progress_bar:
            yield from tqdm(map(func, sequence))
            return
        else:
            yield from map(func, sequence)
            return

    status = MPI.Status()
    workers = dlp_mpi.SIZE - 1

    class tags(IntEnum):
        """Avoids magic constants."""
        start = auto()
        stop = auto()
        default = auto()
        failed = auto()

    # dlp_mpi.barrier()

    if dlp_mpi.RANK == 0:
        i = 0

        failed_indices = []

        with dlp_mpi.util.progress_bar(
                sequence=sequence,
                display_progress_bar=progress_bar,
        ) as pbar:
            pbar.set_description(f'busy: {workers}')
            while workers > 0:
                result = COMM.recv(
                    source=MPI.ANY_SOURCE,
                    tag=MPI.ANY_TAG,
                    status=status)
                if status.tag in [tags.default, tags.start]:
                    COMM.send(i, dest=status.source)
                    i += 1
                if status.tag in [tags.default]:
                    yield result
                if status.tag in [tags.default, tags.failed]:
                    pbar.update()

                if status.tag in [tags.stop, tags.failed]:
                    workers -= 1
                    if progress_bar:
                        pbar.set_description(f'busy: {workers}')
                if status.tag in [tags.failed]:
                    last_index = result
                    failed_indices += [(status.source, last_index)]

        try:
            total = len(sequence)
        except TypeError:
            total = None

        # Move this to a separate function and check that all cases are correct
        if total is not None or len(failed_indices) > 0:
            if len(failed_indices) > 0 or (not total < i):
                failed_indices = '\n'.join([
                    f'worker {rank_} failed for index {index}'
                    for rank_, index in failed_indices
                ])
                raise AssertionError(
                    f'{total}, {i}: Iterator is not consumed.\n'
                    f'{failed_indices}'
                )

        assert workers == 0, workers
    else:
        next_index = -1
        try:
            COMM.send(None, dest=0, tag=tags.start)
            next_index = COMM.recv(source=0)
            if indexable:
                while True:
                    try:
                        val = sequence[next_index]
                    except IndexError:
                        break
                    result = func(val)
                    COMM.send(result, dest=0, tag=tags.default)
                    next_index = COMM.recv(source=0)
            else:
                for i, val in enumerate(sequence):
                    if i == next_index:
                        result = func(val)
                        COMM.send(result, dest=0, tag=tags.default)
                        next_index = COMM.recv(source=0)
        except BaseException:
            COMM.send(next_index, dest=0, tag=tags.failed)
            raise
        else:
            COMM.send(None, dest=0, tag=tags.stop)
