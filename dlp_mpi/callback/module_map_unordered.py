import dlp_mpi
from dlp_mpi import MPI, COMM

__all__ = [
    'map_unordered',
]


def map_unordered(
        func,
        sequence,
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

    dlp_mpi.barrier()

    if dlp_mpi.RANK == 0:
        i = 0

        try:
            total = len(sequence)
        except TypeError:
            total = None

        with tqdm(total=total, disable=not progress_bar) as pbar:
            pbar.set_description(f'busy: {workers}')
            while workers > 0:
                result = COMM.recv(
                    source=MPI.ANY_SOURCE,
                    tag=MPI.ANY_TAG,
                    status=status)
                if status.tag == tags.default:
                    COMM.send(i, dest=status.source)
                    yield result
                    i += 1
                    pbar.update()
                elif status.tag == tags.start:
                    COMM.send(i, dest=status.source)
                    i += 1
                    pbar.update()
                elif status.tag == tags.stop:
                    workers -= 1
                    pbar.set_description(f'busy: {workers}')
                else:
                    raise ValueError(status.tag)

        assert workers == 0
    else:
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
        finally:
            COMM.send(None, dest=0, tag=tags.stop)
