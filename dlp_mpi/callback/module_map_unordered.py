import dlp_mpi
from dlp_mpi import MPI, COMM
from dlp_mpi.mpi import RankInt

__all__ = [
    'map_unordered',
]


def map_unordered(
        func,
        sequence,
        *,
        progress_bar=False,
        indexable=True,
        comm=None,
):
    """
    Similar to the builtin function map, but the function is executed on the
    workers and the result is sent to the master process.

    A master process pushes tasks to the workers and receives the result.
    Pushing a task means to send an index to the worker and the worker
    processing the corresponding entry in the sequence.

    By default it is assumed that the sequence is indexable
    (i.e. sequence[3] is allowed). When this is not the case, the argument
    indexable can be set to False and the worker iterates through the
    sequence to get the correct item. In this case it is recommended that
    iterating through the sequence is fast.

    Requires at least 2 mpi processes, but to gain a speedup 3 are needed.
    Only rank 0 gets the results.
    This map is lazy. When the master process blocks in the for loop, the
    master process cannot submit new tasks to the workers.

    Assume function body is fast.

    Parallel: The execution of func.
    """
    from tqdm import tqdm
    from enum import IntEnum, auto

    if comm is None:
        # Clone does here two thinks.
        # - It is a barrier and syncs all processes. This is not necessary
        #   and may slightly worse the startup time.
        # - Create a new communicator that ensures that all communication
        #   (e.g. recv and send) are just inside this function.
        #   This prevents some undesired cross communications between this
        #   function and functions that are called after this function. This
        #   could also be achieved with a barrier at the end of this function.
        #   This style allows to shutdown workers when they are finished and
        #   also do some failure handling after this function.
        comm = COMM.Clone()

    rank = RankInt(comm.rank)
    size = comm.size

    if size == 1:
        if progress_bar:
            try:
                total = len(sequence)
            except TypeError:
                total = None
            yield from tqdm(map(func, sequence), total=total)
            return
        else:
            yield from map(func, sequence)
            return


    status = MPI.Status()
    workers = size - 1

    class tags(IntEnum):
        """Avoids magic constants."""
        start = auto()
        stop = auto()
        default = auto()
        failed = auto()

    # dlp_mpi.barrier()

    if rank == 0:
        i = 0

        failed_indices = []

        with dlp_mpi.util.progress_bar(
                sequence=sequence,
                display_progress_bar=progress_bar,
        ) as pbar:
            pbar.set_description(f'busy: {workers}')
            while workers > 0:
                result = comm.recv(
                    source=MPI.ANY_SOURCE,
                    tag=MPI.ANY_TAG,
                    status=status)
                if status.tag in [tags.default, tags.start]:
                    comm.send(i, dest=status.source)
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
            comm.send(None, dest=0, tag=tags.start)
            next_index = comm.recv(source=0)
            if indexable:
                while True:
                    try:
                        val = sequence[next_index]
                    except IndexError:
                        break
                    result = func(val)
                    comm.send(result, dest=0, tag=tags.default)
                    next_index = comm.recv(source=0)
            else:
                for i, val in enumerate(sequence):
                    if i == next_index:
                        result = func(val)
                        comm.send(result, dest=0, tag=tags.default)
                        next_index = comm.recv(source=0)
        except BaseException:
            comm.send(next_index, dest=0, tag=tags.failed)
            raise
        else:
            comm.send(None, dest=0, tag=tags.stop)
