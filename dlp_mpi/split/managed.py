from enum import IntEnum, auto

import dlp_mpi
from dlp_mpi import MPI, COMM
from dlp_mpi import RANK, SIZE


__all__ = [
    'split_managed'
]

class _tags(IntEnum):

    # The first time the worker request a task, this tag is send:
    #    need new data
    start = auto()

    # The last time the worker request a task, this tag is send:
    #    finished successfully the work.
    stop = auto()

    # The default tag
    #    processed the data and need new data
    default = auto()

    # Something went wrong and got an exception. This worker is now broken.
    # The master process will fail when all processes finished (i.e. stop of failed)
    failed = auto()

    # Special when the split is used as map.
    # In the moment disabled.
    data = auto()


def split_managed(
        sequence,
        progress_bar=True,
        allow_single_worker=False,
        pbar_prefix=None,
        root=dlp_mpi.MASTER,
        # gather_mode=False,
        is_indexable=False,
):
    """
    A master process pushes tasks to the workers.
    Required at least 2 mpi processes, but to produce a speedup 3 are required.

    Parallel: Body of the for loop.
    Communication: Indices
    Redundant computation: Each process (except the master) consumes the
                           iterator.

    Note:
        Inside the for loop a break is allowed to mark the current task as
        successful and do not calculate further tasks.


    iter_mode:
        True: Iterate over the iterator on each slave and yield examples with
              the correct index. Similar to itertools.islice(RANK, None, SIZE)
              but uses an assignment from a master process instead of round
              robin selection.
        False: Use getitem instead of iterationg over the iterator.


    ToDo:
        - Make a more efficient scheduling
          - allow indexable
          - Use round robin in the beginning and for the last task this
            scheduler. Or submit chunks of work.
        - When a slave throw a exception, the task is currently ignored.
          Change it that the execution get canceled.

    """
    if allow_single_worker and SIZE == 1:
        if not progress_bar:
            yield from sequence
        else:
            from tqdm import tqdm
            yield from tqdm(sequence, mininterval=2)
        return

    assert SIZE > 1, SIZE
    assert root < SIZE, (root, SIZE)
    assert root == 0, root

    status = MPI.Status()
    workers = SIZE - 1

    # ToDo: Ignore workers that failed before this function is called.
    # registered_workers = set()

    # dlp_mpi.barrier()

    failed_indices = []

    if RANK == root:
        i = 0

        if pbar_prefix is None:
            pbar_prefix = ''
        else:
            pbar_prefix = f'{pbar_prefix}, '

        with dlp_mpi.util.progress_bar(
                sequence=sequence,
                display_progress_bar=progress_bar,
        ) as pbar:
            pbar.set_description(f'{pbar_prefix}busy: {workers}')
            while workers > 0:
                last_index = COMM.recv(
                    source=MPI.ANY_SOURCE,
                    tag=MPI.ANY_TAG,
                    status=status,
                )

                if status.tag in [_tags.default, _tags.start]:
                    COMM.send(i, dest=status.source)
                    i += 1

                if status.tag in [_tags.default, _tags.failed]:
                    pbar.update()

                if status.tag in [_tags.stop, _tags.failed]:
                    workers -= 1
                    if progress_bar:
                        pbar.set_description(f'{pbar_prefix}busy: {workers}')

                if status.tag == _tags.failed:
                    failed_indices += [(status.source, last_index)]

        assert workers == 0, workers

        try:
            length = len(sequence)
        except TypeError:
            length = None

        # i is bigger than len(iterator), because the slave says value is to big
        # and than the master increases the value
        if length is not None:
            if (not length < i) or len(failed_indices) > 0:
                failed_indices = '\n'.join([
                    f'worker {rank_} failed for index {index}'
                    for rank_, index in failed_indices
                ])
                raise AssertionError(
                    f'{length}, {i}: Iterator is not consumed.\n'
                    f'{failed_indices}'
                )
    else:
        next_index = -1
        successfull = False
        try:
            COMM.send(None, dest=root, tag=_tags.start)
            next_index = COMM.recv(source=root)

            if not is_indexable:
                for i, val in enumerate(sequence):
                    if i == next_index:
                        assert val is not None, val
                        data = yield val
                        assert data is None, data
                        COMM.send(next_index, dest=root, tag=_tags.default)
                        next_index = COMM.recv(source=root)
            else:
                length = len(sequence)
                assert length is not None, length

                while next_index < length:
                    val = sequence[next_index]
                    assert val is not None, val
                    data = yield val
                    assert data is None, data
                    COMM.send(next_index, dest=root, tag=_tags.default)
                    next_index = COMM.recv(source=root)

            successfull = True
        finally:
            if successfull:
                COMM.send(next_index, dest=root, tag=_tags.stop)
            else:
                COMM.send(next_index, dest=root, tag=_tags.failed)

'''
def split_managed_(
        iterator,
        # length=None,
        disable_pbar=False,
        allow_single_worker=False,
        pbar_prefix=None,
        root=0,
        gather_mode=False,
        is_indexable=False,
):
    """
    A master process pushes tasks to the workers.
    Required at least 2 mpi processes, but to produce a speedup 3 are required.

    Parallel: Body of the for loop.
    Communication: Indices
    Redundant computation: Each process (except the master) consumes the
                           iterator.

    Note:
        Inside the for loop a break is allowed to mark the current task as
        successful and do not calculate further tasks.


    iter_mode:
        True: Iterate over the iterator on each slave and yield examples with
              the correct index. Similar to itertools.islice(RANK, None, SIZE)
              but uses an assignment from a master process instead of round
              robin selection.
        False: Use getitem instead of iterationg over the iterator.


    ToDo:
        - Make a more efficient scheduling
          - allow indexable
          - Use round robin in the beginning and for the last task this
            scheduler. Or submit chunks of work.
        - When a slave throw a exception, the task is currently ignored.
          Change it that the execution get canceled.

    """
    from tqdm import tqdm

    if allow_single_worker and SIZE == 1:
        if disable_pbar:
            yield from iterator
        else:
            yield from tqdm(iterator, mininterval=2)
        return

    assert SIZE > 1, SIZE
    assert root < SIZE, (root, SIZE)
    assert root == 0, root

    status = MPI.Status()
    workers = SIZE - 1
    # registered_workers = set()

    # print(f'{RANK} reached Barrier in share_master')
    dlp_mpi.barrier()
    # print(f'{RANK} left Barrier in share_master')

    failed_indices = []

    if RANK == root:
        i = 0
        try:
            length = len(iterator)
        except Exception:
            length = None

        if pbar_prefix is None:
            pbar_prefix = ''
        else:
            pbar_prefix = f'{pbar_prefix}, '

        with tqdm(
                total=length, disable=disable_pbar, mininterval=2, smoothing=None
        ) as pbar:
            pbar.set_description(f'{pbar_prefix}busy: {workers}')
            while workers > 0:
                last_index = COMM.recv(
                    source=MPI.ANY_SOURCE,
                    tag=MPI.ANY_TAG,
                    status=status,
                )

                if status.tag in [_tags.default, _tags.data, _tags.start]:
                    COMM.send(i, dest=status.source)
                    i += 1

                if status.tag in [_tags.default, _tags.data, _tags.failed]:
                    pbar.update()

                if status.tag in [_tags.stop, _tags.failed]:
                    workers -= 1
                    if not disable_pbar:
                        pbar.set_description(f'{pbar_prefix}busy: {workers}')

                if status.tag == _tags.failed:
                    failed_indices += [(status.source, last_index)]

                if status.tag == _tags.data:
                    yield last_index

        assert workers == 0, workers
        # i is bigger than len(iterator), because the slave says value is to big
        # and than the master increases the value
        if length is not None:
            if (not length < i) or len(failed_indices) > 0:
                failed_indices = '\n'.join([
                    f'worker {rank_} failed for index {index}'
                    for rank_, index in failed_indices
                ])
                raise AssertionError(
                    f'{length}, {i}: Iterator is not consumed.\n'
                    f'{failed_indices}'
                )
            # assert length < i, f'{length}, {i}: Iterator is not consumed'
    else:
        next_index = -1
        successfull = False
        processing_count = 0
        try:
            COMM.send(None, dest=root, tag=_tags.start)
            next_index = COMM.recv(source=root)
            # print(f'RANK {RANK} first next_index {next_index}', flush=True)

            if not is_indexable:
                for i, val in enumerate(iterator):
                    # print(f'RANK {RANK} search {next_index} now {i}', flush=True)
                    if i == next_index:
                        assert val is not None, val
                        data = yield val
                        if gather_mode:
                            if data is None:
                                processing_count += 1
                                COMM.send(data, dest=root, tag=_tags.start)
                            else:
                                COMM.send(data, dest=root, tag=_tags.data)
                        else:
                            assert data is None, data
                            COMM.send(next_index, dest=root, tag=_tags.default)
                        next_index = COMM.recv(source=root)
            else:
                length = len(iterator)
                assert length is not None, length
                assert not gather_mode, gather_mode
                while next_index < length:
                    val = iterator[next_index]
                    assert val is not None, val
                    data = yield val
                    assert data is None, data
                    COMM.send(next_index, dest=root, tag=_tags.default)
                    next_index = COMM.recv(source=root)

            # print('gather_mode', RANK, gather_mode, next_index, flush=True)
            if gather_mode:
                processing_count -= 1

                while processing_count > 0:
                    # print(RANK, 'processing_count', processing_count, flush=True)
                    data = yield None
                    assert data is not None, data
                    processing_count -= 1
                    COMM.send(data, dest=root, tag=_tags.data)
                    next_index = COMM.recv(source=root)
            # print('end gather_mode', RANK, gather_mode, flush=True)

            successfull = True
        # except BaseException as e:
        #     print('*'*300, '\n', type(e), e, flush=True)
        #     raise
        finally:
            # print('shutdown', RANK, successfull, flush=True)
            if successfull:
                COMM.send(next_index, dest=root, tag=_tags.stop)
            else:
                COMM.send(next_index, dest=root, tag=_tags.failed)
'''