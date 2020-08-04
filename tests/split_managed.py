import time
import dlp_mpi
from dlp_mpi import RANK, SIZE


def executable():
    print(f'executable test {RANK}')

    examples = list(range(5))

    ranks = dlp_mpi.gather(dlp_mpi.RANK)
    if dlp_mpi.IS_MASTER:
        assert ranks == [0, 1, 2], ranks

    for i in dlp_mpi.split_managed(examples, progress_bar=False):
        assert dlp_mpi.RANK in [1, 2], (dlp_mpi.RANK, dlp_mpi.SIZE)


def speedup():
    print(f'speedup test {RANK}')
    examples = list(range(4))

    ranks = dlp_mpi.gather(dlp_mpi.RANK)
    if dlp_mpi.IS_MASTER:
        assert ranks == [0, 1, 2], ranks

    sleep_time = 0.1

    start = time.perf_counter()
    for i in dlp_mpi.split_managed(examples, progress_bar=False):
        time.sleep(sleep_time)
        # print(f'Callback from {RANK}')
        assert dlp_mpi.RANK in [1, 2], (dlp_mpi.RANK, dlp_mpi.SIZE)
    elapsed = time.perf_counter() - start

    serial_time = sleep_time * len(examples)

    # Two workers, one manager (3 mpi processes) reduce the time by 0.5
    # Consider some python overhead
    assert elapsed < 0.6 * serial_time, (elapsed, serial_time)
    assert elapsed >= 0.5 * serial_time, (elapsed, serial_time)


def cross_communication():
    print(f'cross_communication test {RANK}')

    examples = list(range(5))

    ranks = dlp_mpi.gather(dlp_mpi.RANK)
    if dlp_mpi.IS_MASTER:
        assert ranks == [0, 1, 2], ranks

    dlp_mpi.barrier()
    if RANK == 1:
        time.sleep(0.1)
    elif RANK == 2:
        pass

    results = []
    for i in dlp_mpi.split_managed(examples, progress_bar=False):
        assert dlp_mpi.RANK in [1, 2], (dlp_mpi.RANK, dlp_mpi.SIZE)

        if RANK == 1:
            results.append(i)
        elif RANK == 2:
            results.append(i)
            time.sleep(0.2)

    if RANK == 1:
        assert results in [[0, 2, 3, 4], [1, 2, 3, 4]], results
    elif RANK == 2:
        assert results in [[1], [0]], results

    for i in dlp_mpi.split_managed(examples, progress_bar=False):
        assert dlp_mpi.RANK in [1, 2], (dlp_mpi.RANK, dlp_mpi.SIZE)
        if RANK == 1:
            results.append(i)
            time.sleep(0.001)
        elif RANK == 2:
            results.append(i)
            time.sleep(0.2)

    if RANK == 1:
        assert results in [
            [0, 2, 3, 4, 0, 2, 3, 4],
            [0, 2, 3, 4, 1, 2, 3, 4],
            [1, 2, 3, 4, 0, 2, 3, 4],
            [1, 2, 3, 4, 1, 2, 3, 4],
        ], results
    elif RANK == 2:
        assert results in [
            [1, 1],
            [1, 0],
            [0, 1],
            [0, 0],
        ], results


def worker_fails():
    print(f'worker_fails test {RANK}')

    examples = list(range(5))

    ranks = dlp_mpi.gather(dlp_mpi.RANK)
    if dlp_mpi.IS_MASTER:
        assert ranks == [0, 1, 2], ranks

    processed = []
    try:
        dlp_mpi.barrier()
        if RANK == 2:
            # Delay rank 2, this ensures that rank 1 gets the first example
            # Does no longer work, becasue in split_managed is COMM.Clone
            # used.
            time.sleep(0.1)
        for i in dlp_mpi.split_managed(examples, progress_bar=False):
            processed.append(i)
            if RANK == 1:
                print(f'let {RANK} fail for data {i}')
                raise ValueError('failed')
            assert dlp_mpi.RANK in [1, 2], (dlp_mpi.RANK, dlp_mpi.SIZE)
    except ValueError:
        assert RANK in [1], RANK
        assert processed in [[0], [1]], processed
    except AssertionError:
        assert RANK in [0], RANK
        assert processed == [], processed
    else:
        assert RANK in [2], RANK
        assert processed == [1, 2, 3, 4], processed


def pbar():
    print(f'executable test {RANK}')

    examples = list(range(5))

    ranks = dlp_mpi.gather(dlp_mpi.RANK)
    if dlp_mpi.IS_MASTER:
        assert ranks == [0, 1, 2], ranks

    class MockPbar:
        call_history = []

        def __init__(self):
            self.i = 0

        def set_description(self, text):
            self.call_history.append(text)

        def update(self, inc=1):
            self.i += 1
            self.call_history.append(f'update {self.i}')


    import contextlib
    @contextlib.contextmanager
    def mock_pbar(total, mininterval, smoothing):
        yield MockPbar()

    import mock

    with mock.patch('tqdm.tqdm', mock_pbar):

        dlp_mpi.barrier()
        if RANK == 2:
            time.sleep(0.02)

        for i in dlp_mpi.split_managed(examples):
            time.sleep(0.04)
            assert dlp_mpi.RANK in [1, 2], (dlp_mpi.RANK, dlp_mpi.SIZE)

    if RANK == 0:
        assert MockPbar.call_history == [
            'busy: 2', 'update 1', 'update 2', 'update 3', 'update 4',
            'busy: 1', 'update 5', 'busy: 0'], MockPbar.call_history
    else:
        assert MockPbar.call_history == [], MockPbar.call_history


def overhead():
    """

    This test shows the overhead of map_unordered.
    A simple for loop with map cam process around 1 000 000 examples per
    second. When using map_unordered, obviously the number should decrease.

    When your code processes less than 1000 examples per second, you can expect
    a gain from map_unordered. When you process serial more than 10000 examples
    per second, it is unlikely to get a gain from map_unordered.
    Thing about chunking to get less than 100 example per second.
    """
    print(f'executable test {RANK}')

    examples = list(range(10000))

    ranks = dlp_mpi.gather(dlp_mpi.RANK)
    if dlp_mpi.IS_MASTER:
        assert ranks == [0, 1, 2], ranks

    total = 0
    # def bar(i):
    #     nonlocal total
    #     total += i
    #     return i

    start = time.perf_counter()
    for i in dlp_mpi.split_managed(examples, progress_bar=False):
        total += i
    elapsed = time.perf_counter() - start

    if RANK == 0:
        assert total == 0, total
    elif RANK == 1:
        assert total > 10_000_000, total
    elif RANK == 2:
        assert total > 10_000_000, total
    else:
        raise ValueError(RANK)

    time_per_example = elapsed / 1000
    mpi_examples_per_second = 1 / time_per_example

    assert mpi_examples_per_second >= 10_000, mpi_examples_per_second
    assert mpi_examples_per_second <= 300_000, mpi_examples_per_second

    print('split_managed examples/second =', mpi_examples_per_second)

    start = time.perf_counter()
    for i in examples:
        total += i
    elapsed = time.perf_counter() - start

    time_per_example = elapsed / 1000
    py_examples_per_second = 1 / time_per_example

    assert py_examples_per_second >= 250_000, py_examples_per_second
    assert py_examples_per_second <= 9_000_000, py_examples_per_second


if __name__ == '__main__':
    from dlp_mpi.testing import test_relaunch_with_mpi
    test_relaunch_with_mpi()

    dlp_mpi.barrier()
    executable()
    dlp_mpi.barrier()
    speedup()
    dlp_mpi.barrier()
    cross_communication()
    dlp_mpi.barrier()
    worker_fails()
    dlp_mpi.barrier()
    pbar()
    dlp_mpi.barrier()
    overhead()
    dlp_mpi.barrier()

    # ToDo: find a way to test the progress bar. Maybe with mock?
