import threading
import socket
import contextlib
import io
import sys


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("",0))
        return s.getsockname()[1]


def _in_thread(host, port, rank, size, callback, out: dict):
    """
    >>> _in_thread('localhost', 12345, 0, 2, lambda rank, size: print(f'Hello from {rank}/{size}'), {})
    Hello from 0/2
    """

    import inspect
    kwargs = {
        k: v
        for k, v in dict(host=host, port=port, rank=rank, size=size).items()
        if k in inspect.signature(callback).parameters
    }

    try:
        out[rank] = callback(**kwargs)
    except BaseException as e:
        import traceback
        print(f'Rank {rank} of {size}: {e}')
        traceback.print_exc()
        raise


def thread_based_test(callback, size, in_main=0):
    port = get_free_port()
    # host = 'localhost'
    host = socket.gethostname()
    assert size > 0, size

    threads = {}
    out = {}
    for rank in sorted(range(size), key=lambda x: size if x == in_main else x):
        kwargs = dict(host=host, port=port, rank=rank, size=size, callback=callback, out=out)
        if rank == 0:
            _in_thread(**kwargs)
        else:
            t = threading.Thread(target=_in_thread, kwargs=kwargs)
            t.daemon = True
            t.start()
            threads[rank] = t

    for i, t in threads.items():
        print(f'Wait for thread {i}')
        t.join()
        print(f'Thread {i} finished')

    return out
