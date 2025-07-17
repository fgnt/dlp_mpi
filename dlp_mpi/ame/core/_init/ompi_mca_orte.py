import os
import time
from pathlib import Path
from .common import get_host_and_port, get_authkey


def get_host_rank_size():
    """
    >>> import tempfile
    >>> import os
    >>> os.environ['OMPI_COMM_WORLD_RANK'] = '0'
    >>> os.environ['OMPI_COMM_WORLD_SIZE'] = '2'
    >>> with tempfile.TemporaryDirectory() as tmpdir:  # doctest: +ELLIPSIS
    ...     os.environ['OMPI_MCA_orte_top_session_dir'] = tmpdir
    ...     *vars, authkey1 = get_host_rank_size()
    ...     print(vars)
    ...     os.environ['OMPI_COMM_WORLD_RANK'] = '1'
    ...     *vars, authkey2 = get_host_rank_size()
    ...     print(vars)
    ...     assert authkey1 == authkey2
    ['...', ..., 0, 2]
    ['...', ..., 1, 2]
    """

    # Using PMIx would be better, but I couldn't find how to get it work.

    rank = int(os.environ['OMPI_COMM_WORLD_RANK'])
    size = int(os.environ['OMPI_COMM_WORLD_SIZE'])

    dir = Path(os.environ['OMPI_MCA_orte_top_session_dir'])
    tmp_file = dir / 'host_and_port.txt_'
    file = dir / 'host_and_port.txt'
    assert tmp_file != file, f'{tmp_file} == {file}'

    if rank == 0:
        host, port = get_host_and_port()
        authkey = get_authkey()
        with open(tmp_file, 'w') as f:
            f.write(f'{host}:{port}')
        with open(tmp_file, 'ab') as f:
            f.write(b'\n')
            f.write(authkey)

        os.rename(tmp_file, file)  # "atomic operation", see https://docs.python.org/3/library/os.html#os.rename
    else:
        # If this is not the first process, read the port from the port file
        i = 0
        while True:
            i += 1
            try:
                # with open(file, 'r') as f:
                #     host, port, authkey = f.read().split(':', maxsplit=2)
                with open(file, 'rb') as f:
                    host_port, authkey = f.read().split(b'\n', maxsplit=1)
                    host, port = host_port.decode().split(':', maxsplit=1)
                    port = int(port)
                break
            except FileNotFoundError as e:
                if i > 3600:  # 6 minutes
                    raise FileNotFoundError(f'File {file} not found after {i} tries.') from e
                time.sleep(0.1)
    return host, port, rank, size, authkey

