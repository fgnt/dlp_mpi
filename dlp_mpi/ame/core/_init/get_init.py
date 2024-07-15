import os

DEBUG = False


def get():
    if 'SLURM_SRUN_COMM_HOST' in os.environ:
        type_ = 'SLURM'
        from .slurm import get_host_rank_size
        host, port, rank, size, authkey = get_host_rank_size()
        if DEBUG:
            print(f'SLURM: {host}:{port}, Rank {rank} of {size}')
    elif 'PMI_RANK' in os.environ:
        type_ = 'MPICH / PMI'
        from .pmi import get_host_rank_size
        # IP is 127.0.0.1, i.e. localhost
        host, port, rank, size, authkey = get_host_rank_size()
        if DEBUG:
            print(f'PMI: {host}:{port}, Rank {rank} of {size}')
    elif 'OMPI_COMM_WORLD_RANK' in os.environ:
        type_ = 'OpenMPI'
        from .ompi_mca_orte import get_host_rank_size
        # IP is 127.0.0.1, i.e. localhost
        host, port, rank, size, authkey = get_host_rank_size()
        if DEBUG:
            print(f'OMPI: {host}:{port}, Rank {rank} of {size}')
    elif 'AME_RANK' in os.environ:
        type_ = 'AME'
        from .common import str_to_authkey, authkey_encode
        # Custom launcher
        host = os.getenv("AME_HOST", '127.0.0.1')
        port = int(os.getenv("AME_PORT",
                              12345))  # ToDO: Replace the port with a free port
        rank = int(os.getenv("AME_RANK", 0))
        size = int(os.getenv("AME_SIZE", 1))
        if 'AME_AUTHKEY' in os.environ:
            authkey = authkey_encode(os.getenv("AME_AUTHKEY"))
        else:
            authkey = str_to_authkey(f'{host}:{port}')
        if DEBUG:
            print(f'AME: {host}:{port}, Rank {rank} of {size}')

    else:
        type_ = 'None'
        from .common import str_to_authkey

        rank = 0
        size = 1
        host = 'localhost'
        port = -1
        authkey = str_to_authkey(f'{host}:{port}')

    assert isinstance(host, str), (host, port, rank, size, authkey, type_)
    assert isinstance(port, int), (host, port, rank, size, authkey, type_)
    assert isinstance(rank, int), (host, port, rank, size, authkey, type_)
    assert isinstance(size, int), (host, port, rank, size, authkey, type_)
    assert isinstance(authkey, bytes), (host, port, rank, size, authkey, type_)
    return {
        'host': host,
        'port': port,
        'rank': rank,
        'size': size,
        'authkey': authkey
    }
