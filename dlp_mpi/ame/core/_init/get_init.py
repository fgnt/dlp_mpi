import os

DEBUG = False


def _get(type_, debug=DEBUG):

    from . import slurm, pmi, ompi_mca_orte, common

    def get_ame_host_rank_size():
        from .common import str_to_authkey, authkey_encode
        # Custom launcher
        host = os.getenv("AME_HOST", '127.0.0.1')
        port = int(os.getenv("AME_PORT", 12345))  # ToDO: Replace the port with a free port
        rank = int(os.getenv("AME_RANK", 0))
        size = int(os.getenv("AME_SIZE", 1))
        if 'AME_AUTHKEY' in os.environ:
            authkey = authkey_encode(os.getenv("AME_AUTHKEY"))
        else:
            authkey = str_to_authkey(f'{host}:{port}')
        return host, port, rank, size, authkey

    def get_fallback_host_rank_size():
        from .common import str_to_authkey

        rank = 0
        size = 1
        host = 'localhost'
        port = -1
        authkey = str_to_authkey(f'{host}:{port}')
        return host, port, rank, size, authkey
    
    type_to_fn = {}

    if type_ is None:
        type_ = []
        # ToDO: Add something like AME_SHARED_DIR:
        #  - Used to share the port (and maybe authkey) between the processes.
        #  - Can be used by OpenMPI (alternative to OMPI_MCA_orte_top_session_dir) and SLURM (for cases where SLURM_STEP_RESV_PORTS is not defined).
        if 'AME_RANK' in os.environ:
            type_to_fn['AME'] = get_ame_host_rank_size
        if 'PMI_RANK' in os.environ:
            type_to_fn['MPICH / PMI'] = pmi.get_host_rank_size
        if 'OMPI_COMM_WORLD_RANK' in os.environ:
            type_to_fn['OpenMPI'] = ompi_mca_orte.get_host_rank_size
        if 'SLURM_SRUN_COMM_HOST' in os.environ:
            type_to_fn['SLURM'] = slurm.get_host_rank_size
        # if len(type_to_fn) == 0:
        type_to_fn['None'] = get_fallback_host_rank_size

    type_ = list(type_to_fn.keys())[0]
    try:
        host, port, rank, size, authkey = type_to_fn[type_]()
    except Exception as e:
        raise RuntimeError(
            f'Error getting host, port, rank, size using {type_} ({type_to_fn[type_].__module__}.{type_to_fn[type_].__name__}): {e}'
        ) from e

    if debug:
        print(f'{type_}: {host}:{port}, Rank {rank} of {size} ({list(type_to_fn.keys())})')
        if rank == 0:
            print('Environment variables:')
            for k, v in os.environ.items():
                print(f'{k}: {str(v)[:100]}')
    return host, port, rank, size, authkey, type_, type_to_fn


def print_info():

    host, port, rank, size, authkey, type_, type_to_fn = _get(None)

    if rank == 0:
        print('dlp_mpi.ame init info from the root process:')
        print(f'  Using {type_} for getting host, port, rank, size:')
        print(f'    Host: {host}, Port: {port}')
        print(f'    Rank: {rank}, Size: {size}')
        # print(f'Authkey: {authkey}')
        print('  Available methods:')
        for t, fn in type_to_fn.items():
            print(f'    {t}: {fn.__module__}.{fn.__name__}')


def get():

    host, port, rank, size, authkey, type_, _ = _get(None)

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


if __name__ == "__main__":
    print_info()
