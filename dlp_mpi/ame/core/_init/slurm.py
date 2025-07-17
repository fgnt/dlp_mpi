import os
from .common import str_to_authkey


def expand_node_list(node_list):
    """
    >>> expand_node_list('node[01-03,04,05]')
    ['node01', 'node02', 'node03', 'node04', 'node05']
    >>> expand_node_list('node01')
    ['node01']
    """
    if '[' in node_list:
        nodes = []
        base, ranges = node_list.split('[')
        range_parts = ranges.strip(']').split(',')
        for range_part in range_parts:
            if '-' in range_part:
                start, end = range_part.split('-')
                prefix_len = len(start)
                for i in range(int(start), int(end) + 1):
                    nodes.append(f"{base}{str(i).zfill(prefix_len)}")
            else:
                nodes.append(f"{base}{range_part}")
        return nodes
    elif ',' in node_list:
        return node_list.split(',')
    else:
        return [node_list]


def get_host_rank_size():
    ###########################################################################
    # HOST
    ###########################################################################
    # SLURM_STEP_NODELIST n2cn[0501,0509]
    # Is it correct, that the first node is the one where the rank 0 is running?
    _HOST = expand_node_list(os.environ['SLURM_STEP_NODELIST'])[0]

    # SLURM_SRUN_COMM_HOST is the IP of the submit node in salloc, hence I got OSError: [Errno 99] Cannot assign requested address
    # _HOST = os.environ['SLURM_SRUN_COMM_HOST']

    # maybe not always defined
    # _HOST = os.environ['PMIX_HOSTNAME']

    # SLURM_STEP_NODELIST=cn-0256  Might be wrong, if srun uses a subset of the nodes
    # _HOST = os.environ['SLURMD_NODENAME']  # The name of the node, where the task is running -> useless
    # _HOST = os.environ['SLURM_TOPOLOGY_ADDR']  # SLURM_TOPOLOGY_ADDR=opasw[06,14-15,07,11].opasw13.cn-0256

    ###########################################################################
    # PORT
    ###########################################################################
    # It is not clear, whether the port is free to be used.
    # Given the fact, that SLURM_SRUN_COMM_HOST is the IP of the submit node in
    # salloc, it is probably not planned to be used outside of slum plugins.
    # _PORT = int(os.environ['SLURM_SRUN_COMM_PORT'])

    # I cannot find a proper documentation for SLURM_STEP_RESV_PORTS.
    # But https://supercloud.mit.edu/tensorboard indicates that it can be used
    # for used code. So I will use it.
    # SLURM_STEP_RESV_PORTS=17058-17059
    _PORT = int(os.environ['SLURM_STEP_RESV_PORTS'].split('-')[0])

    ###########################################################################
    # RANK, SIZE and authkey
    ###########################################################################
    RANK = int(os.environ['SLURM_PROCID'])  # or PMIX_RANK,
    SIZE = int(os.environ['SLURM_NTASKS'])  # or SLURM_NPROCS, PMIX_SIZE

    authkey = os.getenv(
        "AME_AUTHKEY",
        str_to_authkey(os.environ['SLURM_JOB_START_TIME'] + __file__))

    return _HOST, _PORT, RANK, SIZE, authkey
