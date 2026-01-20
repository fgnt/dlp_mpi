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

    # SLURM_SRUN_COMM_HOST is the IP of the submit node in salloc,
    # hence I got OSError: [Errno 99] Cannot assign requested address
    # _HOST = os.environ['SLURM_SRUN_COMM_HOST']

    # maybe not always defined
    # _HOST = os.environ['PMIX_HOSTNAME']

    # SLURM_STEP_NODELIST=cn-0256  Might be wrong, if srun uses a subset of the nodes
    # _HOST = os.environ['SLURMD_NODENAME']  # The name of the node, where the task is running -> useless
    # _HOST = os.environ['SLURM_TOPOLOGY_ADDR']  # SLURM_TOPOLOGY_ADDR=opasw[06,14-15,07,11].opasw13.cn-0256

    ###########################################################################
    # PORT
    ###########################################################################
    if 'SLURM_STEP_RESV_PORTS' in os.environ:
        # I cannot find a proper documentation for SLURM_STEP_RESV_PORTS.
        # But https://supercloud.mit.edu/tensorboard indicates that it can be used
        # for user code. So I will use it.
        # SLURM_STEP_RESV_PORTS=17058-17059
        _PORT = int(os.environ['SLURM_STEP_RESV_PORTS'].split('-')[0])
    elif 'SLURM_JOB_ID' in os.environ:
        # Fallback: If SLURM_STEP_RESV_PORTS is not defined,
        # select a port from the range 60001-63000, based on the job ID
        # Using modulo should make it very unlikely,
        # that two jobs get the same port.
        job_id = int(os.environ['SLURM_JOB_ID'])
        PORT_BASE, PORT_SPAN = 60001, 3000
        _PORT = PORT_BASE + (job_id % PORT_SPAN)

    elif 'SLURM_SRUN_COMM_PORT' in os.environ:
        # This breaks, if the submit node and the compute nodes are the same.
        # 
        # Always the same as SLURM_STEP_LAUNCHER_PORT ?
        # 
        # ToDO: Find something better than 'SLURM_SRUN_COMM_PORT'.
        # It is not guaranteed, that the PORT is free, only very likely.
        # 
        # It is not clear, whether the port is free to be used.
        # Given the fact, that SLURM_SRUN_COMM_HOST is the IP of the submit node in
        # salloc, it is probably not planned to be used outside of slurm plugins.
        # 
        # SLURM_SRUN_COMM_PORT is the port used to communicate with the submit node.
        # So if the submit node and the compute nodes are different, this should probably
        # work.
        # 
        # _PORT = int(os.environ['SLURM_SRUN_COMM_PORT'])
        _PORT = int(os.environ['SLURM_SRUN_COMM_PORT'])
    else:
        raise RuntimeError(
            "Cannot find SLURM_STEP_RESV_PORTS, SLURM_JOB_ID or SLURM_SRUN_COMM_PORT "
            "in the environ."
        )

    ###########################################################################
    # RANK, SIZE and authkey
    ###########################################################################
    RANK = int(os.environ['SLURM_PROCID'])  # or PMIX_RANK,
    SIZE = int(os.environ['SLURM_NTASKS'])  # or SLURM_NPROCS, PMIX_SIZE

    authkey = os.getenv(
        "AME_AUTHKEY",
        str_to_authkey(os.environ['SLURM_JOB_START_TIME'] + __file__))
    if isinstance(authkey, str):
        authkey = authkey.encode('utf-8')

    return _HOST, _PORT, RANK, SIZE, authkey
