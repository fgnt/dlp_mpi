# from ame.core import _HOST, _PORT, SIZE, RANK


# print(_HOST, _PORT, SIZE, RANK)

if __name__ == '__main__':
    import os

    print('SLURM_SRUN', os.environ['SLURM_SRUN_COMM_HOST'], os.environ['SLURM_SRUN_COMM_PORT'], os.environ['SLURM_PROCID'], os.environ['SLURM_NTASKS'])

    # print('PMI', os.environ['PMI_RANK'], os.environ['PMI_SIZE'])
    # print(os.environ['OMPI_COMM_WORLD_RANK'], os.environ['OMPI_COMM_WORLD_SIZE'])

    for k, v in os.environ.items():
        # if 'PMI' in k:
        #     print(k, v)
        # if 'OMPI' in k:
        #     print(k, v)
        if 'SLURM' in k and 'NODE' in k:
            print(k, v[:50])
        # if 'SRUN' in k:
        #     print(k, v)
