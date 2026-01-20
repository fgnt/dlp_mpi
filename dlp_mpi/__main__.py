
if __name__ == "__main__":
    import dlp_mpi

    backend = str(dlp_mpi.MPI).split("'")[1]

    if dlp_mpi.RANK == 0:
        print(f'MPI backend: {backend}')

    if 'dlp_mpi.ame.MPI' == backend:
        import dlp_mpi.ame.core._init.get_init as get_init
        get_init.print_info()

    dlp_mpi.barrier()
    print(f'Hello from rank {dlp_mpi.RANK} of {dlp_mpi.SIZE}!')
