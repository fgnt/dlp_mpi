"""
This code doesn't work!!!
The documentation for PMIx is terrible.
"""
from cffi import FFI

# Define the C header for PMIx
pmix_header = """
    int PMIx_Init(int *rc, char ***err);
    int PMIx_Finalize(void);
    int PMIx_Spawn(int *rc, char *cmd, char *argv[]);
    int PMIx_Put(char *key, void *val, size_t vallen);
    int PMIx_Commit(void);
    int PMIx_Get(char *key, void **val, size_t *vallen);
"""

# Create a ffi object
ffi = FFI()



# Load the PMIx shared library
pmix_lib = ffi.dlopen('/lib/x86_64-linux-gnu/libpmix.so')  # Update the path accordingly

# Parse the C header
ffi.cdef(pmix_header)

# Initialize PMIx
def initialize():
    rc = ffi.new("int *")
    err = ffi.new("char ***")
    result = pmix_lib.PMIx_Init(rc, err)
    if result != 0:
        raise RuntimeError(f"Failed to initialize PMIx")
    return rc[0]

# Finalize PMIx
def finalize():
    result = pmix_lib.PMIx_Finalize()
    if result != 0:
        raise RuntimeError("Failed to finalize PMIx")
    return result

# # Spawn additional processes
# def spawn_processes(num_procs):
#     cmd = ffi.new("char []", b"python")  # Example command to execute another Python script
#     args = [ffi.new("char []", b"child_script.py")]  # Example arguments for the child script
#     argv = ffi.new("char *[]", args + [ffi.NULL])
#     rc = ffi.new("int *")
#     result = pmix_lib.PMiX_Spawn(rc, cmd, argv)
#     if result != 0:
#         raise RuntimeError("Failed to spawn processes")
#     return rc[0]

# Put data into PMIx key-value store
def put_data(key, value):
    c_key = ffi.new("char []", key.encode())
    c_value = ffi.new("char []", value.encode())
    result = pmix_lib.PMIx_Put(c_key, c_value, len(value))
    if result != 0:
        raise RuntimeError("Failed to put data into PMIx key-value store")
    return result

# Commit changes to PMIx key-value store
def commit():
    result = pmix_lib.PMIx_Commit()
    if result != 0:
        raise RuntimeError("Failed to commit changes to PMIx key-value store")
    return result

# Get data from PMIx key-value store
def get_data(key):
    c_key = ffi.new("char []", key.encode())
    c_value = ffi.new("void **")
    vallen = ffi.new("size_t *")
    result = pmix_lib.PMIx_Get(c_key, c_value, vallen)
    if result != 0:
        raise RuntimeError("Failed to get data from PMIx key-value store")
    return ffi.string(c_value[0], vallen[0]).decode() if c_value[0] else None

if __name__ == '__main__':
    import os
    rank = int(os.environ['PMIX_RANK'])
    size = int(os.environ['OMPI_COMM_WORLD_SIZE'])

    if rank == 0:
        initialize()
        data = "Hello from rank 0"
        put_data("mykey", data)
        commit()
        finalize()
    else:
        import time
        time.sleep(1)
        # data = get_data("mykey")
        # print(f"Rank {rank} received data: {data}")
