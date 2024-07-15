import socket
import os
import re
import base64

from .common import get_host_and_port, get_authkey, authkey_encode, authkey_decode

return_codes = {
    0: ('PMI_SUCCESS', 'operation completed successfully'),
    -1: ('PMI_FAIL', 'operation failed'),
    1: ('PMI_ERR_INIT', 'PMI not initialized'),
    2: ('PMI_ERR_NOMEM', 'input buffer not large enough'),
    3: ('PMI_ERR_INVALID_ARG', 'invalid argument'),
    4: ('PMI_ERR_INVALID_KEY', 'invalid key argument'),
    5: ('PMI_ERR_INVALID_KEY_LENGTH', 'invalid key length argument'),
    6: ('PMI_ERR_INVALID_VAL', 'invalid val argument'),
    7: ('PMI_ERR_INVALID_VAL_LENGTH', 'invalid val length argument'),
    8: ('PMI_ERR_INVALID_LENGTH', 'invalid length argument'),
    9: ('PMI_ERR_INVALID_NUM_ARGS', 'invalid number of arguments'),
    10: ('PMI_ERR_INVALID_ARGS', 'invalid args argument'),
    11: ('PMI_ERR_INVALID_NUM_PARSED', 'invalid num_parsed length argument'),
    12: ('PMI_ERR_INVALID_KEYVALP', 'invalid keyvalp argument'),
    13: ('PMI_ERR_INVALID_SIZE', 'invalid size argument'),
}


class PMI:
    def __init__(self, verbose=False):
        pmi_fd = int(os.environ['PMI_FD'])
        self.sock = socket.fromfd(pmi_fd, socket.AF_UNIX, socket.SOCK_STREAM)
        self.verbose = verbose
        self.kvsname = 'mykvs'

    def exec(self, msg: str, check_rc=True):
        if isinstance(msg, str):
            msg = msg.encode()
        msg = msg.strip() + b'\n'
        if self.verbose:
            print(f'Send {msg.decode().strip()}', flush=True)

        self.sock.sendall(msg)
        response = self.sock.recv(1024)
        if self.verbose:
            print(f'Received ({rank}):', response.decode().strip(), flush=True)
        if check_rc:
            try:
                rc = int(re.search(b'rc=(\d+)', response).group(1))
            except Exception:
                raise RuntimeError('Could not parse return code', response)
            if rc != 0:
                raise RuntimeError(return_codes[rc], response)
            return response, rc
        else:
            return response, None

    def init(self):
        response, rc = self.exec('cmd=init pmi_version=1 pmi_subversion=1\n')

        if response != b'cmd=response_to_init pmi_version=1 pmi_subversion=1 rc=0\n':
            raise RuntimeError(return_codes[rc], response)

    def put(self, key, value):
        response, rc = self.exec(f'cmd=put kvsname={self.kvsname} key={key} value={value}\n')
        if response != b'cmd=put_result rc=0 msg=success\n':
            raise RuntimeError(return_codes[rc], response)

    def get(self, key):
        response, rc = self.exec(f'cmd=get kvsname={self.kvsname} key={key}\n')
        if not response.startswith(b'cmd=get_result rc=0 msg=success value='):
            raise RuntimeError(return_codes[rc], response)
        return response.split(b'value=')[1].decode().strip()

    def __setitem__(self, key, value):
        self.put(key, value)

    def __getitem__(self, key):
        return self.get(key)

    def barrier(self):
        response, rc = self.exec('cmd=barrier_in\n', check_rc=False)
        if response != b'cmd=barrier_out\n':
            raise RuntimeError(return_codes[rc], response)

    def close(self):
        self.sock.close()


def get_host_rank_size():
    rank = int(os.environ['PMI_RANK'])
    size = int(os.environ['PMI_SIZE'])

    pmi = PMI()
    try:
        if rank == 0:
            host, port = get_host_and_port()
            authkey = get_authkey()

            pmi.init()
            pmi['mykey'] = f'{host}:{port}'
            pmi['authkey'] = authkey_decode(authkey)
            pmi.barrier()
            pmi.barrier()
        else:
            pmi.barrier()
            host, port = pmi['mykey'].split(':', maxsplit=1)
            authkey = authkey_encode(pmi['authkey'])
            port = int(port)
            pmi.barrier()
    finally:
        pmi.close()
    return host, port, rank, size, authkey


if __name__ == '__main__':

    # Assume fd is the file descriptor
    rank = int(os.environ['PMI_RANK'])
    size = int(os.environ['PMI_SIZE'])


    pmi = PMI(verbose=True)
    if rank == 0:
        # Get the PMI_FD environment variable

        pmi.init()
        pmi['mykey'] = '127.0.0.1:12345'
        pmi.barrier()
        pmi.barrier()
    else:
        pmi.barrier()
        print(f'get {rank}', pmi['mykey'])
        pmi.barrier()

    # print(rank, data)


