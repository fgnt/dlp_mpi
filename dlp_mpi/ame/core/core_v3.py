from ..constants import *
from .logger import info
from .con_v3 import establish_connection_v3, Clientv3, Rootv3


__all__ = [
    'COMM_WORLD',
    'Status',
]


class c:
    red = '\033[91m'
    green = '\033[92m'
    yellow = '\033[93m'
    blue = '\033[94m'
    magenta = '\033[95m'
    cyan = '\033[96m'
    reset = '\033[0m'


class Status:
    def __init__(self):
        self.source: int = None
        self.tag: int = None


class Communicator_v3:
    def __init__(
            self,
            rank,
            size,
            port,
            host,
            authkey,
            depth=0,
            debug=DEBUG,
    ):
        assert isinstance(rank, int), (rank, type(rank))
        assert isinstance(size, int), (size, type(size))

        self.rank = rank
        self.size = size

        self._depth = depth
        self._debug = debug
        self._host = host
        self._port = port
        self._authkey = authkey

        if self.size > 1:
            assert isinstance(port, int), (port, type(port))
            self._con: 'Clientv3 | Rootv3' = establish_connection_v3(
                host=host,
                port=port,
                rank=rank,
                size=size,
                authkey=authkey,
                debug=debug,
            )
        else:
            class DummyCon:
                def send(self, obj, dest, tag=0):
                    # Dummy implementation for bcast
                    assert dest == [], dest

                def recv(self, source, tag=ANY_TAG, status=None):
                    # Dummy implementation for gather
                    assert source == [], source
                    return {}
            self._con: 'Clientv3 | Rootv3' = DummyCon()

    def send(self, obj, dest, tag=0):
        assert isinstance(dest, int), (dest, type(dest))
        self._con.send(obj, dest, tag)

    def recv(self, source=ANY_SOURCE, tag=ANY_TAG, status=None):
        assert isinstance(source, int), (source, type(source))
        return self._con.recv(source, tag, status)

    def bcast(self, obj, root=0, _tag=BCAST_TAG):
        if self.rank == root:
            dest = [i for i in range(self.size) if i != root]
            self._con.send(obj, dest, tag=_tag)
            return obj
        else:
            return self._con.recv(source=root)

    def gather(self, obj, root=0, _tag=GATHER_TAG):
        if self.rank == root:
            source = [i for i in range(self.size) if i != root]
            ret = self._con.recv(source)
            ret = [
                ret[i] if i != root else obj
                for i in range(0, self.size)
            ]
            return ret
        else:
            self._con.send(obj, root, tag=_tag)
            return None

    def barrier(self):
        # Note: This is a naive implementation.
        #       The BARRIER_TAG is used for an improved implementation,
        #       i.e., send only a header and no body.
        self.bcast(None, _tag=BARRIER_TAG)
        self.gather(None, _tag=BARRIER_TAG)

    def clone(self):
        from ._init.common import find_free_port, get_authkey
        port = find_free_port() if self.rank == 0 else None
        authkey = get_authkey(force_random=True) if self.rank == 0 else None

        # bcast and gather act as barrier
        port, authkey = self.bcast([port, authkey])
        self.gather(None, _tag=BARRIER_TAG)

        return self.__class__(
            rank=self.rank,
            size=self.size,
            port=port,
            host=self._host,
            authkey=authkey,
            depth=self._depth + 1,
            debug=self._debug,
        )

    Barrier = barrier
    Clone = clone


Communicator = Communicator_v3


# class _MPI:
#     COMM_WORLD = Communicator(rank=RANK, size=SIZE)
#     COMM = COMM_WORLD
#
#     RANK = RANK
#     SIZE = SIZE
#
#
# MPI = _MPI()


from ._init.get_init import get
# _HOST, _PORT, RANK, SIZE, AUTHKEY = get()
_kwargs = get()

try:
    COMM_WORLD = Communicator(**_kwargs)
    COMM = COMM_WORLD
except Exception:
    raise RuntimeError(_kwargs)
