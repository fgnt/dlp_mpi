"""
I wrote this code while reading the following tutorial:
https://realpython.com/python-sockets/#echo-client-and-server
Hence, some code has its origin in the tutorial.


"""
import os
import pickle
import struct
import sys
import time
import selectors
import socket
import base64
import hashlib
import types

from ..constants import *
from .logger import info


class SocketClosed(Exception):
    pass


class Mixin:
    def __init__(self, host, port, rank, size, debug=False):
        self.host = host
        self.port = port
        self.rank = rank
        self.size = size
        self.debug = debug

    # @staticmethod
    # def _msg_recv(sock, length):
    #     chunks = []
    #     remaining = length
    #     while remaining > 0:
    #         chunk = sock.recv(min(remaining, 65536))
    #         if not chunk:
    #             raise SocketClosed("Socket closed")
    #         chunks.append(chunk)
    #         remaining -= len(chunk)
    #     return b''.join(chunks)

    # Not sure, what the optimal buffer size would be.
    _MAX_RECV = 65536  # = 2**16
    # _MAX_RECV = 1048576  # = 2**20
    # _MAX_RECV = 16777216  # = 2**24  # With this, get many packages of length 65483

    @classmethod
    def recv_nbytes(cls, sock: 'socket.SocketType', length):
        read = 0
        try:
            view = memoryview(bytearray(length))
            while length:
                recv_length = sock.recv_into(view[read:], min(length, cls._MAX_RECV))
                if recv_length == 0:
                    raise SocketClosed("Socket closed")
                length -= recv_length
                read += recv_length
            return view.tobytes()  # convert to bytes
        except KeyboardInterrupt as e:
            raise KeyboardInterrupt(f"Waiting for {length} more bytes. Got {read} bytes.") from e

    _HEADER_FMT = 'Qi'
    _HEADER_FMT_SIZE = struct.calcsize(_HEADER_FMT)
    # i int (4 bytes)
    # I unsigned int (4 bytes)
    # q long long (8 bytes)
    # Q unsigned long long (8 bytes)

    @classmethod
    def msg_recv(cls, sock):
        length, tag = struct.unpack(cls._HEADER_FMT, cls.recv_nbytes(sock, cls._HEADER_FMT_SIZE))
        if tag not in [BARRIER_TAG]:
            data = pickle.loads(cls.recv_nbytes(sock, length))
        else:
            data = None
        return data, tag

    @classmethod
    def msg_send(cls, sock, tag, data):
        if tag not in [BARRIER_TAG]:
            b = pickle.dumps(data)
            try:
                msg = struct.pack(cls._HEADER_FMT, len(b), tag)
                assert len(msg) == cls._HEADER_FMT_SIZE, (len(msg), cls._HEADER_FMT_SIZE)
                sock.sendall(msg)
            except struct.error as e:
                raise RuntimeError(f"Could not sendall(struct.pack('ii', {len(b)}, {tag}))") from e
            sock.sendall(b)
        else:
            assert data is None, (data, tag)
            sock.sendall(struct.pack(cls._HEADER_FMT, 0, tag))


class Rootv3(Mixin):
    def __init__(self, sel, socks, host, port, rank, size, debug=False):
        super().__init__(host, port, rank, size, debug)
        self.sel = sel
        self.socks = socks

    def __del__(self):
        self.sel.close()

    def send(self, data, dest, tag=ANY_TAG):
        if isinstance(dest, int):
            dest = {dest}
        elif isinstance(dest, (tuple, list, set)):
            # broadcast
            dest = set(dest)
            assert all(isinstance(i, int) for i in dest), dest
        else:
            raise TypeError(dest, type(dest))

        while dest:
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    raise Exception('Got unexpected connection', key, mask)
                else:
                    if mask & selectors.EVENT_READ:
                        pass
                    if mask & selectors.EVENT_WRITE:
                        if key.data.rank in dest:
                            dest.remove(key.data.rank)
                            self.msg_send(key.fileobj, tag, data)
                if not dest:
                    break

    def recv(self, source=ANY_SOURCE, tag=ANY_TAG, status=None):
        assert tag == ANY_TAG, (tag, f'Only ANY_TAG ({ANY_SOURCE}) is supported. Not {tag}')

        if isinstance(source, int):
            pass
        elif isinstance(source, (tuple, list, set)):
            # gather
            source = set(source)
            assert all(isinstance(i, int) and source != ANY_SOURCE for i in source), source
            assert status is None, status
        else:
            raise TypeError(source, type(source))

        datas = {}
        while isinstance(source, int) or source:
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    raise Exception('Got unexpected connection', key, mask)
                else:
                    if mask & selectors.EVENT_READ:
                        if ANY_SOURCE == source or key.data.rank == source or key.data.rank in source:
                            try:
                                data, msg_tag = self.msg_recv(key.fileobj)
                            except SocketClosed:
                                if source != ANY_SOURCE:
                                    raise
                                # The other side closed the socket.from
                                # That can happen, when the source is ANY_SOURCE,
                                # i.e. one RANK has finished, while others are still running.
                                self.sel.unregister(key.fileobj)
                                key.fileobj.close()
                                del self.socks[key.data.addr]
                                assert len(self.socks) > 0, self.socks
                                continue

                            if ANY_SOURCE == source or key.data.rank == source:
                                if status:
                                    status.source = key.data.rank
                                    status.tag = msg_tag
                                return data
                            else:
                                source.remove(key.data.rank)
                                datas[key.data.rank] = data
                    if mask & selectors.EVENT_WRITE:
                        pass
                if isinstance(source, set) and len(source) == 0:
                    break
        return datas


class Clientv3(Mixin):
    def __init__(self, sock, host, port, rank, size, debug=False):
        super().__init__(host, port, rank, size, debug)
        self.sock = sock

    def __del__(self):
        self.sock.close()

    def send(self, obj, dest, tag=ANY_TAG):
        assert dest == 0, dest
        try:
            return self.msg_send(self.sock, tag, obj)
        except SocketClosed:
            if DEBUG:
                raise SocketClosed(
                    f"Could not send {obj} from {self.rank} to {dest} with tag {tag}") from None
            else:
                # Usually, one process fails, while the others are still running.
                # So in most cases, the SocketClosed is not the root of the
                # problem, hence print only one line instead of a fill traceback.
                info(f"Issue in send for RANK {self.rank}. Set DLP_MPI_DEBUG to get a traceback.")
                sys.exit(1)

    def recv(self, source=ANY_SOURCE, tag=ANY_TAG, status=None):
        assert tag == ANY_TAG, (tag, f'Only ANY_TAG ({ANY_SOURCE}) is supported. Not {tag}')
        assert source in [0, ANY_SOURCE], source

        try:
            data, msg_tag = self.msg_recv(self.sock)
        except SocketClosed:
            if DEBUG:
                raise SocketClosed(
                    f"Could not receive on {self.rank} from {source} with tag {tag}") from None
            else:
                # Usually, one process fails, while the others are still running.
                # So in most cases, the SocketClosed is not the root of the
                # problem, hence print only one line instead of a fill traceback.
                info(f"Issue in recv for RANK {self.rank}. Set DLP_MPI_DEBUG to get a traceback.")
                sys.exit(1)
        if status:
            status.source = 0
            status.tag = msg_tag

        return data


class _Socket:
    def __init__(self, s):
        self.s = s

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.s.close()

    def connect(self, *args):
        return self.s.connect(*args)

    def sendall(self, bytes):
        info(f"sendall {bytes} (len: {len(bytes)})", frames=2)
        return self.s.sendall(bytes)

    def recv(self, *args):
        r = self.s.recv(*args)
        info(f"recv {r}", frames=2)
        return r

    def fileno(self):
        return self.s.fileno()

    def close(self):
        return self.s.close()

    def recv_into(self, buffer: memoryview, nbytes=None):
        r = self.s.recv_into(buffer=buffer, nbytes=nbytes)
        info(f"{r} = recv_into {buffer.tobytes()[:r]} {nbytes}", frames=2)
        return r


def establish_connection_v3(host, port, rank, size, *, authkey, debug=False) -> 'Rootv3|Clientv3':
    if size < 200:
        fmt = 'B'  # limited to 256 processes
    else:
        fmt = 'H'  # limited to 65536 processes
    fmt_len = struct.calcsize(fmt)

    try:
        _ = struct.pack(fmt, size)  # Fail early if size is not a compatible type
    except Exception:
        raise RuntimeError(f"Size {size} ({type(size)}) is not a compatible type for struct.unpack({fmt!r}, size)")

    if rank == 0:
        sel = selectors.DefaultSelector()
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            lsock.bind((host, port))
        except Exception:
            for k, v in os.environ.items():
                if 'port' in k.lower():
                    print(k, v)
            raise Exception(f"Could not bind to {(host, port)}")
        lsock.listen()
        # lsock.setblocking(False)  # We don't need the non-blocking behaviour.
        sel.register(lsock, selectors.EVENT_READ, data=None)

        try:
            # ToDo: Remove connected dict. It is not nessesary.
            connected = {}
            while True:
                events = sel.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        conn, addr = key.fileobj.accept()
                        # conn.setblocking(False)
                        data = types.SimpleNamespace(addr=addr, rank=None, size=size)
                        events = selectors.EVENT_READ | selectors.EVENT_WRITE
                        if debug:
                            conn = _Socket(conn)
                        sel.register(conn, events, data=data)
                    elif mask & selectors.EVENT_READ:
                        if key.data.addr not in connected:
                            other_rank, = struct.unpack(fmt, Mixin.recv_nbytes(key.fileobj, fmt_len))
                            if authenticate_server_side(key.fileobj, authkey):
                                key.data.rank = other_rank
                                connected[key.data.addr] = (other_rank, key.fileobj)
                            else:
                                # print(f"Rank {rank} got wrong authkey from {key.data.addr}. Ignore connection. {outer_authkey} != {authkey}")
                                pass
                    elif mask & selectors.EVENT_WRITE:
                        pass
                    else:
                        raise Exception('Got unexpected mask', mask, 'cannot happen.')

                if len(connected) == size - 1:
                    return Rootv3(sel, dict(connected), host, port, rank, size, debug)

        except KeyboardInterrupt:
            raise
    else:
        start_time = time.time()
        for trial in range(1000):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM).__enter__()

                if debug:
                    s = _Socket(s)

                try:
                    s.connect((host, port))
                    s.sendall(struct.pack(fmt, rank))
                    authenticate_client_side(s, authkey)
                except BaseException:
                    s.__exit__(None, None, None)
                    raise
                if trial >= 50:
                    info(f"{rank} Connected to {host}:{port} after {trial} trials in {time.time() - start_time:.2f}s")
                return Clientv3(s, host, port, rank, size, debug)
            except ConnectionRefusedError as e:
                if trial < 10:
                    time.sleep(0.01)
                elif trial < 20:
                    time.sleep(0.1)
                elif trial < 50:
                    if debug:
                        info(f"{rank} Could not connect to {host}:{port}. Try again in 1s")
                    time.sleep(1)
                elif trial < 100:
                    # This is serious, hence do the prints also without debug flag.
                    info(f"{rank} Could not connect to {host}:{port}. Try again in 10s")
                    time.sleep(10)
                else:
                    info(f"{rank} Could not connect to {host}:{port} after {time.time() - start_time:.2f}s. "
                         f"Giving up.")
                    raise


class _DoctestSocket:
    def __init__(self, other):
        self.other = other
        self.queue = b''

    def sendall(self, data):
        self.other.queue += data

    def recv(self, n):
        for i in range(100):
            if self.queue:
                data = self.queue[:n]
                self.queue = self.queue[len(data):]
                return data
            time.sleep(0.01)
        raise RuntimeError("Timeout")

    def recv_into(self, buffer, nbytes=None):
        data = self.recv(nbytes)
        buffer[:len(data)] = data
        return len(data)

    def close(self):
        pass


def _doctext_socket_pair():
    a = _DoctestSocket(None)
    b = _DoctestSocket(a)
    a.other = b
    return a, b


_AUTH_VERSION = 3


def authenticate_server_side(sock: socket.socket, authkey, version=_AUTH_VERSION):
    """
    Versions:
     - 1: Send authkey from client to server.
     - 2: Send a challenge from server to client. Client has to respond with a
          hash of the challenge and the authkey.
     - 3: Like 2, but additionally, the client has to send a challenge to the
          server. So the server knowns that the client has the correct authkey
          and the client knows that the server has the correct authkey.

    >>> import threading
    >>> # s1, s2 = _doctext_socket_pair()
    >>> s1, s2 = socket.socketpair(socket.AF_UNIX)
    >>> t = threading.Thread(target=authenticate_client_side, args=(s2, b'123', 1))
    >>> t.start()
    >>> authenticate_server_side(s1, b'123', 1)
    True
    >>> s1.close(), s2.close()
    (None, None)

    >>> import threading
    >>> # s1, s2 = _doctext_socket_pair()
    >>> s1, s2 = socket.socketpair(socket.AF_UNIX)
    >>> t = threading.Thread(target=authenticate_client_side, args=(s2, b'123', 2))
    >>> t.start()
    >>> authenticate_server_side(s1, b'123', 2)
    True
    >>> s1.close(), s2.close()
    (None, None)

    >>> import threading
    >>> # s1, s2 = _doctext_socket_pair()
    >>> s1, s2 = socket.socketpair(socket.AF_UNIX)
    >>> t = threading.Thread(target=authenticate_client_side, args=(s2, b'123', 3))
    >>> t.start()
    >>> authenticate_server_side(s1, b'123', 3)
    True
    >>> s1.close(), s2.close()
    (None, None)

    >>> # s1, s2 = _doctext_socket_pair()
    >>> s1, s2 = socket.socketpair(socket.AF_UNIX)
    >>> t = threading.Thread(target=authenticate_client_side, args=(s2, b'323', 1))
    >>> t.start()
    >>> authenticate_server_side(s1, b'123', 1)  # doctest: +ELLIPSIS
    Authkey mismatch for hostname '': b... != b...
    False
    >>> s1.close(), s2.close()
    (None, None)

    >>> # s1, s2 = _doctext_socket_pair()
    >>> s1, s2 = socket.socketpair(socket.AF_UNIX)
    >>> t = threading.Thread(target=authenticate_client_side, args=(s2, b'323', 2))
    >>> t.start()
    >>> authenticate_server_side(s1, b'123', 2)  # doctest: +ELLIPSIS
    Authkey mismatch for hostname '': b... != b...
    False
    >>> s1.close(), s2.close()
    (None, None)

    >>> # s1, s2 = _doctext_socket_pair()
    >>> s1, s2 = socket.socketpair(socket.AF_UNIX)
    >>> t = threading.Thread(target=authenticate_client_side, args=(s2, b'323', 3))
    >>> t.start()
    >>> authenticate_server_side(s1, b'123', 3)  # doctest: +ELLIPSIS
    Authkey mismatch for hostname '': b... != b...
    False
    >>> s1.close(), s2.close()
    (None, None)
    """
    if version == 1:
        outer_authkey = Mixin.recv_nbytes(sock, len(authkey))
        if authkey != outer_authkey:
            print(f'Authkey mismatch for hostname {sock.getpeername()!r}: {authkey} != {outer_authkey}')
            return False
        return True
    elif version == 2:
        challenge = os.urandom(16)
        sock.sendall(challenge)
        expected_response = hashlib.sha256(challenge + authkey).digest()
        response = Mixin.recv_nbytes(sock, len(expected_response))
        if response != expected_response:
            print(f'Authkey mismatch for hostname {sock.getpeername()!r}: {response} != {expected_response}')
            return False
        return True
    elif version == 3:
        c = Challenge(authkey, sock)
        if c.challenger():
            c.challenged()
            return True
        return False
    else:
        raise ValueError(f"Unknown auth version: {version}")


def authenticate_client_side(sock, authkey, version=_AUTH_VERSION):
    if version == 1:
        sock.sendall(authkey)
    elif version == 2:
        challenge = Mixin.recv_nbytes(sock, 16)
        response = hashlib.sha256(challenge + authkey).digest()
        sock.sendall(response)
    elif version == 3:
        c = Challenge(authkey, sock)
        c.challenged()
        if c.challenger():
            return True
        return False
    else:
        raise ValueError(f"Unknown auth version: {version}")


class Challenge:
    def __init__(self, authkey, sock):
        self.authkey = authkey
        self.sock = sock

    def challenger(self):
        challenge = os.urandom(16)
        response = hashlib.sha256(challenge + self.authkey).digest()

        self.sock.sendall(challenge)
        recv_response = Mixin.recv_nbytes(self.sock, len(response))
        if recv_response != response:
            print(f'Authkey mismatch for hostname {self.sock.getpeername()!r}: {recv_response} != {response}')
            return False
        return True

    def challenged(self):
        challenge = Mixin.recv_nbytes(self.sock, 16)
        response = hashlib.sha256(challenge + self.authkey).digest()
        self.sock.sendall(response)
