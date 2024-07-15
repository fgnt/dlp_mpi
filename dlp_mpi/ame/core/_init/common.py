import os
import socket
import base64

from ...constants import AUTHKEY_LENGTH


def find_free_port():
    # https://stackoverflow.com/a/45690594/5766934
    import socket
    from contextlib import closing

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def get_host_and_port():
    """
    Return a host and port that can be used by the root process.
    This function should never be called by a non-root process.
    """
    # host = '127.0.0.1'
    host = socket.gethostname()
    port = find_free_port()
    return host, port


def get_authkey(force_random=False):
    """
    Generate a random 32-byte authentication key.

    This key will be used to authenticate the connection between the
    root process and the non-root processes.

    Note: Your communication will not be encrypted and it is assumed, that
          the program is running in a secure environment.
          It protects mainly against accidental connections from other
          programs.

    >>> len(get_authkey()), type(get_authkey())
    (64, <class 'bytes'>)
    >>> os.environ['AME_AUTHKEY'] = 'test'
    >>> len(get_authkey()), type(get_authkey())
    (3, <class 'bytes'>)
    >>> del os.environ['AME_AUTHKEY']
    """
    if not force_random and 'AME_AUTHKEY' in os.environ:
        authkey = os.environ['AME_AUTHKEY']
        return authkey_encode(authkey)
    return os.urandom(AUTHKEY_LENGTH)


def str_to_authkey(s):
    """
    >>> len(str_to_authkey('test')), type(str_to_authkey('test'))
    (64, <class 'bytes'>)
    """
    # use md5 to hash the string to a 32-byte key
    import hashlib
    m = hashlib.sha512()
    m.update(s.encode('utf-8'))
    authkey = m.digest()
    assert len(authkey) == AUTHKEY_LENGTH, (len(authkey), AUTHKEY_LENGTH)
    return authkey


def authkey_decode(authkey):
    """
    bytes to str

    >>> authkey_decode(b'test'*16)
    'dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdA=='
    """
    assert len(authkey) == AUTHKEY_LENGTH, (len(authkey), AUTHKEY_LENGTH)
    return base64.b64encode(authkey).decode('utf-8')


def authkey_encode(authkey):
    """
    str to bytes

    >>> authkey_encode('dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdA==')
    b'testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttest'
    """
    authkey = base64.b64decode(authkey.encode('utf-8'))
    assert len(authkey) == AUTHKEY_LENGTH, (len(authkey), AUTHKEY_LENGTH)
    return authkey
