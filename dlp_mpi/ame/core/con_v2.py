import json
import pickle
import struct
import socket
import selectors
import types
import asyncio
import time

from paderbox.utils.mapping import Dispatcher

from ..constants import *
from .logger import info
# from ame.core.core import TagError, tag_match

__all__ = [
    # 'P2P',
    'PickleBackend',
    'JsonBackend',
    # 'ABCBackend',
]


class TagError(Exception):
    pass

def tag_match(got, expected):
    """
    >>> tag_match(1, 1)
    True
    >>> tag_match(1, 2)
    False
    >>> tag_match(1, ANY_TAG)
    True
    >>> tag_match(BCAST_TAG, ANY_TAG)
    False
    """
    assert got != ANY_TAG, got
    if expected == ANY_TAG and got >= 0:
        return True
    return expected == got


if DEBUG:
    def _debug_wait_for(aw):
        return asyncio.wait_for(aw, 10 + 3 * (SIZE - RANK))
else:
    def _debug_wait_for(aw):
        return aw


class TrueFalseEvent:
    def __init__(self):
        self._event_true = asyncio.Event()
        self._event_false = asyncio.Event()
        self._event_false.set()

    def set(self):
        self._event_true.set()
        self._event_false.clear()

    def clear(self):
        self._event_true.clear()
        self._event_false.set()

    async def wait_true(self):
        await self._event_true.wait()

    async def wait_false(self):
        await self._event_false.wait()

class c:
    reset = '\033[0m'
    magenta = '\033[35m'
    cyan = '\033[36m'

class P2Pv2:
    """
    Idea for recv:
     - The buffer_task runs in the background and fills the buffer with the next tag.
     - The user can check if the buffer is ready with the correct tag by using the ready() method.
     - The user can then call recv() to get the data.
    Motivation:
    The buffer task runs in the background and reads the tag from the message.
    Once the tag is known, we know, that another RANK wants to send data.
    The recv has to solve two problems:
     - It may want to recv data from any rank, so it want to know the first
       process that is ready. If another P2P was faster, the data should be
       kept for the next recv call.
     - The tag has to match the expected tag.

    This class is used as follows:
     - The buffer tasks runs always.
     - The ready() is called and might be canceled.
     - The recv() is called and is not allowed to be canceled.
       i.e. the data has to be used.

    """
    def __init__(self, reader, writer, depth, rank, backend='json', debug=False):
        self.reader: asyncio.StreamReader = reader
        self.writer: asyncio.StreamWriter = writer

        self.depth = depth
        self.rank = rank
        self.debug = debug

        self.buffer = None

        self.backend = backend

        self._shutdown = False

        self._data_ready = asyncio.Event()
        self._load_data = asyncio.Event()

        # self._buffer_task = None
        self._buffer_task = asyncio.create_task(self.buffer_task())
        # The garbage collector (GC) sometimes closes the loop, before the del
        # is called. Additionally, it looks like the __del__ might be called,
        # while a coroutine is running. So supress the warning and errors:
        #    Disable the logging and let the GC do its job, instead of a clean
        #    shutdown, which is impossible without a context manager, which
        #    cannot be used to stay compatible with the MPI interface.
        self._buffer_task._log_destroy_pending = False

    @property
    def backend(self):
        return self._backend

    @backend.setter
    def backend(self, value):
        if value == 'pickle': self._backend = PickleBackend()
        elif value == 'json': self._backend = JsonBackend()
        else: raise TypeError(value)

    async def shutdown(self):
        if self.debug:
            info()

        self._shutdown = True
        if self._buffer_task:
            if self.debug:
                info('cancel buffer task')
            # self._load_data.set()
            self._buffer_task.cancel()
            await self._buffer_task
            if self.debug:
                info('buffer task canceled')

        self.writer.close()

    def __del__(self):
        # pass
        assert self.buffer is None, 'Impl error'
        # info(f'{self.depth} {self.rank}')

        # if self._buffer_task:
        #     if self.debug:
        #         info('cancel buffer task')
        #     # self._load_data.set()
        #     self._buffer_task.cancel()
        #     # await self._buffer_task
        #     if self.debug:
        #         info('buffer task canceled')

        # self.writer.close()

    def to_idle(self):
        assert not self._load_data.is_set(), 'timing issue'
        self._shutdown = True
        self._load_data.set()

    async def buffer_task(self):
        while True:
            # await self._data_ready.wait_false()
            try:
                await self._load_data.wait()
            except asyncio.CancelledError:
                return
            self._load_data.clear()

            if self._shutdown:
                # self._shutdown = False
                # self._buffer_task = None
                return

            try:
                header = await _debug_wait_for(self.reader.read(8))
            except asyncio.CancelledError:
                return
            if not header:
                return
                # raise EOFError(f'{self.depth} EOF', header)

            length, msg_tag = struct.unpack('ii', header)
            # data = await _debug_wait_for(self.reader.read(length))
            assert self.buffer is None, (self.buffer, 'Impl error')
            self.buffer = length, msg_tag

            assert self._data_ready.is_set() is False, 'Impl error'

            # self._buffer_task = None
            self._data_ready.set()

    async def ready(self, tag):
        assert self._load_data.is_set() is False, 'Impl error'
        self._load_data.set()
        # if self._buffer_task is None:
        #     self._buffer_task = asyncio.create_task(self.buffer_task())
        #     self._buffer_task.__log_destroy_pending = False

        await self._data_ready.wait()
        if tag_match(self.buffer[1], tag):
            return True
        else:
            return False

    async def get_data(self, tag=ANY_TAG, status=None):
        length, msg_tag = self.buffer

        if self.debug:
            info(f'{self.depth} from {self.rank} with tag {msg_tag}')
        data = await _debug_wait_for(self.reader.read(length))
        if self.debug:
            info(f'{self.depth} from {self.rank} with tag {msg_tag} data: {data}')
        self.buffer = None
        self._data_ready.clear()

        if status:
            status.source = self.rank
            status.tag = msg_tag

        assert tag_match(msg_tag, tag), (msg_tag, tag, 'Tag mismatch. This is a critical issue, use ready() to check the tag before calling recv()')
        data = self.backend._loads(data)
        if self.debug:
            info(f'{self.depth} from {self.rank} with tag {msg_tag} data: {data} status: {status}')
        return data

    async def recv(self, tag=ANY_TAG, status=None):
        if await self.ready(tag):
            if self.debug:
                info(f'{self.depth} from {self.rank}')
            data = await self.get_data(tag, status=status)
            if self.debug:
                info(f'{self.depth} from {self.rank} data: {data} status: {status}')
            return data
        else:
            assert self.buffer is not None, 'Impl error'
            raise TagError(f'{self.depth} Expected tag {tag}, got {self.buffer[1]}')

    async def send(self, data, tag):
        # print(f'{self.depth} {c.cyan}send{c.reset} {self._decode(data)} to {self.rank} with tag {tag}')

        data = self.backend._dumps(data)

        assert tag != ANY_TAG, tag
        header = struct.pack(
            'ii',
            len(data),  # MPI is also limited to 2GB
            tag,
        )
        self.writer.write(header)
        self.writer.write(data)
        try:
            await _debug_wait_for(self.writer.drain())
        except ConnectionResetError:
            raise ConnectionResetError(f'{self.depth} data: {data} ({pickle.loads(data)}) tag: {tag}, rank: {self.rank}')




# class P2P:
#     def __init__(self, reader, writer, depth, rank, backend='json'):
#         self.reader: asyncio.StreamReader = reader
#         self.writer: asyncio.StreamWriter = writer
#         self.buffer = None
#         self.depth = depth
#         self.rank = rank
#
#         self._buffer_task = None
#
#         if backend == 'pickle':
#             self.backend = PickleBackend()
#         elif backend == 'json':
#             self.backend = JsonBackend()
#         else:
#             raise TypeError(backend)
#
#     def _decode(self, data):
#         try:
#             return f'pickle({pickle.loads(data)!r})'
#         except pickle.UnpicklingError:
#             pass
#         try:
#             return f'json({json.loads(data)!r})'
#         except json.JSONDecodeError:
#             pass
#         return f'{data!r}'
#
#     async def send(self, data, tag):
#         # print(f'{self.depth} {c.cyan}send{c.reset} {self._decode(data)} to {self.rank} with tag {tag}')
#
#         data = self.backend._dumps(data)
#
#         assert tag != ANY_TAG, tag
#         header = struct.pack(
#             'ii',
#             len(data),  # MPI is also limited to 2GB
#             tag,
#         )
#         self.writer.write(header)
#         self.writer.write(data)
#         try:
#             await _debug_wait_for(self.writer.drain())
#         except ConnectionResetError:
#             raise ConnectionResetError(f'{self.depth} data: {data} ({pickle.loads(data)}) tag: {tag}, rank: {self.rank}')
#
#     async def set_buffer(self):
#         header = await _debug_wait_for(self.reader.read(8))
#         if not header:
#             self._buffer_task = None
#             return
#             # raise EOFError(f'{self.depth} EOF', header)
#
#         length, msg_tag = struct.unpack('ii', header)
#         data = await _debug_wait_for(self.reader.read(length))
#         self.buffer = data, msg_tag
#
#     async def recv(self, tag=ANY_TAG):
#         if self.buffer is None:
#             if self._buffer_task is not None and self._buffer_task.done():
#                 await self._buffer_task
#                 self._buffer_task = None
#             if self._buffer_task is None:
#                 self._buffer_task = asyncio.create_task(self.set_buffer())
#             try:
#                 # "shield" the task to prevent it from being cancelled.
#                 # The cancel happens, when the user used recv with
#                 # ANY_SOURCE and another rank was faster to send the data.
#                 await asyncio.shield(self._buffer_task)
#             except asyncio.CancelledError:
#                 raise asyncio.CancelledError(self._buffer_task)
#                 raise TagError('Cancelled')
#             except EOFError:
#                 self._buffer_task = None
#                 raise TagError('EOF')
#             self._buffer_task = None
#
#         if self.buffer is None:
#             raise TagError('Other side closed the socket')
#
#         data, msg_tag = self.buffer
#         self.buffer = None
#
#         if not tag_match(msg_tag, tag):
#                 self.buffer = data, msg_tag
#                 raise TagError(f'{self.depth} Expected tag {tag}, got {msg_tag}')
#
#         # print(f'{self.depth} {c.magenta}recv{c.reset} {self._decode(data)!r} from {self.rank} with tag {msg_tag}')
#         return self.backend._loads(data), msg_tag


class ABCBackend:
    """
    This class implements the actual communication between two processes.

    """
    def __init__(self, reader, writer, depth, rank):
        self.reader = reader
        self.writer = writer
        self.depth = depth
        self.rank = rank

        self.p2p = P2P(reader, writer, depth=depth, rank=rank)

    def _loads(self, data):
        raise NotImplementedError

    def _dumps(self, data):
        raise NotImplementedError

    async def dump(self, data, tag):
        # if _DEBUG:
        #     print(f'{self.__class__.__name__} (D{self.depth}, R{RANK}):  dump {data!r} T{tag}', flush=True)

        data = self._dumps(data)
        await self.p2p.send(data, tag)

        # header = struct.pack(
        #     'ii',
        #     len(data),  # MPI is also limited to 2GB
        #     tag,
        # )
        # self.writer.write(header)
        # self.writer.write(data)
        # await self.wait_for(self.writer.drain())

        # if _DEBUG:
        #     print(f'{self.__class__.__name__} (D{self.depth}, R{RANK}):  dumped {data!r} T{tag}', flush=True)

    # class Loader:
    #     def __init__(self, tag, deserializer, reader):
    #         self.tag = tag
    #         self.trigger = asyncio.Event()
    #         self.deserializer = deserializer
    #         self.reader = reader
    #
    #     @property
    #     def tag(self):
    #         return self._tag
    #
    #     @tag.setter
    #     def tag(self, value):
    #         self._tag = value
    #         self.trigger.set()
    #
    #     async def load(self):
    #         header = await _debug_wait_for(self.reader.read(8))
    #         if not header:
    #             raise EOFError('EOF', header)
    #             # raise RuntimeError('EOF', header)
    #
    #         length, tag = struct.unpack('ii', header)
    #         data = await _debug_wait_for(self.reader.read(length))
    #
    #         return self.deserializer(data), tag
    #
    #     async def get(self):
    #         self.trigger.clear()
    #         obj, tag = await self.load()
    #
    #         assert tag != ANY_TAG, tag
    #
    #         if self.tag != ANY_TAG:
    #             if tag != self.tag:
    #                 await self.trigger.wait()
    #                 self.trigger.clear()

    async def load(self, tag=ANY_TAG):
        # if _DEBUG:
        #     print(f'{self.__class__.__name__} (D{self.depth}, R{RANK}):  load', flush=True)

        self.p2p.recv_tag = tag
        data, tag = await self.p2p.recv()

        # header = await self.wait_for(self.reader.read(8))
        # if not header:
        #     raise EOFError('EOF', header)
        #     # raise RuntimeError('EOF', header)
        #
        # length, tag = struct.unpack('ii', header)
        # data = await self.wait_for(self.reader.read(length))

        # if _DEBUG:
        #     print(f'{self.__class__.__name__} (D{self.depth}, R{RANK}):  loaded {data!r} T{tag}', flush=True)

        return self._loads(data), tag


class PickleBackend:#(ABCBackend):
    def _loads(self, data):
        return pickle.loads(data)

    def _dumps(self, data):
        return pickle.dumps(data)


class JsonBackend:#(ABCBackend):
    def _loads(self, data):
        data = data.decode()
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            raise ValueError(f'Could not decode {data}')

    def _dumps(self, data):
        return json.dumps(data).encode()



async def establish_connection(
        # runner,
        *,
        depth,
        host=_HOST,  # IP of rank 0
        port=_PORT,  # port of rank 0
        rank=RANK,  # rank of this process
        size=SIZE,
        debug=DEBUG,
):
    rank_to_p2p = Dispatcher()
    if rank == 0:
        finished_setup = asyncio.Event()

        async def handle_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            if debug:
                info(f'{depth} Connection from {writer.get_extra_info("peername")}')

            assert len(rank_to_p2p) < size, f'All ranks already connected. {rank_to_p2p}'

            # rank = await reader.read(1024)
            # # Always use json for first message. That minimizes the security risk.
            p2p = P2Pv2(reader, writer, depth=depth, rank='unknown', debug=debug)
            if debug:
                info(f'{depth} from {rank} data: Created P2Pv2 object')
            other_rank = await p2p.recv()
            if debug:
                info(f'{depth} from {rank} data: Received {other_rank}')
            addr = writer.get_extra_info('peername')

            if debug:
                info(f'{depth} from {rank} data: Received {other_rank} from {addr}')

            assert other_rank not in rank_to_p2p, f'Rank {other_rank} already exists. {rank_to_p2p}'
            p2p.backend = 'pickle'
            rank_to_p2p[other_rank] = p2p

            if len(rank_to_p2p) == size - 1:
                if debug:
                    info(f'{depth} All ranks connected')
                finished_setup.set()

        task = None
        # async def setup():
        if size > 1:
            # nonlocal task
            server = await asyncio.start_server(
                handle_request, host, port)

            addr = server.sockets[0].getsockname()
            if debug:
                info(f'Serving on {addr} ({depth})')

            task = asyncio.create_task(server.serve_forever())

            await finished_setup.wait()
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass
                # print("main(): cancel_me is cancelled now")

            # return server

        # if size > 1:
        #     runner.run(setup())
            # runner.run(finished_setup.wait())

    else:
        # async def connect():
        i = 0
        while True:
            i += 1
            try:
                reader, writer = await asyncio.open_connection(host, port)
                p2p = P2Pv2(reader, writer, depth=depth, rank='unknown (actually 0)', debug=debug)
                await p2p.send(rank, 0)
                # writer.write(JsonBackend().dumps(RANK))
                if debug:
                    info(f'Connected to server from {rank}. ({depth})')

                # data = await reader.read(100)
                # print(f'Received: {backend.loads(data)}')

                p2p.backend = 'pickle'
                rank_to_p2p[0] = p2p
                break
            except ConnectionRefusedError:
                # 1h -> 3600 s / 0.1 ms = 36000
                # 5 min -> 300 s / 0.1 ms = 3000
                if i > 3000:
                    raise
                if i > 10:
                    info(f'Failed to connect, retrying in 0.1s ({depth})')
                await asyncio.sleep(0.1)
                continue

        # rank_to_p2p[0] = runner.run(connect()) #  P2P(*runner.run(connect()), depth=depth, rank=0, backend=backend)

    return rank_to_p2p
