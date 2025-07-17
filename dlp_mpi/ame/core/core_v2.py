import functools
import os
import asyncio
import json
import pickle


from dlp_mpi.ame.constants import *
from dlp_mpi.ame.core.con_v2 import P2Pv2, establish_connection
from dlp_mpi.ame.core.logger import info
# from ame.core.con import establish_connection_v3


# backend = 'json'
backend = 'pickle'

__all__ = [
    'COMM_WORLD',
    # 'RANK',
    # 'SIZE',
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



class Communicator_v2:
    # size = SIZE
    # rank = RANK
    # Barrier = lambda self: None
    # bcast = lambda self, data, *args, **kwargs: data
    # gather = lambda self, data, *args, **kwargs: [data]
    # Clone = lambda self: self

    def __init__(
            self,
            *,
            rank,
            size,
            port,
            host,
            _runner=None,
            _depth=0,
            debug=DEBUG,
    ):
        self.rank = rank
        self.size = size

        self._debug = debug
        self._host = host
        self._port = port

        self._depth = _depth
        if _runner is None:
            from ame.backport_runner import Runner
            self._runner = Runner()
            self._runner.__enter__()
        else:
            self._runner = _runner
        info(f'{self._depth} Runner: {self._runner}, {id(self._runner)} {RANK}')

        if self.size > 1:
            self._rank_to_p2p = self._runner.run(
                establish_connection(
                    depth=self._depth, rank=self.rank, size=self.size, host=self._host, port=self._port,
                    debug=self._debug,
                )
            )
            self._loop = self._runner.get_loop()
            info(f'{self._depth} Loop: {self._loop}')
        else:
            self._rank_to_p2p = {}

        assert self.rank < self.size, (self.rank, self.size)

    def __del__(self):
        # if self._debug:
        #     info(f'{self._depth} __del__')

        pass

        # if hasattr(self, '_rank_to_p2p'):

            # for k, v in self._rank_to_p2p.items():
            #     v.writer.close()

        #     async def shutdown_p2p():
        #         for k, v in self._rank_to_p2p.items():
        #             await v.shutdown()
        #     # self._runner._loop.create_task(shutdown_p2p())
        #
        #
        #
        #     info(f'{self._depth} Loop: {self._runner._loop}, {self._runner}, {RANK}, {id(self._runner)}, {id(self._runner._loop)}')
        #     # if self._runner._loop is not None:
        #     if self._runner._loop.is_closed():
        #         info(f'Issue: Loop is closed')
        #
        #         # for k, v in self._rank_to_p2p.items():
        #         #     if v._buffer_task:
        #         #         v._buffer_task._log_destroy_pending = False
        #         # Process shutting down. The loop is already closed,
        #         # hence we cannot run the shutdown_p2p coroutine.
        #
        #         # for k, v in self._rank_to_p2p.items():
        #         #     self._shutdown = True
        #         #     if v._buffer_task:
        #         #         v._buffer_task.cancel()
        #         #         # await self._buffer_task
        #         #     v.writer.close()
        #     else:
        #         pass
        #         # pass
        #         # asyncio.create_task()
        #
        #         # self._runner.run(shutdown_p2p())
        #
        # if self._debug:
        #     info(f'{self._depth} __del__ finished')

    def clone(self):
        return self.__class__(
            rank=self.rank,
            size=self.size,
            port=self._port,
            host=self._host,
            _runner=self._runner,
            _depth=self._depth + 1,
        )

    def send(self, obj, dest, tag=0):
        self._runner.run(self._send(obj, dest, tag))

    async def _send(self, obj, dest, tag=0):
        assert dest == 0 or self.rank == 0, f'Communication only supported with rank 0, not {self.rank} to {dest}'
        await self._rank_to_p2p[dest].send(obj, tag)

    def recv(self, source=ANY_SOURCE, tag=ANY_TAG, status=None):
        return self._runner.run(self._recv(source=source, tag=tag, status=status))

    async def _recv(self, source, tag=ANY_TAG, status=None):
        if source == ANY_SOURCE:
            async def task_fn(i):
                return i, await self._rank_to_p2p[i].ready(tag=tag)

            tasks = [
                asyncio.create_task(task_fn(i))
                for i in range(0, self.size)
                if i != self.rank
            ]
            while tasks:
                finished, unfinished = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED)

                for x in finished:
                    i, result = x.result()

                    if result:
                        # cancel the other tasks, we have a result. We need to wait for the cancellations
                        # to propagate.
                        for task in unfinished:
                            task.cancel()
                        await asyncio.wait(unfinished)

                        return await self._rank_to_p2p[i].get_data(tag=tag,
                                                                   status=status)

                tasks = unfinished
            raise RuntimeError(
                f'Go no message with the requested tag: {tag}. Got ')
        else:
            return await self._rank_to_p2p[source].recv(tag=tag)

    def bcast(self, obj, root=0):
        return self._bcast(obj, root)

    def _bcast(self, obj, root=0, tag=BCAST_TAG):
        assert root == 0 or self.rank == 0, f'Communication only supported with rank 0, not {self.rank} to {root}'
        if self.rank == root:
            async def dummy_fn():
                await asyncio.gather(*[
                    self._send(obj, i, tag)
                    for i in range(1, self.size)
                ])

            self._runner.run(dummy_fn())
            return obj
        else:
            return self.recv(root, tag=tag)

    def gather(self, obj, root=0):
        return self._gather(obj, root)

    def _gather(self, obj, root=0, tag=GATHER_TAG):
        assert root == 0 or self.rank == 0, f'Communication only supported with rank 0, not {self.rank} to {root}'
        if self.rank == root:

            async def dummy_fn():
                ret = await asyncio.gather(*[
                    self._recv(i, tag=tag)
                    for i in range(1, self.size)
                    if i != root
                ])
                if self._debug:
                    info(f'{self._depth} {ret}')
                ret.insert(root, obj)
                return ret
            return self._runner.run(dummy_fn())
        else:
            self.send(obj, root, tag)
            return None

    def barrier(self):
        self._bcast(None, tag=BARRIER_TAG)
        self._gather(None, tag=BARRIER_TAG)

    Barrier = barrier
    Clone = clone



Communicator = Communicator_v2

# try:
#     COMM_WORLD = Communicator(rank=RANK, size=SIZE)
#     COMM = COMM_WORLD
# except Exception:
#     raise RuntimeError(RANK, SIZE)
