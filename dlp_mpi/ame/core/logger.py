import sys
import inspect
from pathlib import Path


def info(*args, color=True, frames=1):
    """

    >>> if sys.version_info < (3, 11):  # co_qualname was added in Python 3.11
    ...     import pytest
    ...     pytest.skip('Requires Python 3.11 or newer') 

    >>> foo()  # doctest: +ELLIPSIS
    logger.py:77 foo: debug message
    >>> Bar()()  # doctest: +ELLIPSIS
    logger.py:82 Bar.__call__: debug message
    >>> Bar().method_exception()  # doctest: +ELLIPSIS
    logger.py:88 Bar.method_exception: debug message
    >>> Bar.static_method()  # doctest: +ELLIPSIS
    logger.py:92 Bar.static_method: debug message
    >>> Bar.class_method()  # doctest: +ELLIPSIS
    logger.py:96 Bar.class_method: debug message
    """
    class c:
        red = '\033[91m'
        reset = '\033[0m'
        magenta = '\033[95m'
        cyan = '\033[96m'
        yellow = '\033[93m'
        green = '\033[92m'
        blue = '\033[94m'

    assert frames >= 1, frames

    frame = inspect.currentframe()

    prefix = ''
    msg = []
    for i in reversed(range(frames)):
        outer_frame = frame.f_back
        for _ in range(i):
            outer_frame = outer_frame.f_back
        outer_frame_info = inspect.getframeinfo(outer_frame)
        try:
            # added in py311 https://docs.python.org/3/whatsnew/changelog.html#changelog
            qualname = outer_frame.f_code.co_qualname
        except AttributeError:
            try:
                qualname = outer_frame.f_code.co_name
            except AttributeError:
                # ToDO: Find a the reason and a proper fix for:
                # AttributeError: 'code' object has no attribute 'co_qualname'. Did you mean: 'co_filename'?
                qualname = '???'
                # qualname = str(outer_frame.f_code)
                # <code object recv at 0x..., file ".../dlp_mpi/dlp_mpi/ame/core/con_v3.py", line 207>

        file = Path(outer_frame_info.filename).name
        file_color = {
            'core.py': c.cyan,
            'p2p.py': c.yellow,
            'con_v3.py': c.yellow,
        }.get(file, c.red)
        file = f'{file}:{outer_frame_info.lineno}'

        if color:
            file = f'{file_color}{file}{c.reset}'
            qualname = f'{c.magenta}{qualname}{c.reset}'

        if i == 0:
            msg.append(f'{prefix}{file} {qualname}: {" ".join(map(str, args))}')
        else:
            msg.append(f'{prefix}{file} {qualname}')
        prefix = f'{" "*len(prefix)} ⤷ '
    print(*msg, sep='\n')


def foo():
    info('debug message', color=False)


class Bar:
    def __call__(self):
        info('debug message', color=False)

    def method_exception(self):
        try:
            raise Exception
        except Exception:
            [info('debug message', color=False)]

    @staticmethod
    def static_method():
        info('debug message', color=False)

    @classmethod
    def class_method(cls):
        info('debug message', color=False)
