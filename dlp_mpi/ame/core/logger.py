import inspect
from pathlib import Path


def info(*args, color=True, frames=1):
    """
    >>> foo()  # doctest: +ELLIPSIS
    logger.py:... foo debug message
    >>> Bar()()  # doctest: +ELLIPSIS
    logger.py:... Bar.__call__ debug message
    >>> Bar.static_method()  # doctest: +ELLIPSIS
    logger.py:... Bar.static_method debug message
    >>> Bar.class_method()  # doctest: +ELLIPSIS
    logger.py:... Bar.class_method debug message
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
        qualname = outer_frame.f_code.co_qualname

        file = Path(outer_frame_info.filename).name
        file_color = {
            'core.py': c.cyan,
            'p2p.py': c.yellow,
        }.get(file, c.red)
        file = f'{Path(outer_frame_info.filename).name}:{outer_frame_info.lineno} {file}'

        if color:
            file = f'{file_color}{file}{c.reset}'
            qualname = f'{c.magenta}{qualname}{c.reset}'

        if i == 0:
            msg.append(f'{prefix}{file}::{qualname} {" ".join(map(str, args))}')
        else:
            msg.append(f'{prefix}{file}::{qualname}')
        prefix = f'{" "*len(prefix)} ⤷ '
    print(*msg, sep='\n')


def foo():
    info('debug message', color=False)


class Bar:
    def __call__(self):
        info('debug message', color=False)

    @staticmethod
    def static_method():
        info('debug message', color=False)

    @classmethod
    def class_method(cls):
        info('debug message', color=False)
