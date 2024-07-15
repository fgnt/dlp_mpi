"""

"""

import sys
import subprocess
import argparse
import os
import textwrap

DEBUG = False


def launch():
    workers = []

    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument('-n', '-np', type=int, dest='SIZE', default=1)
    parser.add_argument('--pty', type=int, dest='pty', metavar='RANK', default=None,
                        help="Connects the stdin of the process with the given "
                             "RANK to the terminal, enabling interactive "
                             "input (or debugging). If omitted, no process "
                             "will have interactive input.")
    parser.add_argument('--debug', action='store_true', dest='debug', default=DEBUG)
    parser.add_argument('workload', nargs=argparse.REMAINDER)

    # parse_known_args may consume program args
    # args, workload = parser.parse_known_args()

    args = parser.parse_args()
    size = args.SIZE
    debug = args.debug
    pty = args.pty
    workload = args.workload

    # os.execlp('python', 'python', 'my_script.py')

    if not workload:
        raise ValueError("no executable provided", sys.argv)

    from ..core._init.common import get_host_and_port, get_authkey, authkey_decode
    host, port = get_host_and_port()

    os.environ['AME_SIZE'] = str(size)
    os.environ['AME_PORT'] = str(port)
    os.environ['AME_HOST'] = str(host)
    os.environ['AME_AUTHKEY'] = authkey_decode(get_authkey(force_random=True))

    for rank in range(size):
        workers.append(subprocess.Popen(
            [*workload],
            env=dict(os.environ, AME_RANK=str(rank)),
            stdout=subprocess.PIPE if debug else None,
            stderr=subprocess.PIPE if debug else None,
            stdin=subprocess.DEVNULL if pty != rank else None,

            # according to https://stackoverflow.com/questions/33277452/prevent-unexpected-stdin-reads-and-lock-in-subprocess
            # helps start_new_session to disable stdin
            # start_new_session=True if pty != rank else False,
        ))

    def wait_for_workers():
        for i, worker in enumerate(workers):
            if debug:
                stdout, stderr = worker.communicate()
                print(f"Worker {i} finished with {worker.returncode}")
                if stdout:
                    print(" stdout")
                    print(textwrap.indent(stdout.decode(), " | "))
                if stderr:
                    print(" stderr")
                    print(textwrap.indent(stderr.decode(), " | "))
            else:
                worker.wait()

    try:
        wait_for_workers()
    except KeyboardInterrupt:
        wait_for_workers()

    if debug:
        print("All workers finished")


if __name__ == '__main__':
    launch()
