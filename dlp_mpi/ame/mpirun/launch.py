"""
ToDO: Internode communication.

"""

import sys
import subprocess
from pathlib import Path
import argparse
import os
import textwrap
import io

DEBUG = False


def launch():
    stdouterr = {}

    workers = []
    if Path(__file__).stem == "launch":

        if DEBUG:
            print("Launching AME Root")

        parser = argparse.ArgumentParser(add_help=True)
        parser.add_argument('-n', '-np', type=int, dest='SIZE', default=1)
        args, workload = parser.parse_known_args()
        size = args.SIZE

        # os.execlp('python', 'python', 'my_script.py')

        if not workload:
            raise ValueError("no executable provided", sys.argv)

        from ame._init.common import get_host_and_port
        host, port = get_host_and_port()

        os.environ['AME_SIZE'] = str(size)
        os.environ['AME_PORT'] = str(port)
        os.environ['AME_HOST'] = str(host)
        for rank in range(1, size):
            workers.append(subprocess.Popen(
                ['python', '-m', 'ame.worker', *workload],
                env=dict(os.environ, AME_RANK=str(rank)),
                stdout=subprocess.PIPE if DEBUG else None,
                stderr=subprocess.PIPE if DEBUG else None,
                # start_new_session=True,
            ))
        os.environ['AME_RANK'] = '0'
    elif Path(__file__).stem == "worker":
        if DEBUG:
            print("Launching AME Worker")
        workload = sys.argv[1:]
    else:
        raise NotImplementedError("Unknown launch mode", __file__.name)

    try:
        cp = subprocess.run(
            [*workload],
            env=dict(os.environ),
            stdout=subprocess.PIPE if DEBUG else None,
            stderr=subprocess.PIPE if DEBUG else None,
            check=False,
        )
    except KeyboardInterrupt:
        class Dummy:
            returncode = 'KeyboardInterrupt'
        cp = Dummy()
    if cp.returncode != 0:
        print(f"RANK {os.environ['AME_RANK']} failed with {cp.returncode}")
    else:
        if DEBUG:
            print(f"RANK {os.environ['AME_RANK']} finished.")

    if DEBUG:
        print(f"Worker {0} finished with {cp.returncode}")
        if cp.stdout:
            print(" stdout")
            print(textwrap.indent(cp.stdout.decode(), " | "))
        if cp.stderr:
            print(" stderr")
            print(textwrap.indent(cp.stderr.decode(), " | "))

    if workers:
        for i, worker in enumerate(workers, start=1):
            if DEBUG:
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

        if DEBUG:
            print("All workers finished")


if __name__ == '__main__':
    launch()


