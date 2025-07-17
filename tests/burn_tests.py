import subprocess
from pathlib import Path
import tempfile
import shlex
import pytest
import os
import sys


examples_folder = Path(__file__).parent.parent / 'examples'


if sys.version_info < (3, 11):
    class CalledProcessError(subprocess.CalledProcessError):
        __notes__ = None

        def add_note(self, note):
            if self.__notes__ is None:
                self.__notes__ = []
            self.__notes__.append(note)
        def __str__(self):
            msg = super().__str__()
            if self.__notes__:
                msg += '\n' + '\n'.join(self.__notes__)
            return msg

def run(cmd, cwd=None, check=True, **kwargs):
    """Run a command in a subprocess."""
    try:
        return subprocess.run(cmd, shell=isinstance(cmd, str), check=check, capture_output=True, text=True, cwd=cwd, universal_newlines=True,
                              **kwargs)
    except subprocess.CalledProcessError as e:
        if not hasattr(e, 'add_note'):
            e.__class__ = CalledProcessError  # type: ignore
        e.add_note(f'\n\nstdout: {e.stdout}\nstderr: {e.stderr}')
        raise


def test():
    result = run(f"mpiexec --oversubscribe -np 2 {sys.executable} -c 'import dlp_mpi; print(dlp_mpi.RANK)'")
    assert result.stdout in ['0\n1\n', '1\n0\n'], f"Unexpected output: {result.stdout}"


def exec_code(code, size=2, backend='ame'):
    """
    Execute code with mpiexec -np <size> python ....

    Catch the stdout of each rank and return it, ordered by RANK.

    """
    env = os.environ.copy()
    env['DLP_MPI_BACKEND'] = backend
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        (tmpdir / 'code.py').write_text(code)
        try:
            # --oversubscribe: mpiexec fails, if the number of physical cores is less than SIZE.
            #                  With this option it will run anyway.
            run(["mpiexec", "--oversubscribe", "-np", f"{size}", "bash", "-c", f"{sys.executable} {tmpdir / 'code.py'} > {tmpdir / '$OMPI_COMM_WORLD_RANK.txt'}"],
                cwd=tmpdir, env=env)
        except subprocess.CalledProcessError as e:
            if not hasattr(e, 'add_note'):
                e.__class__ = CalledProcessError  # type: ignore
            for rank in range(size):
                try:
                    e.add_note(f'\nOutput from rank {rank}:\n' + (tmpdir / f"{rank}.txt").read_text() + '\n')
                except FileNotFoundError:
                    e.add_note(f"\nOutput file for rank {rank} not found.\n")

            raise

        assert set(tmpdir.glob("*.txt")) == {tmpdir / f"{rank}.txt" for rank in range(size)}, f"Expected {size} output files, found: {list(tmpdir.glob('*.txt'))}"
        out = []
        for rank in range(size):
            out.append((tmpdir / f"{rank}.txt").read_text())
        return out


def mpi_1(backend, size=2):
    """
    >>> mpi_1('mpi4py')
    rank=0, size=2
    <BLANKLINE>
    rank=1, size=2
    <BLANKLINE>
    >>> mpi_1('ame')
    rank=0, size=2
    <BLANKLINE>
    rank=1, size=2
    <BLANKLINE>
    """
    for output in exec_code((examples_folder / 'mpi_1_rank_and_size.py').read_text(), size=size, backend=backend):
        print(output)


def mpi_2(backend, size=2):
    """
    >>> mpi_2('mpi4py')
    rank=0, size=2, data={'key1': [7, 2.72, (2+3j)], 'key2': ('abc', 'xyz')}
    <BLANKLINE>
    rank=1, size=2, data={'key1': [7, 2.72, (2+3j)], 'key2': ('abc', 'xyz')}
    <BLANKLINE>
    >>> mpi_2('ame')
    rank=0, size=2, data={'key1': [7, 2.72, (2+3j)], 'key2': ('abc', 'xyz')}
    <BLANKLINE>
    rank=1, size=2, data={'key1': [7, 2.72, (2+3j)], 'key2': ('abc', 'xyz')}
    <BLANKLINE>
    """
    for output in exec_code((examples_folder / 'mpi_2_broadcast_data.py').read_text(), size=size, backend=backend):
        print(output)


def mpi_3(backend, size=2):
    """
    >>> mpi_3('mpi4py')
    rank=0, size=2, data=['hello', 'world']
    <BLANKLINE>
    rank=1, size=2, data=None
    <BLANKLINE>
    >>> mpi_3('ame')
    rank=0, size=2, data=['hello', 'world']
    <BLANKLINE>
    rank=1, size=2, data=None
    <BLANKLINE>
    """
    for output in exec_code((examples_folder / 'mpi_3_gather_data.py').read_text(), size=size, backend=backend):
        print(output)


def mpi_4(backend, size=2):
    """
    >>> mpi_4('mpi4py')
    rank=0, size=2, data=10
    rank=0, size=2, data=12
    job splits: [[20, 24], [22, 26]]
    flat result:  [20, 24, 22, 26]
    <BLANKLINE>
    rank=1, size=2, data=11
    rank=1, size=2, data=13
    <BLANKLINE>
    >>> mpi_4('ame')
    rank=0, size=2, data=10
    rank=0, size=2, data=12
    job splits: [[20, 24], [22, 26]]
    flat result:  [20, 24, 22, 26]
    <BLANKLINE>
    rank=1, size=2, data=11
    rank=1, size=2, data=13
    <BLANKLINE>
    """
    for output in exec_code((examples_folder / 'mpi_4_scatter_gather.py').read_text(), size=size, backend=backend):
        print(output)


def mpi_5(backend, size=2):
    """

    >>> mpi_5('mpi4py')
    ### Unordered map scattered around processes:
    ['h', 'e', 'l', 'l', 'o']
    ### Map function run only on master:
    0 0 h
    0 1 e
    0 2 l
    0 3 l
    0 4 o
    ['h', 'e', 'l', 'l', 'o']
    <BLANKLINE>
    1 0 h
    1 1 e
    1 2 l
    1 3 l
    1 4 o
    <BLANKLINE>
    >>> mpi_5('ame')
    ### Unordered map scattered around processes:
    ['h', 'e', 'l', 'l', 'o']
    ### Map function run only on master:
    0 0 h
    0 1 e
    0 2 l
    0 3 l
    0 4 o
    ['h', 'e', 'l', 'l', 'o']
    <BLANKLINE>
    1 0 h
    1 1 e
    1 2 l
    1 3 l
    1 4 o
    <BLANKLINE>
    """
    for output in exec_code((examples_folder / 'mpi_5_map_unordered.py').read_text(), size=size, backend=backend):
        print(output)


def mpi_5_3(backend, size=3):
    """
    >>> mpi_5_3('mpi4py')
    >>> mpi_5_3('ame')
    """
    outputs = exec_code((examples_folder / 'mpi_5_map_unordered.py').read_text(), size=size, backend=backend)

    for expected in [
            ' 0 h\n',
            ' 1 e\n',
            ' 2 l\n',
            ' 3 l\n',
            ' 4 o\n',
    ]:
        assert any(
            expected in output
            for output in outputs[1:]
        ), f"Expected {expected!r} in outputs[1:], but got: {outputs[1:]}"

    assert any([
        "['h', 'e', 'l', 'l', 'o']\n###" in outputs[0],
        "['h', 'e', 'l', 'o', 'l']\n###" in outputs[0],
        "['h', 'e', 'o', 'l', 'l']\n###" in outputs[0],
        "['e', 'h', 'l', 'l', 'o']\n###" in outputs[0],
        "['e', 'h', 'l', 'o', 'l']\n###" in outputs[0],
        "['e', 'h', 'o', 'l', 'l']\n###" in outputs[0],
        "['e', 'l', 'h', 'o', 'l']\n###" in outputs[0],
        "['e', 'l', 'h', 'l', 'o']\n###" in outputs[0],
        "['e', 'l', 'l', 'h', 'o']\n###" in outputs[0],
        "['e', 'l', 'l', 'o', 'h']\n###" in outputs[0],
        "['h', 'l', 'e', 'o', 'l']\n###" in outputs[0],
        "['h', 'l', 'e', 'l', 'o']\n###" in outputs[0],
        "['h', 'l', 'l', 'e', 'o']\n###" in outputs[0],
        "['h', 'l', 'l', 'o', 'e']\n###" in outputs[0],
    ]), outputs[0]
