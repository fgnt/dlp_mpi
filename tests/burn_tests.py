import subprocess
from pathlib import Path
import tempfile
import shlex

examples_folder = Path(__file__).parent.parent / 'examples'


def run(cmd, cwd=None, check=True):
    """Run a command in a subprocess."""
    try:
        return subprocess.run(cmd, shell=isinstance(cmd, str), check=check, capture_output=True, text=True, cwd=cwd, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        e.add_note(f'\n\nstdout: {e.stdout}\nstderr: {e.stderr}')
        raise


def test():
    result = run("mpiexec -np 2 python -c 'import dlp_mpi; print(dlp_mpi.RANK)'")
    assert result.stdout in ['0\n1\n', '1\n0\n'], f"Unexpected output: {result.stdout}"


def exec_code(code, size=2):
    """
    Execute code with mpiexec -np <size> ....

    Catch the stdout of each rank and return it, ordered by RANK.

    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        (tmpdir / 'code.py').write_text(code)
        try:
            run(["mpiexec", "-np", f"{size}", "bash", "-c", f"python {tmpdir / 'code.py'} > {tmpdir / '$OMPI_COMM_WORLD_RANK.txt'}"], cwd=tmpdir)
        except subprocess.CalledProcessError as e:
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


def test_1(size=2):
    """
    >>> test_1()
    rank=0, size=2
    <BLANKLINE>
    rank=1, size=2
    <BLANKLINE>
    """
    for output in exec_code((examples_folder / 'mpi_1_rank_and_size.py').read_text(), size=size):
        print(output)


def test_2(size=2):
    """
    >>> test_2()
    rank=0, size=2, data={'key1': [7, 2.72, (2+3j)], 'key2': ('abc', 'xyz')}
    <BLANKLINE>
    rank=1, size=2, data={'key1': [7, 2.72, (2+3j)], 'key2': ('abc', 'xyz')}
    <BLANKLINE>
    """
    for output in exec_code((examples_folder / 'mpi_2_broadcast_data.py').read_text(), size=size):
        print(output)


def test_3(size=2):
    """
    >>> test_3()
    rank=0, size=2, data=['hello', 'world']
    <BLANKLINE>
    rank=1, size=2, data=None
    <BLANKLINE>
    """
    for output in exec_code((examples_folder / 'mpi_3_gather_data.py').read_text(), size=size):
        print(output)


def test_4(size=2):
    """
    >>> test_4()
    rank=0, size=2, data=10
    rank=0, size=2, data=12
    job splits: [[20, 24], [22, 26]]
    flat result:  [20, 24, 22, 26]
    <BLANKLINE>
    rank=1, size=2, data=11
    rank=1, size=2, data=13
    <BLANKLINE>
    """
    for output in exec_code((examples_folder / 'mpi_4_scatter_gather.py').read_text(), size=size):
        print(output)


def test_5(size=2):
    """

    >>> test_5()
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
    for output in exec_code((examples_folder / 'mpi_5_map_unordered.py').read_text(), size=size):
        print(output)


def test_5_3(size=3):
    """
    >>> test_5_3()
    """
    outputs = exec_code((examples_folder / 'mpi_5_map_unordered.py').read_text(), size=size)

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
