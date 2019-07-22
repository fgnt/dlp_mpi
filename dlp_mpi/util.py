import logging
import contextlib


LOG = logging.getLogger('dlp_mpi')


@contextlib.contextmanager
def progress_bar(
        sequence,
        display_progress_bar,
):


    try:
        length = len(sequence)
    except TypeError:
        length = None

    if display_progress_bar:
        try:
            from tqdm import tqdm
        except ImportError:
            LOG.warning('Can not import tqdm. Disable the progress bar.')
        else:
            # Smoothing has problems with a huge amount of workers (e.g. 200)
            with tqdm(
                    total=length,
                    # disable=not display_progress_bar,
                    mininterval=2,
                    smoothing=None,
            ) as pbar:
                yield pbar
            return

    class DummyPBar:
        def set_description(self, *args, **kwargs):
            pass

        def update(self, *args, **kwargs):
            pass

    yield DummyPBar()
