import collections
import dlp_mpi


class NestedDict:
    """
    A dict, where you can write to arbitary nested values. Read and overwrite
    is not allowed (i.e. always use different keys).
    Has a special method `gather` to move the data to one MPI process and sync
    the data.

    Example, how to use:

        data = dlp_mpi.collection.NestedDict()

        for ex in dlp_mpi.split_managed(ds, progress_bar=True):
            data[...][...] = ...

        data = data.gather()  # Move all data to master and return nested dict.
        if dlp_mpi.IS_MASTER:
            ...   # Do something with data


    Toy example (i.e. without multiple processes):

        >>> d = NestedDict()
        >>> d['a']['b'] = 3
        >>> d['a']['c'] = 4
        >>> d
        NestedDict({('a', 'b'): 3, ('a', 'c'): 4, ...}, ())
        >>> d['a']
        NestedDict({('a', 'b'): 3, ('a', 'c'): 4, ...}, ('a',))
        >>> d.gather()
        {'a': {'b': 3, 'c': 4}}
        >>> d['a']['c'] = 5
        Traceback (most recent call last):
        ...
        RuntimeError: This is a mapping designed for MPI, where the data is
        later synced. Only write operations are allowed, not read
        and not overwrite. You tried to overwrite the following:
            self['a']['c']

    """
    def __init__(self, _data=None, _prefix=tuple()):
        if _data is None:
            _data = {}
        self._data = _data
        self._prefix = _prefix

    def __repr__(self):
        return f'{self.__class__.__qualname__}({str(self._data)[:-1]}, ...}}, {self._prefix})'

    def __getitem__(self, item):
        return NestedDict(self._data, self._prefix + (item,))

    def __setitem__(self, key, value):
        nested_key = self._prefix + (key,)
        if nested_key in self._data:
            raise RuntimeError(
                f'This is a mapping designed for MPI, where the data is\n'
                f'later synced. Only write operations are allowed, not read\n'
                f'and not overwrite. You tried to overwrite the following:\n'
                f'''    self['{"']['".join(nested_key)}']'''
            )
        self._data[nested_key] = value

    def gather(self, root=dlp_mpi.MASTER):
        """
        Call `dlp_mpi.gather` and sync the data.
        """
        data = dlp_mpi.gather(self._data, root)
        if dlp_mpi.IS_MASTER:
            total_length = sum([len(d) for d in data])

            merged_data = {k: v for d in data for k, v in d.items()}
            if total_length != len(merged_data):
                keys = [k for d in data for k, v in d.items()]

                duplicates = {  # https://stackoverflow.com/a/9835819/5766934
                    k: []
                    for k, count in collections.Counter(keys).items()
                    if count > 1
                }

                for rank, d in enumerate(data):
                    for dup, ranks in duplicates.items():
                        if dup in d:
                            ranks.append(rank)

                msg = [
                    f'Error in {self.__class__.__qualname__}.\n'
                    f'Different worker tried to write to the same key:'
                ]
                for i, (k, v) in enumerate(duplicates.items()):
                    if i > 5:
                        msg.append('    ...')
                        break

                    k = ''.join([f'[{e!r}]'for e in k])
                    msg.append(
                        f'''    Ranks: {v} tried to write to the key {k}'''
                    )

                raise AssertionError('\n'.join(msg))

            from paderbox.utils.nested import deflatten
            return deflatten(merged_data, sep=None)
        else:
            return data
