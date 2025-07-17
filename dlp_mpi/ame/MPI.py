"""
This module provides the MPI interface, mimicking the mpi4py package.
"""

from .constants import ANY_TAG, ANY_SOURCE
from .core import COMM_WORLD, Status

__all__ = [
    'COMM_WORLD',
    'Status',
    'ANY_TAG',
    'ANY_SOURCE',
]
