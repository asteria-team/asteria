"""
Timing utilities.
"""

import time


def timestamp() -> int:
    """
    Get the current UNIX timestamp.

    :return: The seconds since the epoch
    :rtype: int
    """
    return int(time.time())
