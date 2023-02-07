"""
Global datalake integrity verification.
"""

import mlops.datalake._util as util
import mlops.datalake.dataset.integrity as dataset_integrity


def verify():
    """Verify global datalake integrity."""
    util.ctx.datalake_init()
    dataset_integrity.verify()
