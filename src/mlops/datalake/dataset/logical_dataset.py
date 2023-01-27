"""
LogicalDataset class implementation.
"""


class LogicalDataset:
    """
    A LogicalDataset represents a collection of PhysicalDataset
    instances that are combined and partitioned to prepare for ML.
    """

    def __init__(self, identifier: str):
        self.identifier = identifier
        """The unique identifier for the LogicalDataset instance."""
