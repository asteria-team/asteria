"""
NaturalLanguageProcessingDataset definition.
"""

from ..physical_dataset import DatasetDomain, DatasetType, PhysicalDataset


class NaturalLanguageProcessingDatasetType(DatasetType):
    pass


class NaturalLanguageProcessingDataset(PhysicalDataset):
    """NaturalLanguageProcessingDataset is the base for all datasets in this domain."""

    def __init__(
        self, type: NaturalLanguageProcessingDatasetType, identifier: str
    ):
        super().__init__(
            DatasetDomain.NATURAL_LANGUAGE_PROCESSING, type, identifier
        )

    def _verify_integrity(self):
        raise NotImplementedError("Not implemented.")
