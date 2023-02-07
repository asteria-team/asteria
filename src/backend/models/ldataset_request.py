"""
Data model for logical dataset requests.
"""

from typing import Any, Dict

from pydantic import BaseModel


class LogicalDatasetRequest(BaseModel):
    query: Dict[str, Any]
    """The DQL query filter document."""
