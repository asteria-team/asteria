"""
Data model for physical dataset requests.
"""

from typing import Any, Dict

from pydantic import BaseModel


class PhysicalDatasetRequest(BaseModel):
    query: Dict[str, Any]
    """The DQL query filter document."""
