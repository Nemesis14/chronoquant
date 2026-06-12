# =============================================================================
# schemas/base.py — Base Pydantic model for ChronoQuant schemas
# =============================================================================
# Purpose:
#  - Provide a single shared base class for all project schemas
#  - Enforce immutability and strict field validation across schema modules
# =============================================================================

from pydantic import BaseModel, ConfigDict


class ChronoBase(BaseModel):
    model_config = ConfigDict(
        frozen    = True,
        extra     = "forbid",
        populate_by_name = True,
    )
