from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IntegrationSource:
    project: str
    source_bucket: str
    source_object: str
    source_generation: int
