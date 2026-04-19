from __future__ import annotations

from .regime_engine import RegimeContext, RegimeEngine, RegimeState


class RegimeEngineHMM(RegimeEngine):
    """
    Backward-compatible placeholder name kept for the public repo.
    """

    pass


__all__ = ["RegimeContext", "RegimeEngineHMM", "RegimeState"]
