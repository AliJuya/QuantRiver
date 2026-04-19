from __future__ import annotations


class DefaultVolAccessor:
    """
    Placeholder access helper kept for API compatibility with the original structure
    engine wiring.
    """

    def get(self, payload: dict, key: str, default=None):
        if not isinstance(payload, dict):
            return default
        return payload.get(key, default)
