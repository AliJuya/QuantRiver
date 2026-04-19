from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable


@dataclass(frozen=True)
class WarmupReq:
    name: str
    req: Dict[str, int]  # tf -> bars


def compute_global_warmup(reqs: Iterable[WarmupReq]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in reqs:
        for tf, n in r.req.items():
            if n <= 0:
                continue
            out[tf] = max(out.get(tf, 0), n)
    return out