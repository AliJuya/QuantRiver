from __future__ import annotations


def pair_key(fast_tf: str, slow_tf: str) -> str:
    return f"{str(fast_tf)}|{str(slow_tf)}"


_PAIR_ROWS = (
    ("5m|1h", "5m", "1h"),
    ("15m|2h", "15m", "2h"),
    ("1h|2h", "1h", "2h"),
    ("1h|12h", "1h", "12h"),
)


VOLATILITY_ENGINE_CONFIG = {
    "tf_pairs": [
        {
            "key": key,
            "fast_tf": fast_tf,
            "slow_tf": slow_tf,
        }
        for key, fast_tf, slow_tf in _PAIR_ROWS
    ],
    "rv_bv": {
        "per_pair_windows": {
            key: {
                "n_fast": 20,
                "n_bv_fast": 20,
                "n_slow": 20,
            }
            for key, _, _ in _PAIR_ROWS
        }
    },
    "percentiles": {
        "lookbacks_per_pair": {
            key: {
                "lookback_fast": 64,
                "lookback_slow": 64,
            }
            for key, _, _ in _PAIR_ROWS
        }
    },
}

# Backward-compatible alias kept for older call sites.
VOLATILITY_ENGINE_V1_CONFIG = VOLATILITY_ENGINE_CONFIG
