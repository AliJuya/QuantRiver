from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event, Thread
from typing import Callable, Optional

from core.data_engine.data_engine import DataEngine, DataEngineConfig
from core.data_engine.tick_river import Tick


@dataclass
class SyntheticTickSource:
    hz: int = 400
    price0: float = 3000.0

    def __post_init__(self):
        self._stop = Event()
        self._th: Optional[Thread] = None

    def start(self, *, on_tick: Callable[[Tick], None]) -> None:
        self._stop.clear()

        def loop():
            price = self.price0
            trade_id = 1
            dt = 1.0 / max(1, self.hz)
            while not self._stop.is_set():
                now = datetime.now(timezone.utc)
                price += ((trade_id % 7) - 3) * 0.01
                on_tick(Tick(ts=now, price=price, qty=0.01, side="BUY", trade_id=trade_id))
                trade_id += 1
                time.sleep(dt)

        self._th = Thread(target=loop, name="SyntheticTickSource", daemon=True)
        self._th.start()

    def stop(self) -> None:
        self._stop.set()


def fmt_dt(dt):
    if not dt:
        return "None"
    return dt.strftime("%H:%M:%S")


def main():
    src = SyntheticTickSource(hz=500)

    de = DataEngine(
        config=DataEngineConfig(
            input_mode="ticks",
            tick_river_maxlen=50_000,
            candle_river_maxlen=5000,     # small to observe bounded behavior
            tfs=("1m", "5m"),
        ),
        tick_source=src,
    )

    de.start()
    t0 = time.time()

    try:
        while True:
            st = de.stats()
            tr = st["tick_river"]
            cr = st["candle_rivers"]

            last_1s = de.get_candle_river("1s").last()
            last_1m = de.get_candle_river("1m").last()
            last_5m = de.get_candle_river("5m").last()

            print(
                f"[t+{time.time()-t0:6.1f}s] "
                f"ticks size={tr['size']} push={tr['pushes']} pop={tr['pops']} drop={tr['dropped']} | "
                f"1s size={cr['1s']['size']} push={cr['1s']['pushes']} drop={cr['1s']['dropped']} last={fmt_dt(last_1s.open_time) if last_1s else 'None'} | "
                f"1m size={cr['1m']['size']} push={cr['1m']['pushes']} drop={cr['1m']['dropped']} last={fmt_dt(last_1m.open_time) if last_1m else 'None'} | "
                f"5m size={cr['5m']['size']} push={cr['5m']['pushes']} drop={cr['5m']['dropped']} last={fmt_dt(last_5m.open_time) if last_5m else 'None'}"
            )
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n[STOP] stopping...")
    finally:
        de.stop()


if __name__ == "__main__":
    main()