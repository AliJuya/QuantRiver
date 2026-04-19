from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event, Thread
from typing import Callable, Optional

from core.data_engine.tick_river import Tick


@dataclass
class BinanceAggTradeWSTickSource:
    """
    Live TickSource: Binance Futures aggTrade WebSocket
    - Unstoppable: reconnect loop with backoff
    - Emits Tick(ts, price, qty, side, trade_id)
    """
    symbol: str  # e.g. "ETHUSDT"
    is_usd_m_futures: bool = True  # True -> fstream (USDT/USDC-margined), False -> dstream (coin-margined)
    timeout_sec: int = 20
    reconnect_backoff_min: float = 0.25
    reconnect_backoff_max: float = 10.0

    def __post_init__(self) -> None:
        self._stop = Event()
        self._th: Optional[Thread] = None
        self._on_tick: Optional[Callable[[Tick], None]] = None

        # stats
        self._msgs = 0
        self._ticks = 0
        self._errors = 0
        self._reconnects = 0

    def start(self, *, on_tick: Callable[[Tick], None]) -> None:
        self._on_tick = on_tick
        self._stop.clear()

        if self._th and self._th.is_alive():
            return

        self._th = Thread(target=self._run, name=f"BinanceAggTradeWS[{self.symbol}]", daemon=True)
        self._th.start()

    def stop(self) -> None:
        self._stop.set()

    def stats(self) -> dict:
        return {
            "msgs": self._msgs,
            "ticks": self._ticks,
            "errors": self._errors,
            "reconnects": self._reconnects,
        }

    # ---------------- internals ----------------

    def _ws_url(self) -> str:
        base = "wss://fstream.binance.com/ws" if self.is_usd_m_futures else "wss://dstream.binance.com/ws"
        stream = f"{self.symbol.lower()}@aggTrade"
        return f"{base}/{stream}"

    def _run(self) -> None:
        # websocket-client is the lightest for threaded WS.
        try:
            import websocket  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "Missing dependency: websocket-client\n"
                "Install: pip install websocket-client"
            ) from e

        backoff = self.reconnect_backoff_min

        while not self._stop.is_set():
            url = self._ws_url()

            def on_message(_ws, message: str):
                self._msgs += 1
                try:
                    data = json.loads(message)

                    # Binance aggTrade WS payload keys:
                    # { e, E, s, a, p, q, f, l, T, m, M }
                    # T = trade time (ms)
                    t_ms = int(data["T"])
                    ts = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc)

                    price = float(data["p"])
                    qty = float(data["q"])
                    trade_id = int(data.get("a")) if data.get("a") is not None else None

                    # m=true => buyer is maker => taker is seller => aggressive side SELL
                    side = "SELL" if bool(data.get("m")) else "BUY"

                    tick = Tick(ts=ts, price=price, qty=qty, side=side, trade_id=trade_id)
                    self._ticks += 1

                    # never block WS thread with heavy work
                    cb = self._on_tick
                    if cb:
                        cb(tick)

                except Exception:
                    self._errors += 1

            def on_error(_ws, _err):
                self._errors += 1

            def on_close(_ws, _status_code, _msg):
                pass

            ws = websocket.WebSocketApp(
                url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            self._reconnects += 1
            try:
                ws.run_forever(ping_interval=15, ping_timeout=10, ping_payload="ping")
            except Exception:
                self._errors += 1

            if self._stop.is_set():
                break

            # backoff reconnect
            time.sleep(backoff)
            backoff = min(self.reconnect_backoff_max, backoff * 1.5)