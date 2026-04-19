from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Callable, Iterable, Optional

import pandas as pd

from core.data_engine.candle_aggregator_tf import tf_to_seconds
from core.data_engine.candle_river import Candle
from core.data_engine.tick_river import Tick


def _normalize_tf(tf: str) -> str:
    out = str(tf).strip().lower()
    tf_to_seconds(out)
    return out


def _to_utc_timestamp(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series, utc=True)

    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce")
        max_abs = numeric.abs().max()
        if pd.isna(max_abs):
            return pd.to_datetime(numeric, utc=True)
        if max_abs >= 1e17:
            unit = "ns"
        elif max_abs >= 1e14:
            unit = "us"
        elif max_abs >= 1e11:
            unit = "ms"
        else:
            unit = "s"
        return pd.to_datetime(numeric, unit=unit, utc=True)

    return pd.to_datetime(series, utc=True)


def _normalize_paths(path: str | Iterable[str]) -> list[str]:
    if isinstance(path, str):
        return [path]
    return [str(p) for p in path]


def _read_parquet(path: str | Iterable[str]) -> pd.DataFrame:
    frames = [pd.read_parquet(p) for p in _normalize_paths(path)]
    if not frames:
        return pd.DataFrame()
    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True)


def _require_columns(df: pd.DataFrame, cols: tuple[str, ...], *, path: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{path} is missing required columns: {missing}")


def _first_existing(df: pd.DataFrame, candidates: tuple[str, ...]) -> str:
    for name in candidates:
        if name in df.columns:
            return name
    raise ValueError(f"Missing required columns. Expected one of: {candidates}")


def _ticks_to_1s_frame(ticks: pd.DataFrame) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

    frame = ticks.loc[:, ["timestamp", "price", "qty"]].copy()
    frame = frame.set_index("timestamp")

    out = frame.resample("1s", label="left", closed="left").agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("qty", "sum"),
    )

    out["close"] = out["close"].ffill()
    out = out.dropna(subset=["close"])

    prev_close = out["close"].shift(1)
    out["open"] = out["open"].fillna(prev_close).fillna(out["close"])
    out["high"] = out["high"].fillna(out["close"])
    out["low"] = out["low"].fillna(out["close"])
    out["volume"] = out["volume"].fillna(0.0)

    out = out.reset_index()
    if len(out) <= 1:
        return out.iloc[0:0].copy()
    return out.iloc[1:].reset_index(drop=True)


def _normalize_candle_frame(
    candles: pd.DataFrame,
    *,
    tf: str,
    path_label: str,
) -> pd.DataFrame:
    if candles.empty:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

    ts_col = _first_existing(candles, ("timestamp", "open_time"))
    open_col = _first_existing(candles, ("open", "o"))
    high_col = _first_existing(candles, ("high", "h"))
    low_col = _first_existing(candles, ("low", "l"))
    close_col = _first_existing(candles, ("close", "c"))
    volume_col = _first_existing(candles, ("volume", "v"))

    out = pd.DataFrame(
        {
            "timestamp": _to_utc_timestamp(candles[ts_col]),
            "open": pd.to_numeric(candles[open_col], errors="raise"),
            "high": pd.to_numeric(candles[high_col], errors="raise"),
            "low": pd.to_numeric(candles[low_col], errors="raise"),
            "close": pd.to_numeric(candles[close_col], errors="raise"),
            "volume": pd.to_numeric(candles[volume_col], errors="raise"),
        }
    )
    out = out.sort_values("timestamp").reset_index(drop=True)
    out = out.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"])

    if out.empty:
        raise ValueError(f"{path_label} did not contain usable {tf} candles")
    return out


def _resample_from_base(base_candles: pd.DataFrame, source_tf: str, target_tf: str) -> pd.DataFrame:
    source_tf = _normalize_tf(source_tf)
    target_tf = _normalize_tf(target_tf)

    source_sec = tf_to_seconds(source_tf)
    target_sec = tf_to_seconds(target_tf)

    if target_sec == source_sec:
        return base_candles
    if target_sec < source_sec:
        raise ValueError(
            f"Cannot downsample from source_tf={source_tf} to target_tf={target_tf}"
        )
    if target_sec % source_sec != 0:
        raise ValueError(
            f"target_tf must be an integer multiple of source_tf: "
            f"source_tf={source_tf} target_tf={target_tf}"
        )

    frame = base_candles.set_index("timestamp")
    out = frame.resample(f"{target_sec}s", label="left", closed="left").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    )
    out = out.dropna(subset=["open", "high", "low", "close"])
    out = out.reset_index()
    return out.reset_index(drop=True)


def _row_to_candle(row, tf: str) -> Candle:
    tf = _normalize_tf(tf)
    open_time = row.timestamp.to_pydatetime()
    return Candle(
        tf=tf,
        open_time=open_time,
        close_time=open_time + timedelta(seconds=tf_to_seconds(tf)),
        open=float(row.open),
        high=float(row.high),
        low=float(row.low),
        close=float(row.close),
        volume=float(row.volume),
    )


def _rows_to_candles(rows: list[dict], tf: str) -> list[Candle]:
    tf = _normalize_tf(tf)
    delta = timedelta(seconds=tf_to_seconds(tf))
    out: list[Candle] = []
    for row in rows:
        open_time = row["timestamp"].to_pydatetime()
        out.append(
            Candle(
                tf=tf,
                open_time=open_time,
                close_time=open_time + delta,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
            )
        )
    return out


class ParquetTickSource:
    def __init__(
        self,
        *,
        ticks: pd.DataFrame,
        replay_hz: Optional[float] = None,
        start_paused: bool = False,
    ) -> None:
        self._ticks = ticks
        self._replay_hz = replay_hz
        self._gate = Event()
        self._stop = Event()
        self._finished = Event()
        self._th: Optional[Thread] = None
        self._on_tick: Optional[Callable[[Tick], None]] = None

        if start_paused:
            self._gate.clear()
        else:
            self._gate.set()

    def start(self, *, on_tick: Callable[[Tick], None]) -> None:
        self._on_tick = on_tick
        self._stop.clear()
        self._finished.clear()

        if self._th and self._th.is_alive():
            return

        self._th = Thread(target=self._run, name="ParquetTickSource", daemon=True)
        self._th.start()

    def stop(self) -> None:
        self._stop.set()
        self._gate.set()

    def pause(self) -> None:
        self._gate.clear()

    def resume(self) -> None:
        self._gate.set()

    def is_finished(self) -> bool:
        return self._finished.is_set()

    def wait_finished(self, timeout: Optional[float] = None) -> bool:
        return self._finished.wait(timeout)

    def _run(self) -> None:
        on_tick = self._on_tick
        if on_tick is None:
            self._finished.set()
            return

        try:
            for row in self._ticks.itertuples(index=False):
                if self._stop.is_set():
                    break
                if not self._wait_until_resumed():
                    break

                trade_id = None if pd.isna(row.trade_id) else int(row.trade_id)
                side = "SELL" if bool(row.is_buyer_maker) else "BUY"
                on_tick(
                    Tick(
                        ts=row.timestamp.to_pydatetime(),
                        price=float(row.price),
                        qty=float(row.qty),
                        side=side,
                        trade_id=trade_id,
                    )
                )
                self._pace()
        finally:
            self._finished.set()

    def _wait_until_resumed(self) -> bool:
        while not self._stop.is_set():
            if self._gate.wait(timeout=0.1):
                return True
        return False

    def _pace(self) -> None:
        if self._replay_hz and self._replay_hz > 0:
            time.sleep(1.0 / self._replay_hz)


class ParquetCandleSource:
    def __init__(
        self,
        *,
        candles: pd.DataFrame,
        source_tf: str,
        replay_hz: Optional[float] = None,
        start_paused: bool = False,
    ) -> None:
        self._candles = candles
        self._source_tf = _normalize_tf(source_tf)
        self._replay_hz = replay_hz
        self._gate = Event()
        self._stop = Event()
        self._finished = Event()
        self._th: Optional[Thread] = None
        self._on_candle_close: Optional[Callable[[Candle], None]] = None

        if start_paused:
            self._gate.clear()
        else:
            self._gate.set()

    def start(
        self,
        *,
        on_candle_close: Optional[Callable[[Candle], None]] = None,
        on_candle_1s: Optional[Callable[[Candle], None]] = None,
    ) -> None:
        cb = on_candle_close or on_candle_1s
        if cb is None:
            raise ValueError("start() requires on_candle_close or on_candle_1s")

        self._on_candle_close = cb
        self._stop.clear()
        self._finished.clear()

        if self._th and self._th.is_alive():
            return

        self._th = Thread(
            target=self._run,
            name=f"ParquetCandleSource[{self._source_tf}]",
            daemon=True,
        )
        self._th.start()

    def stop(self) -> None:
        self._stop.set()
        self._gate.set()

    def pause(self) -> None:
        self._gate.clear()

    def resume(self) -> None:
        self._gate.set()

    def is_finished(self) -> bool:
        return self._finished.is_set()

    def wait_finished(self, timeout: Optional[float] = None) -> bool:
        return self._finished.wait(timeout)

    def _run(self) -> None:
        on_candle_close = self._on_candle_close
        if on_candle_close is None:
            self._finished.set()
            return

        try:
            for row in self._candles.itertuples(index=False):
                if self._stop.is_set():
                    break
                if not self._wait_until_resumed():
                    break

                on_candle_close(_row_to_candle(row, self._source_tf))
                self._pace()
        finally:
            self._finished.set()

    def _wait_until_resumed(self) -> bool:
        while not self._stop.is_set():
            if self._gate.wait(timeout=0.1):
                return True
        return False

    def _pace(self) -> None:
        if self._replay_hz and self._replay_hz > 0:
            time.sleep(1.0 / self._replay_hz)


class ParquetCandle1sSource(ParquetCandleSource):
    def __init__(
        self,
        *,
        candles_1s: pd.DataFrame,
        replay_hz: Optional[float] = None,
        start_paused: bool = False,
    ) -> None:
        super().__init__(
            candles=candles_1s,
            source_tf="1s",
            replay_hz=replay_hz,
            start_paused=start_paused,
        )


class ParquetBacktestDataSource:
    """
    File-backed source wrapper for backtests.

    - mode="ticks": replay ticks and derive a 1s candle base internally
    - mode="candles": replay any closed base candle timeframe and aggregate upward
    - legacy mode="1s": treated as mode="candles", source_tf="1s"
    """

    def __init__(
        self,
        *,
        mode: str,
        path: str | Iterable[str],
        source_tf: str = "1s",
        replay_hz: Optional[float] = None,
        start_paused: bool = True,
    ) -> None:
        raw_mode = str(mode).strip().lower()
        if raw_mode == "1s":
            raw_mode = "candles"
            source_tf = "1s"
        if raw_mode not in ("ticks", "candles"):
            raise ValueError("mode must be 'ticks', 'candles', or legacy '1s'")

        self.mode = raw_mode
        self.source_tf = "1s" if self.mode == "ticks" else _normalize_tf(source_tf)
        self.path = list(_normalize_paths(path))
        self.replay_hz = replay_hz
        self.start_paused = start_paused

        self._tick_source: Optional[ParquetTickSource] = None
        self._candle_source: Optional[ParquetCandleSource] = None
        self._agg_cache: dict[str, pd.DataFrame] = {}

        if self.mode == "ticks":
            self._ticks = self._load_ticks(self.path)
            self._base_candles = _ticks_to_1s_frame(self._ticks)
        else:
            self._ticks = None
            self._base_candles = self._load_candles(self.path, self.source_tf)

        self._agg_cache[self.source_tf] = self._base_candles

    @classmethod
    def from_daily_range(
        cls,
        *,
        base_path: str,
        symbol: str,
        source_tf: str,
        start_date: date,
        end_date: date,
        lookback_days: int = 0,
        allow_missing_required: bool = False,
        replay_hz: Optional[float] = None,
        start_paused: bool = True,
    ) -> "ParquetBacktestDataSource":
        source_tf = _normalize_tf(source_tf)
        load_start = start_date - timedelta(days=max(0, int(lookback_days)))
        paths: list[str] = []
        missing_required: list[str] = []

        cur = load_start
        while cur <= end_date:
            path = (
                Path(base_path)
                / symbol
                / source_tf
                / f"{cur.year:04d}"
                / f"{cur.month:02d}"
                / f"{cur.day:02d}.parquet"
            )
            if path.exists():
                paths.append(str(path))
            elif cur >= start_date:
                missing_required.append(str(path))
            cur += timedelta(days=1)

        if missing_required:
            if not allow_missing_required:
                raise FileNotFoundError(
                    "Missing required daily parquet files:\n" + "\n".join(missing_required)
                )
            print(
                f"[BACKTEST_WARN] skipping {len(missing_required)} missing daily parquet file(s):\n"
                + "\n".join(missing_required)
            )

        if not paths:
            raise FileNotFoundError(
                f"No parquet files found for {symbol} {source_tf} between {load_start} and {end_date}"
            )

        return cls(
            mode="candles",
            path=paths,
            source_tf=source_tf,
            replay_hz=replay_hz,
            start_paused=start_paused,
        )

    @classmethod
    def from_daily_1s_range(
        cls,
        *,
        base_path: str,
        symbol: str,
        start_date: date,
        end_date: date,
        lookback_days: int = 0,
        allow_missing_required: bool = False,
        replay_hz: Optional[float] = None,
        start_paused: bool = True,
    ) -> "ParquetBacktestDataSource":
        return cls.from_daily_range(
            base_path=base_path,
            symbol=symbol,
            source_tf="1s",
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback_days,
            allow_missing_required=allow_missing_required,
            replay_hz=replay_hz,
            start_paused=start_paused,
        )

    def build_tick_source(self, *, stream_from: Optional[datetime] = None) -> ParquetTickSource:
        if self.mode != "ticks":
            raise ValueError("build_tick_source() requires mode='ticks'")
        if self._tick_source is None:
            self._tick_source = ParquetTickSource(
                ticks=self._slice_from(self._ticks, stream_from),
                replay_hz=self.replay_hz,
                start_paused=self.start_paused,
            )
        return self._tick_source

    def build_candle_source(self, *, stream_from: Optional[datetime] = None) -> ParquetCandleSource:
        if self.mode != "candles":
            raise ValueError("build_candle_source() requires mode='candles'")
        if self._candle_source is None:
            self._candle_source = ParquetCandleSource(
                candles=self._slice_from(self._base_candles, stream_from),
                source_tf=self.source_tf,
                replay_hz=self.replay_hz,
                start_paused=self.start_paused,
            )
        return self._candle_source

    def build_candle1s_source(
        self, *, stream_from: Optional[datetime] = None
    ) -> ParquetCandle1sSource:
        if self.mode != "candles" or self.source_tf != "1s":
            raise ValueError(
                "build_candle1s_source() requires mode='candles' with source_tf='1s'"
            )
        if self._candle_source is None or not isinstance(self._candle_source, ParquetCandle1sSource):
            self._candle_source = ParquetCandle1sSource(
                candles_1s=self._slice_from(self._base_candles, stream_from),
                replay_hz=self.replay_hz,
                start_paused=self.start_paused,
            )
        return self._candle_source

    def pause(self) -> None:
        stream = self._active_stream()
        if stream is not None:
            stream.pause()

    def resume(self) -> None:
        stream = self._active_stream()
        if stream is not None:
            stream.resume()

    def stop(self) -> None:
        stream = self._active_stream()
        if stream is not None:
            stream.stop()

    def is_finished(self) -> bool:
        stream = self._active_stream()
        return stream.is_finished() if stream is not None else False

    def wait_finished(self, timeout: Optional[float] = None) -> bool:
        stream = self._active_stream()
        return stream.wait_finished(timeout) if stream is not None else False

    def first_event_time(self) -> Optional[datetime]:
        if self.mode == "ticks":
            if self._ticks is None or self._ticks.empty:
                return None
            return self._ticks.iloc[0]["timestamp"].to_pydatetime()

        if self._base_candles.empty:
            return None
        return self._base_candles.iloc[0]["timestamp"].to_pydatetime()

    def iter_candles(
        self,
        *,
        tf: Optional[str] = None,
        stream_from: Optional[datetime] = None,
    ):
        target_tf = self.source_tf if tf is None else _normalize_tf(tf)
        frame = self._slice_from(self._frame_for_tf(target_tf), stream_from)
        for row in frame.itertuples(index=False):
            yield _row_to_candle(row, target_tf)

    def iter_candles_1s(self, *, stream_from: Optional[datetime] = None):
        if self.source_tf != "1s":
            raise ValueError("iter_candles_1s() requires source_tf='1s'")
        yield from self.iter_candles(tf="1s", stream_from=stream_from)

    def fetch_candles_before_anchor(
        self,
        tf: str,
        anchor_open_time: datetime,
        count: int,
    ) -> list[Candle]:
        want = int(count)
        if want <= 0:
            return []

        tf = _normalize_tf(tf)
        frame = self._frame_for_tf(tf)
        if frame.empty:
            return []

        anchor_ts = pd.Timestamp(anchor_open_time)
        tf_delta = pd.Timedelta(seconds=tf_to_seconds(tf))
        eligible = frame.loc[(frame["timestamp"] + tf_delta) <= anchor_ts]
        if eligible.empty:
            return []

        rows = eligible.tail(want).to_dict("records")
        return _rows_to_candles(rows, tf)

    def _active_stream(self) -> Optional[ParquetTickSource | ParquetCandleSource]:
        return self._tick_source or self._candle_source

    def _frame_for_tf(self, tf: str) -> pd.DataFrame:
        tf = _normalize_tf(tf)
        cached = self._agg_cache.get(tf)
        if cached is not None:
            return cached

        frame = _resample_from_base(self._base_candles, self.source_tf, tf)
        self._agg_cache[tf] = frame
        return frame

    @staticmethod
    def _slice_from(frame: pd.DataFrame, stream_from: Optional[datetime]) -> pd.DataFrame:
        if stream_from is None or frame.empty:
            return frame
        start_ts = pd.Timestamp(stream_from)
        return frame.loc[frame["timestamp"] >= start_ts].reset_index(drop=True)

    @staticmethod
    def _load_candles(path: str | Iterable[str], source_tf: str) -> pd.DataFrame:
        df = _read_parquet(path)
        path_label = f"{source_tf} parquet input"
        return _normalize_candle_frame(df, tf=source_tf, path_label=path_label)

    @staticmethod
    def _load_ticks(path: str | Iterable[str]) -> pd.DataFrame:
        df = _read_parquet(path)

        if "price" not in df.columns:
            if "p" not in df.columns:
                raise ValueError("tick parquet input must provide 'price' or 'p'")
            df["price"] = pd.to_numeric(df["p"], errors="raise")

        if "qty" not in df.columns:
            if "q" not in df.columns:
                raise ValueError("tick parquet input must provide 'qty' or 'q'")
            df["qty"] = pd.to_numeric(df["q"], errors="raise")

        ts_col = "timestamp" if "timestamp" in df.columns else "T"
        maker_col = "is_buyer_maker" if "is_buyer_maker" in df.columns else "m"
        trade_id_col = "agg_id" if "agg_id" in df.columns else ("a" if "a" in df.columns else None)

        if ts_col not in df.columns:
            raise ValueError("tick parquet input must provide 'timestamp' or 'T'")
        if maker_col not in df.columns:
            raise ValueError("tick parquet input must provide 'is_buyer_maker' or 'm'")

        out = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(df[ts_col], unit="ms", utc=True),
                "price": pd.to_numeric(df["price"], errors="raise"),
                "qty": pd.to_numeric(df["qty"], errors="raise"),
                "is_buyer_maker": df[maker_col].astype(bool),
                "trade_id": df[trade_id_col] if trade_id_col is not None else None,
            }
        )
        out = out.sort_values("timestamp").reset_index(drop=True)
        return out
