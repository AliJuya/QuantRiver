from __future__ import annotations

from dataclasses import dataclass, field
from queue import Empty
from threading import Event, Thread

from core.engine.module_scheduler import ModuleScheduler
from core.engine.signal_engine import SignalEngine
from core.engine.strategy_router import StrategyRouter
from core.execution.execution_adapter import ExecutionAdapter
from core.indicators.indicator_engine import IndicatorEngine
from core.state.events import EngineEventQueue, TFClosedEvent
from core.state.market_state import MarketState


@dataclass
class CoreEngine:
    event_q: EngineEventQueue
    market_state: MarketState
    indicator_engine: IndicatorEngine
    strategy_router: StrategyRouter
    signal_engine: SignalEngine
    execution_adapter: ExecutionAdapter
    models: list[object] = field(default_factory=list)
    modules: list[object] = field(default_factory=list)
    models_by_tf: dict[str, list[object]] = field(default_factory=dict)
    modules_by_tf: dict[str, list[object]] = field(default_factory=dict)
    module_scheduler: ModuleScheduler | None = None

    def __post_init__(self) -> None:
        self._stop = Event()
        self._worker: Thread | None = None
        self._processed = 0
        self._missed = 0
        self._errors = 0
        self._executed = 0
        self._last_error: str | None = None
        if self.module_scheduler is None:
            self.module_scheduler = ModuleScheduler.build(
                models=self.models,
                modules=self.modules,
                strategies=self.strategy_router.strategies,
                models_by_tf=self.models_by_tf,
                modules_by_tf=self.modules_by_tf,
            )

    def start(self) -> None:
        worker = self._worker
        if worker and worker.is_alive():
            return

        self._stop.clear()
        self._worker = Thread(target=self._run, name="CoreEngine", daemon=True)
        self._worker.start()

    def stop(self) -> None:
        self._stop.set()
        worker = self._worker
        if worker and worker.is_alive():
            worker.join(timeout=1.0)
        self._shutdown_components()
        self._worker = None

    def stats(self) -> dict:
        return {
            "processed": self._processed,
            "missed": self._missed,
            "errors": self._errors,
            "executed": self._executed,
            "last_error": self._last_error,
        }

    def process_event(self, event: TFClosedEvent) -> None:
        try:
            self._process_event(event)
        except Exception as exc:
            self._record_error("process", event, exc)

    def prime_event(self, event: TFClosedEvent) -> None:
        try:
            self._prime_event(event)
        except Exception as exc:
            self._record_error("prime", event, exc)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                event = self.event_q.get(timeout=0.25)
            except Empty:
                continue

            self.process_event(event)

    def _process_event(self, event: TFClosedEvent) -> None:
        if not self.market_state.is_warm:
            return

        candle = self._resolve_candle(event)
        if candle is None:
            return

        scheduler = self._prepare_stages(event.tf, candle)
        for module in scheduler.modules_for(event.tf):
            self._call_component(module, event.tf, candle)

        decisions = self.strategy_router.on_tf_close(
            event.tf,
            candle,
            self.market_state,
            strategies=scheduler.strategies_for(event.tf),
        )
        intents = self.signal_engine.to_intents(decisions, self.market_state)
        if intents:
            results = self.execution_adapter.execute_intents(intents, self.market_state)
            self._executed += len(results)

        self._processed += 1

    def _prime_event(self, event: TFClosedEvent) -> None:
        candle = self._resolve_candle(event)
        if candle is None:
            return

        scheduler = self._prepare_stages(event.tf, candle, prime=True)
        for module in scheduler.modules_for(event.tf):
            self._prime_component(module, event.tf, candle, allow_fallback=False)
        self.strategy_router.prime_on_tf_close(
            event.tf,
            candle,
            self.market_state,
            strategies=scheduler.strategies_for(event.tf),
        )

    def _resolve_candle(self, event: TFClosedEvent):
        candle = self.market_state.get_candle(event.tf, event.candle_open_time)
        if candle is None:
            self._missed += 1
        return candle

    def _prepare_stages(self, tf: str, candle, *, prime: bool = False) -> ModuleScheduler:
        self.indicator_engine.on_tf_close(tf, candle, self.market_state)

        scheduler = self.module_scheduler
        if scheduler is None:
            raise RuntimeError("ModuleScheduler is not initialized")

        for model in scheduler.models_for(tf):
            if prime:
                self._prime_component(model, tf, candle, allow_fallback=True)
            else:
                self._call_component(model, tf, candle)

        return scheduler

    def _call_component(self, component, tf: str, candle) -> None:
        handler = getattr(component, "on_tf_close", None)
        if callable(handler):
            handler(tf, candle, self.market_state)
            return

        if callable(component):
            component(tf, candle, self.market_state)
            return

        raise TypeError(f"{component.__class__.__name__} must expose on_tf_close() or be callable")

    def _prime_component(self, component, tf: str, candle, *, allow_fallback: bool) -> None:
        handler = getattr(component, "prime_on_tf_close", None)
        if callable(handler):
            handler(tf, candle, self.market_state)
            return

        if allow_fallback:
            self._call_component(component, tf, candle)

    def _shutdown_components(self) -> None:
        seen: set[int] = set()
        for bucket in (self.models, self.modules):
            for component in bucket:
                ident = id(component)
                if ident in seen:
                    continue
                seen.add(ident)
                self._shutdown_component(component)

        for mapping in (self.models_by_tf, self.modules_by_tf):
            for bucket in mapping.values():
                for component in bucket:
                    ident = id(component)
                    if ident in seen:
                        continue
                    seen.add(ident)
                    self._shutdown_component(component)

    def _shutdown_component(self, component) -> None:
        handler = getattr(component, "shutdown", None)
        if not callable(handler):
            return
        try:
            handler()
        except Exception as exc:
            self._errors += 1
            self._last_error = f"phase=shutdown component={component.__class__.__name__} err={exc!r}"
            print(f"[CORE_ERROR] {self._last_error}")

    def _record_error(self, phase: str, event: TFClosedEvent, exc: Exception) -> None:
        self._errors += 1
        self._last_error = (
            f"phase={phase} tf={event.tf} candle_open_time={event.candle_open_time} err={exc!r}"
        )
        print(f"[CORE_ERROR] {self._last_error}")
