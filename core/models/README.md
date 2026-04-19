## Models Architecture

This public repository keeps the **model orchestration layer** but does not ship the
private research implementations that power the original volatility, structure, and
regime engines.

What remains public:

- `base.py`
  Snapshot and bundle contracts used by the engine and strategies.
- `bundling.py`
  Atomic bundle commit logic for multi-model updates.
- `model_engine.py`
  The dependency-aware execution flow: `vol -> structure + regime`.
- `model_module.py`
  Recipe-style runner integration.
- `adapters/`
  The state-to-model bridge layer that defines warmup, replay, and payload shape.
- `warmup.py`
  Shared warmup checks for model-triggered modules.

What was removed:

- proprietary feature engineering
- private latent-state filters
- trained artifacts and research scripts
- private v2/v3 model generations

What replaces it:

- lightweight placeholder engines under:
  - `vol_engine/`
  - `structure_engine/`
  - `regime_engine/`
- they preserve the same import paths and a compatible payload shape
- they are intended for **architecture demonstration only**

If you want to plug in your own models, the clean extension points are:

1. implement engine logic behind the existing adapter contracts
2. keep payloads serializable through `ModelSnapshot.payload`
3. preserve adapter warmup requirements
4. let `ModelEngine` continue to commit atomic bundles into `state.model_bundle`
