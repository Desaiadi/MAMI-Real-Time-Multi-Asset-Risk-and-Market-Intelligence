"""
Compute backend abstraction — GPU-grade, CPU-default.

The heavy numerical kernel in MAMI is the Monte Carlo VaR simulation: thousands
of random GBM paths. That is exactly the workload a GPU accelerates.

This module exposes a single array namespace ``xp`` that is **CuPy** when an
NVIDIA GPU + CuPy are available, and **NumPy** otherwise. Numerical code written
against ``xp`` runs unchanged on either backend. ``to_host()`` pulls a result
back to a Python float regardless of backend.

Enable the GPU path by installing a CuPy build that matches your CUDA version,
e.g. ``pip install cupy-cuda12x`` (see services/*/requirements-gpu.txt). With no
GPU/CuPy present, everything transparently falls back to NumPy — nothing breaks.
"""
import os

GPU_AVAILABLE = False
_BACKEND = "numpy"

# Allow forcing CPU even on a GPU box (useful for reproducible benchmarks/tests).
_FORCE_CPU = os.getenv("MAMI_FORCE_CPU", "0") == "1"

if not _FORCE_CPU:
    try:
        import cupy as _cp  # type: ignore
        # Touch a device op so a CUDA-less "import cupy" doesn't masquerade as ready.
        _cp.zeros(1).sum()
        xp = _cp
        GPU_AVAILABLE = True
        _BACKEND = "cupy"
    except Exception:
        import numpy as _np
        xp = _np
else:
    import numpy as _np
    xp = _np


def to_host(value):
    """Return a plain Python float from an xp scalar/0-d array (CPU or GPU)."""
    if GPU_AVAILABLE and hasattr(value, "get"):
        return float(value.get())
    return float(value)


def backend_name() -> str:
    """'cupy (GPU)' or 'numpy (CPU)' — for logging which path is live."""
    return f"{_BACKEND} ({'GPU' if GPU_AVAILABLE else 'CPU'})"
