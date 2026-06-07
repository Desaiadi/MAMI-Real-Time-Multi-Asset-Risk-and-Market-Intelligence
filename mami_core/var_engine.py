"""
Value at Risk (VaR) and Conditional VaR (CVaR / Expected Shortfall)
====================================================================
GPU-grade, CPU-default. The Monte Carlo kernels run against the ``xp`` array
namespace from compute.py — CuPy on a GPU, NumPy otherwise — so the same code
scales from a laptop to a GPU box without modification.

  historical_var  – replay actual past returns (CPU, tiny)
  monte_carlo_var – N GBM paths for one position   (xp: GPU/CPU)
  portfolio_var   – aggregate simulated P&L across positions (xp: GPU/CPU)

CVaR = average of losses BEYOND the VaR threshold (Basel IV Expected Shortfall).
"""
import numpy as np
from typing import List, Dict, Tuple

from .compute import xp, to_host


def historical_var(
    returns: List[float], position_value: float, confidence: float = 0.95,
) -> Tuple[float, float]:
    """Historical VaR and CVaR (dollars, negative = loss) from past returns."""
    if len(returns) < 20:
        return 0.0, 0.0
    arr = np.array(returns) * position_value
    percentile = (1 - confidence) * 100
    var  = float(np.percentile(arr, percentile))
    tail = arr[arr <= var]
    cvar = float(tail.mean()) if len(tail) > 0 else var
    return round(var, 2), round(cvar, 2)


def monte_carlo_var(
    S: float, sigma: float, r: float, position_delta: float,
    T: float = 1 / 252, n_paths: int = 5_000, confidence: float = 0.95,
) -> Dict[str, float]:
    """Monte Carlo VaR for one position via GBM (delta-linear P&L). xp backend."""
    Z = xp.random.standard_normal(n_paths)
    log_returns = (r - 0.5 * sigma ** 2) * T + sigma * xp.sqrt(T) * Z
    price_changes = S * (xp.exp(log_returns) - 1)
    pnl = price_changes * position_delta

    var  = to_host(xp.percentile(pnl, (1 - confidence) * 100))
    tail = pnl[pnl <= var]
    cvar = to_host(tail.mean()) if int(tail.shape[0]) > 0 else var

    return {
        "var":        round(var, 2),
        "cvar":       round(cvar, 2),
        "best_case":  round(to_host(xp.percentile(pnl, 99)), 2),
        "worst_case": round(to_host(xp.min(pnl)), 2),
        "n_paths":    n_paths,
    }


def portfolio_var(
    positions: List[Dict], confidence: float = 0.95,
    n_paths: int = 5_000, r: float = 0.053,
) -> Dict[str, float]:
    """Portfolio VaR by aggregating simulated P&L across positions. xp backend."""
    total_pnl = xp.zeros(n_paths)
    T = 1 / 252

    for pos in positions:
        if abs(pos.get("position_delta", 0)) < 0.001:
            continue
        S, sigma, delta = pos["price"], pos["vol"], pos["position_delta"]
        Z = xp.random.standard_normal(n_paths)
        log_r = (r - 0.5 * sigma ** 2) * T + sigma * xp.sqrt(T) * Z
        total_pnl = total_pnl + S * (xp.exp(log_r) - 1) * delta

    var  = to_host(xp.percentile(total_pnl, (1 - confidence) * 100))
    tail = total_pnl[total_pnl <= var]
    cvar = to_host(tail.mean()) if int(tail.shape[0]) > 0 else var

    return {
        "portfolio_var":  round(var, 2),
        "portfolio_cvar": round(cvar, 2),
        "confidence":     confidence,
    }
