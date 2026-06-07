"""
Black-Scholes Option Pricing & Greeks
======================================
Exact implementation of the 1973 formula (NumPy + SciPy).

Greeks are cheap (4 positions, a handful of scalar ops), so they stay on CPU.
The GPU-accelerated kernel in MAMI is the Monte Carlo VaR simulation
(see var_engine.py + compute.py), not this.

Inputs:  S (spot), K (strike), T (years to expiry), sigma (implied vol), r (rate)
Outputs: delta, gamma, vega, theta, price
"""
import numpy as np
from scipy.stats import norm
from typing import Dict


def compute_greeks(
    S: float, K: float, T: float, sigma: float,
    r: float = 0.053, option_type: str = "call",
) -> Dict[str, float]:
    """Compute Black-Scholes price and all four Greeks."""
    if T <= 1e-6:
        if option_type == "call":
            intrinsic = max(S - K, 0.0)
            d = 1.0 if S > K else 0.0
        else:
            intrinsic = max(K - S, 0.0)
            d = -1.0 if S < K else 0.0
        return {"delta": d, "gamma": 0.0, "vega": 0.0, "theta": 0.0, "price": intrinsic}

    sqrt_T = np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    nd1_pdf = norm.pdf(d1)

    if option_type == "call":
        delta = norm.cdf(d1)
        price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        theta = -(S * nd1_pdf * sigma / (2 * sqrt_T)
                  + r * K * np.exp(-r * T) * norm.cdf(d2)) / 365
    else:
        delta = norm.cdf(d1) - 1
        price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
        theta = -(S * nd1_pdf * sigma / (2 * sqrt_T)
                  - r * K * np.exp(-r * T) * norm.cdf(-d2)) / 365

    gamma = nd1_pdf / (S * sigma * sqrt_T)
    vega  = S * nd1_pdf * sqrt_T / 100

    return {
        "delta": round(float(delta), 5),
        "gamma": round(float(gamma), 6),
        "vega":  round(float(vega),  5),
        "theta": round(float(theta), 6),
        "price": round(float(price), 4),
    }


def scale_to_position(
    greeks: Dict[str, float], contracts: int, shares_per_contract: int = 100,
) -> Dict[str, float]:
    """Scale single-share Greeks to the full position."""
    share_scale = contracts * shares_per_contract
    return {
        "delta":          greeks["delta"],
        "gamma":          greeks["gamma"],
        "vega":           greeks["vega"],
        "theta":          greeks["theta"],
        "position_delta": round(greeks["delta"] * share_scale, 1),
        "position_theta": round(greeks["theta"] * share_scale, 2),
        "position_vega":  round(greeks["vega"]  * contracts,   2),
        "option_price":   greeks["price"],
    }
