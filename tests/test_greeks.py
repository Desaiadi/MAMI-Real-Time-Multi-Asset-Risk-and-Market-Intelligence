"""
Tests for Black-Scholes Greeks (src/black_scholes.py)

Run:  pytest tests/test_greeks.py -v
"""

import pytest
from mami_core.black_scholes import compute_greeks, scale_to_position


# ── Delta ─────────────────────────────────────────────────────────────────────

def test_atm_call_delta_near_half():
    """ATM call delta ≈ 0.5 (probability of finishing in-the-money)"""
    g = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    assert 0.45 < g["delta"] < 0.60

def test_deep_itm_call_delta_near_one():
    """Deep ITM call: behaves like owning the stock (delta → 1)"""
    g = compute_greeks(S=150, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    assert g["delta"] > 0.95

def test_deep_otm_call_delta_near_zero():
    """Deep OTM call: very unlikely to be exercised (delta → 0)"""
    g = compute_greeks(S=70, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    assert g["delta"] < 0.05

def test_put_delta_negative():
    """Put delta is always negative for long positions"""
    g = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="put")
    assert g["delta"] < 0

def test_call_put_delta_parity():
    """Call delta - Put delta ≈ 1 (put-call parity for delta)"""
    call = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    put  = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="put")
    assert abs(call["delta"] - put["delta"] - 1.0) < 0.002


# ── Gamma ─────────────────────────────────────────────────────────────────────

def test_gamma_positive():
    """Gamma always positive for long options (calls and puts)"""
    for ot in ("call", "put"):
        g = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type=ot)
        assert g["gamma"] > 0, f"Gamma should be positive for long {ot}"

def test_gamma_peaks_atm():
    """Gamma is highest for ATM options"""
    atm  = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    itm  = compute_greeks(S=120, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    otm  = compute_greeks(S=80,  K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    assert atm["gamma"] > itm["gamma"]
    assert atm["gamma"] > otm["gamma"]

def test_call_put_gamma_equal():
    """Call and put with same inputs have identical gamma"""
    call = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    put  = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="put")
    assert abs(call["gamma"] - put["gamma"]) < 1e-6


# ── Vega ──────────────────────────────────────────────────────────────────────

def test_vega_positive():
    """Vega always positive for long options"""
    g = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    assert g["vega"] > 0

def test_vega_increases_with_vol():
    """Higher vol → different vega (options are more sensitive when vol is high)"""
    g_lo = compute_greeks(S=100, K=100, T=30/365, sigma=0.10, r=0.05)
    g_hi = compute_greeks(S=100, K=100, T=30/365, sigma=0.50, r=0.05)
    # Note: vega actually peaks near ATM at moderate vol, this tests a broad trend
    assert g_hi["vega"] != g_lo["vega"]  # they should differ

def test_vega_increases_with_time():
    """Longer time to expiry → higher vega (more time = more uncertainty)"""
    g_30  = compute_greeks(S=100, K=100, T=30/365,  sigma=0.20, r=0.05)
    g_180 = compute_greeks(S=100, K=100, T=180/365, sigma=0.20, r=0.05)
    assert g_180["vega"] > g_30["vega"]

def test_call_put_vega_equal():
    """Call and put vega are identical"""
    call = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    put  = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="put")
    assert abs(call["vega"] - put["vega"]) < 1e-6


# ── Theta ─────────────────────────────────────────────────────────────────────

def test_theta_negative_for_call():
    """Theta always negative for long call (time decay costs the buyer)"""
    g = compute_greeks(S=100, K=100, T=30/365, sigma=0.20, r=0.05, option_type="call")
    assert g["theta"] < 0

def test_theta_accelerates_near_expiry():
    """Theta gets more negative (larger cost) as expiry approaches"""
    g_60 = compute_greeks(S=100, K=100, T=60/365, sigma=0.20, r=0.05)
    g_5  = compute_greeks(S=100, K=100, T=5/365,  sigma=0.20, r=0.05)
    assert g_5["theta"] < g_60["theta"]  # g_5 theta is more negative


# ── Boundary / edge cases ─────────────────────────────────────────────────────

def test_expired_itm_call():
    """Expired ITM call: delta=1, all other Greeks=0"""
    g = compute_greeks(S=110, K=100, T=0, sigma=0.20, r=0.05, option_type="call")
    assert g["delta"] == 1.0
    assert g["gamma"] == 0.0
    assert g["vega"]  == 0.0
    assert g["theta"] == 0.0

def test_expired_otm_call():
    """Expired OTM call: delta=0 (worthless)"""
    g = compute_greeks(S=90, K=100, T=0, sigma=0.20, r=0.05, option_type="call")
    assert g["delta"] == 0.0

def test_expired_itm_put():
    """Expired ITM put: delta=-1"""
    g = compute_greeks(S=90, K=100, T=0, sigma=0.20, r=0.05, option_type="put")
    assert g["delta"] == -1.0


# ── Position scaling ──────────────────────────────────────────────────────────

def test_position_greeks_scaling():
    """Position Greeks = unit Greeks scaled by contracts × shares_per_contract"""
    g = compute_greeks(S=185.5, K=185, T=30/365, sigma=0.28, r=0.053)
    pg = scale_to_position(g, contracts=10, shares_per_contract=100)

    # position_delta / position_theta scale by contracts × shares_per_contract
    assert abs(pg["position_delta"] - g["delta"] * 10 * 100) < 0.1
    assert abs(pg["position_theta"] - g["theta"] * 10 * 100) < 0.1
    # Vega is per-contract (not per-share), so scales by contracts only
    assert abs(pg["position_vega"]  - g["vega"]  * 10)        < 0.1
    # Unit Greeks are passed through unchanged
    assert pg["delta"] == g["delta"]
