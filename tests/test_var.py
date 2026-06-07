"""
Tests for VaR and CVaR  (src/var_engine.py)

Run:  pytest tests/test_var.py -v
"""

import numpy as np
import pytest
from mami_core.var_engine import historical_var, monte_carlo_var, portfolio_var


POSITION_VALUE = 10_000.0


# ── Historical VaR / CVaR ─────────────────────────────────────────────────────
# historical_var returns a (var, cvar) tuple, both in dollars (negative = loss).

def test_historical_var_is_negative():
    """VaR represents a loss, so should be negative"""
    returns = list(np.random.default_rng(0).standard_normal(252) * 0.01)
    var, cvar = historical_var(returns, POSITION_VALUE, confidence=0.95)
    assert var < 0

def test_historical_var_95_worse_than_75():
    """95% VaR should be worse (more negative) than 75% VaR"""
    returns = list(np.random.default_rng(1).standard_normal(252) * 0.01)
    var_95, _ = historical_var(returns, POSITION_VALUE, 0.95)
    var_75, _ = historical_var(returns, POSITION_VALUE, 0.75)
    assert var_95 <= var_75

def test_historical_var_too_few_returns_zero():
    """Fewer than 20 observations → (0, 0) (not enough history)"""
    assert historical_var([0.01, -0.02, 0.03], POSITION_VALUE, 0.95) == (0.0, 0.0)

def test_historical_cvar_worse_than_var():
    """CVaR is always ≤ VaR (average of the tail is worse than the threshold)"""
    returns = list(np.random.default_rng(2).standard_normal(252) * 0.01)
    var, cvar = historical_var(returns, POSITION_VALUE, 0.95)
    assert cvar <= var

def test_historical_var_scales_with_position_value():
    """Doubling the position value doubles the dollar VaR"""
    returns = list(np.random.default_rng(3).standard_normal(252) * 0.01)
    var_1x, _ = historical_var(returns, POSITION_VALUE, 0.95)
    var_2x, _ = historical_var(returns, 2 * POSITION_VALUE, 0.95)
    assert var_2x == pytest.approx(2 * var_1x, rel=1e-6)


# ── Monte Carlo VaR ───────────────────────────────────────────────────────────
# monte_carlo_var(S, sigma, r, position_delta, ...) uses numpy's global RNG,
# so we seed np.random before calls that need to be deterministic.

def test_mc_var_returns_dict():
    result = monte_carlo_var(S=185.0, sigma=0.28, r=0.053, position_delta=500, n_paths=1000)
    assert isinstance(result, dict)
    assert "var"        in result
    assert "cvar"       in result
    assert "best_case"  in result
    assert "worst_case" in result

def test_mc_var_negative():
    """Monte Carlo VaR should be negative (represents potential loss)"""
    np.random.seed(0)
    result = monte_carlo_var(S=185.0, sigma=0.28, r=0.053, position_delta=500, n_paths=5000)
    assert result["var"] < 0

def test_mc_cvar_worse_than_var():
    """CVaR ≤ VaR"""
    np.random.seed(1)
    result = monte_carlo_var(S=185.0, sigma=0.28, r=0.053, position_delta=500, n_paths=5000)
    assert result["cvar"] <= result["var"]

def test_mc_var_increases_with_vol():
    """Higher vol → worse (more negative) VaR. Same seed → same shocks, fair compare."""
    np.random.seed(42)
    lo = monte_carlo_var(S=185.0, sigma=0.10, r=0.053, position_delta=500, n_paths=5000)
    np.random.seed(42)
    hi = monte_carlo_var(S=185.0, sigma=0.60, r=0.053, position_delta=500, n_paths=5000)
    assert hi["var"] < lo["var"]

def test_mc_var_increases_with_position_size():
    """Larger position → worse VaR"""
    np.random.seed(42)
    small = monte_carlo_var(S=185.0, sigma=0.28, r=0.053, position_delta=100,  n_paths=5000)
    np.random.seed(42)
    large = monte_carlo_var(S=185.0, sigma=0.28, r=0.053, position_delta=1000, n_paths=5000)
    assert large["var"] < small["var"]

def test_mc_var_reproducible_with_seed():
    """Same global seed → same result (important for audit trails)"""
    np.random.seed(99)
    r1 = monte_carlo_var(S=185.0, sigma=0.28, r=0.053, position_delta=500, n_paths=1000)
    np.random.seed(99)
    r2 = monte_carlo_var(S=185.0, sigma=0.28, r=0.053, position_delta=500, n_paths=1000)
    assert r1["var"]  == r2["var"]
    assert r1["cvar"] == r2["cvar"]

def test_mc_worst_case_below_var():
    """The single worst path should be at least as bad as the VaR threshold"""
    np.random.seed(7)
    result = monte_carlo_var(S=185.0, sigma=0.28, r=0.053, position_delta=500, n_paths=5000)
    assert result["worst_case"] <= result["var"]


# ── Portfolio VaR ─────────────────────────────────────────────────────────────

def test_portfolio_var_negative_for_long():
    """A net-long portfolio has a negative (loss) VaR"""
    np.random.seed(5)
    positions = [
        {"price": 185.0, "vol": 0.28, "position_delta": 500},
        {"price": 880.0, "vol": 0.55, "position_delta": 300},
    ]
    result = portfolio_var(positions, confidence=0.95, n_paths=5000)
    assert result["portfolio_var"]  < 0
    assert result["portfolio_cvar"] <= result["portfolio_var"]

def test_portfolio_var_skips_zero_delta():
    """Positions with ~zero delta don't contribute; empty book → 0 VaR"""
    result = portfolio_var([{"price": 100.0, "vol": 0.3, "position_delta": 0.0}], n_paths=5000)
    assert result["portfolio_var"] == 0.0
