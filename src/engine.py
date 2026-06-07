"""
MAMI Simulation Engine
======================
Central orchestrator that replaces the full distributed stack for local demo:

  Production MAMI component     →  Local equivalent
  ─────────────────────────────────────────────────
  FIX / WebSocket connectors    →  GBM price generator
  Apache Kafka                  →  In-process asyncio queue
  Spark Structured Streaming    →  In-process aggregation
  Delta Lake / Databricks       →  In-memory deque per symbol
  RAPIDS / cuDF (GPU)           →  NumPy (CPU)
  Kubernetes microservices      →  Single FastAPI process
"""

import numpy as np
from collections import deque
from datetime import datetime
from typing import Dict, List, Any, Optional

from mami_core.config import (
    PORTFOLIO, INITIAL_PRICES, VOLATILITIES,
    RISK_FREE_RATE, MONTE_CARLO_PATHS, VAR_CONFIDENCE,
    HISTORY_SIZE, ANOMALY_THRESHOLD, FLASH_CRASH_PROB, WARMUP_TICKS,
)
from mami_core.black_scholes import compute_greeks, scale_to_position
from mami_core.var_engine import portfolio_var
from mami_core.anomaly import AnomalyDetector, VolatilityForecaster


class SimulationEngine:
    """
    Generates market data and computes risk in real-time.
    All state is in-memory; designed to feed the WebSocket broadcast loop.
    """

    def __init__(self):
        # ── Price state ────────────────────────────────────────────
        self.prices:   Dict[str, float]  = {s: float(p) for s, p in INITIAL_PRICES.items()}
        self.px_hist:  Dict[str, deque]  = {s: deque(maxlen=HISTORY_SIZE) for s in INITIAL_PRICES}
        self.ret_hist: Dict[str, deque]  = {s: deque(maxlen=HISTORY_SIZE) for s in INITIAL_PRICES}
        self.vol_hist: Dict[str, deque]  = {s: deque(maxlen=120) for s in INITIAL_PRICES}

        # ── ML components ──────────────────────────────────────────
        self.detectors:   Dict[str, AnomalyDetector]   = {s: AnomalyDetector() for s in INITIAL_PRICES}
        self.forecaster:  VolatilityForecaster          = VolatilityForecaster()

        # ── Cached risk outputs ────────────────────────────────────
        self.greeks:    Dict = {}
        self.portfolio: Dict = {}
        self.var:       Dict = {}
        self.alerts:    deque = deque(maxlen=50)
        self.scores:    Dict[str, float] = {s: 0.0 for s in INITIAL_PRICES}
        self.forecasts: Dict = {}

        # ── Simulation state ───────────────────────────────────────
        self.tick: int = 0
        # dt = 1 tick in trading-year units (0.5 s per tick)
        self._dt = 0.5 / (252 * 6.5 * 3600)
        self._crash: Dict[str, int] = {}  # symbol → remaining crash ticks

        # Warm up: run price generator without broadcasting
        for _ in range(WARMUP_TICKS):
            self._advance_prices(silent=True)

        # Initial risk snapshot
        self._compute_greeks()
        self._compute_var()

    # ── Public API ─────────────────────────────────────────────────────────────

    def step(self) -> Dict[str, Any]:
        """Advance one tick; return full serialisable state."""
        self._advance_prices()
        self._run_ml()
        self._compute_greeks()
        if self.tick % 10 == 0:
            self._compute_var()
        self.tick += 1
        return self.snapshot()

    def snapshot(self) -> Dict[str, Any]:
        """Current state dict — safe to JSON-serialise."""
        prices_out: Dict = {}
        for sym, price in self.prices.items():
            hist = list(self.px_hist[sym])
            prev = hist[-2] if len(hist) >= 2 else price
            chg  = (price - prev) / prev * 100 if prev else 0.0
            half_spread = np.random.uniform(0.01, 0.06)
            prices_out[sym] = {
                "price":          round(price, 2),
                "bid":            round(price - half_spread, 2),
                "ask":            round(price + half_spread, 2),
                "spread":         round(2 * half_spread, 4),
                "change_pct":     round(chg, 4),
                "history":        [round(p, 2) for p in hist][-120:],
                "anomaly_score":  self.scores.get(sym, 0.0),
                "forecast":       self.forecasts.get(sym, {}),
            }

        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "tick":      self.tick,
            "prices":    prices_out,
            "greeks":    self.greeks,
            "portfolio": self.portfolio,
            "var":       self.var,
            "alerts":    list(self.alerts)[:20],
        }

    def trigger_crash(self, symbol: Optional[str] = None) -> str:
        """Manually inject a flash crash — useful for live demos."""
        target = symbol if symbol in self.prices else np.random.choice(list(self.prices.keys()))
        self._crash[target] = np.random.randint(4, 8)
        return target

    def history(self, symbol: str) -> Dict:
        """Price + return history for a symbol (for REST endpoint)."""
        if symbol not in self.px_hist:
            return {"error": f"Unknown symbol: {symbol}"}
        rets = list(self.ret_hist[symbol])
        return {
            "symbol":    symbol,
            "prices":    [round(p, 2) for p in self.px_hist[symbol]],
            "returns":   [round(r, 6) for r in rets],
            "current":   round(self.prices[symbol], 2),
            "vol_ann_pct": round(np.std(rets) * np.sqrt(252 * 6.5 * 7200) * 100, 2) if len(rets) > 10 else 0.0,
        }

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _advance_prices(self, silent: bool = False):
        """GBM tick for every symbol; occasional flash-crash shocks."""
        for sym in INITIAL_PRICES:
            S     = self.prices[sym]
            sigma = VOLATILITIES[sym]

            if self._crash.get(sym, 0) > 0:
                # Flash crash: large negative jump
                shock = np.log(1 - np.random.uniform(0.012, 0.028))
                self._crash[sym] -= 1
            elif not silent and np.random.random() < FLASH_CRASH_PROB:
                # Spontaneous crash event
                shock = np.log(1 - np.random.uniform(0.018, 0.040))
                self._crash[sym] = np.random.randint(3, 7)
            else:
                # Standard GBM: drift + diffusion
                drift     = (RISK_FREE_RATE - 0.5 * sigma ** 2) * self._dt
                diffusion = sigma * np.sqrt(self._dt) * np.random.standard_normal()
                shock     = drift + diffusion

            new_S = max(S * np.exp(shock), 1.0)
            ret   = (new_S - S) / S

            self.prices[sym] = new_S
            self.px_hist[sym].append(new_S)
            if not silent:
                self.ret_hist[sym].append(ret)
                self.vol_hist[sym].append(np.random.randint(200, 6000))

    def _run_ml(self):
        """Isolation Forest + EWMA forecaster for every symbol."""
        for sym in INITIAL_PRICES:
            rets = list(self.ret_hist[sym])
            vols = list(self.vol_hist[sym])
            if len(rets) < 15:
                continue

            # Feature engineering
            price_vel  = rets[-1] * 100                                 # % tick change
            if len(vols) >= 10:
                v_mean = np.mean(vols);  v_std = np.std(vols) + 1e-6
                vol_z  = (vols[-1] - v_mean) / v_std
            else:
                vol_z = 0.0
            spread_ratio = 1.0 + abs(rets[-1]) * 80                    # wider on big moves

            score = self.detectors[sym].score(price_vel, vol_z, spread_ratio)
            self.scores[sym] = score

            fc = self.forecaster.update(sym, rets)
            self.forecasts[sym] = fc

            # Emit alert if score breaches threshold
            if score >= ANOMALY_THRESHOLD:
                alert_type = (
                    "FLASH_CRASH" if price_vel < -1.5
                    else "VOL_SPIKE"  if fc.get("status") == "SPIKE"
                    else "ANOMALY"
                )
                alert = {
                    "id":           f"{sym}_{self.tick}",
                    "symbol":       sym,
                    "timestamp":    datetime.utcnow().isoformat() + "Z",
                    "type":         alert_type,
                    "severity":     "HIGH" if score > 0.75 else "MEDIUM",
                    "score":        score,
                    "price":        round(self.prices[sym], 2),
                    "change_pct":   round(price_vel, 4),
                    "spike_prob":   fc.get("spike_probability", 0.0),
                }
                # Deduplicate: max 2 alerts per symbol in last 10
                recent = [a["symbol"] for a in list(self.alerts)[:10]]
                if recent.count(sym) < 2:
                    self.alerts.appendleft(alert)

    def _compute_greeks(self):
        """Black-Scholes Greeks for every option position."""
        g_out: Dict      = {}
        net_delta        = 0.0
        total_theta      = 0.0
        total_vega       = 0.0

        for pos in PORTFOLIO:
            S  = self.prices[pos.symbol]
            T  = max(pos.expiry_days / 365.0, 1e-6)
            raw = compute_greeks(S, pos.strike, T, pos.implied_vol, RISK_FREE_RATE, pos.option_type)
            pg  = scale_to_position(raw, pos.contracts)

            net_delta   += pg["position_delta"]
            total_theta += pg["position_theta"]
            total_vega  += pg["position_vega"]

            g_out[pos.symbol] = {
                **pg,
                "option_type": pos.option_type,
                "strike":      pos.strike,
                "contracts":   pos.contracts,
                "stock_price": round(S, 2),
            }

        self.greeks    = g_out
        self.portfolio = {
            "net_delta":   round(net_delta,   1),
            "total_theta": round(total_theta, 2),
            "total_vega":  round(total_vega,  2),
        }

    def _compute_var(self):
        """Monte Carlo portfolio VaR/CVaR (5 000 paths, CPU)."""
        positions = [
            {
                "price":          self.prices[pos.symbol],
                "vol":            VOLATILITIES[pos.symbol],
                "position_delta": self.greeks.get(pos.symbol, {}).get("position_delta", 0),
            }
            for pos in PORTFOLIO
        ]
        self.var = portfolio_var(positions, VAR_CONFIDENCE, MONTE_CARLO_PATHS, RISK_FREE_RATE)
