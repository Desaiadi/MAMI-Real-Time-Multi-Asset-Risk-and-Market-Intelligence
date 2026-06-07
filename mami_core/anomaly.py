"""
ML Anomaly Detection & Volatility Forecasting
===============================================
Two models mirror the MAMI ML intelligence layer:

  1. Isolation Forest  (sklearn)
     Unsupervised anomaly detector. No labeled "flash crash" data needed.
     Learns what normal market behavior looks like, then flags outliers.
     Features: price velocity, volume z-score, spread multiplier.

  2. Volatility Forecaster  (EWMA-based)
     In production MAMI this is an LSTM trained on 60-second return windows.
     Here we use Exponentially Weighted Moving Average — same concept
     (sequential pattern → spike prediction), faster, no deep-learning deps.
"""

import numpy as np
from collections import deque
from sklearn.ensemble import IsolationForest
from typing import Dict, Optional


class AnomalyDetector:
    """
    Isolation Forest anomaly scorer for a single ticker.

    Scores range 0 → 1 (higher = more anomalous).
    MAMI raises an alert when score ≥ ANOMALY_THRESHOLD (default 0.60).
    """

    def __init__(self, warmup: int = 100, contamination: float = 0.05):
        self.model = IsolationForest(
            n_estimators=100,
            contamination=contamination,
            random_state=42,
        )
        self.buffer: deque = deque(maxlen=500)
        self.warmup   = warmup
        self.fitted   = False
        self.refit_at = 200   # Re-train every N ticks to adapt to new regimes
        self._since_refit = 0

    def score(
        self,
        price_velocity: float,    # % price change this tick
        volume_zscore:  float,    # How unusual is volume right now?
        spread_ratio:   float,    # Bid-ask spread vs baseline
    ) -> float:
        """
        Feed one tick's features and return the anomaly score.
        Returns 0.0 until warmup completes.
        """
        features = [price_velocity, volume_zscore, spread_ratio]
        self.buffer.append(features)
        self._since_refit += 1

        if not self.fitted and len(self.buffer) >= self.warmup:
            self._fit()
        elif self.fitted and self._since_refit >= self.refit_at:
            self._fit()

        if not self.fitted:
            return 0.0

        # IsolationForest.score_samples returns negative anomaly scores
        # More negative = more anomalous; flip sign and normalize to 0-1
        raw = -float(self.model.score_samples([features])[0])
        # Typical raw range: 0.30 (normal) → 0.70 (extreme anomaly)
        normalized = (raw - 0.30) / 0.40
        return round(float(np.clip(normalized, 0.0, 1.0)), 4)

    def _fit(self):
        if len(self.buffer) >= 50:
            self.model.fit(list(self.buffer))
            self.fitted = True
            self._since_refit = 0


class VolatilityForecaster:
    """
    EWMA volatility model with spike probability output.

    In production MAMI, an LSTM looks at the last 60 seconds of returns
    and outputs P(volatility spike in next 5 min). This EWMA approach
    captures the same insight: if recent volatility is much higher than
    historical baseline, a spike is likely continuing.

    lambda_ = decay factor (0.94 is the RiskMetrics standard)
    """

    def __init__(self, lambda_: float = 0.94, spike_mul: float = 2.5):
        self._variance: Dict[str, float] = {}
        self._lambda   = lambda_
        self._spike_mul = spike_mul

    def update(self, symbol: str, returns: list) -> Dict[str, float]:
        """
        Update EWMA variance and return volatility forecast.

        Returns:
            current_vol_ann:    Annualized current volatility (%)
            baseline_vol_ann:   60-tick historical baseline (%)
            vol_ratio:          current / baseline
            spike_probability:  P(vol spike continues) ∈ [0, 1]
            status:             'SPIKE' | 'ELEVATED' | 'NORMAL'
        """
        if len(returns) < 5:
            return {"spike_probability": 0.0, "vol_ratio": 1.0, "status": "NORMAL"}

        latest = float(returns[-1])

        if symbol not in self._variance:
            seed = float(np.var(returns[-20:])) if len(returns) >= 20 else 1e-6
            self._variance[symbol] = max(seed, 1e-10)

        # EWMA update: λ × previous + (1-λ) × latest²
        self._variance[symbol] = (
            self._lambda * self._variance[symbol]
            + (1 - self._lambda) * latest ** 2
        )

        current_vol = float(np.sqrt(self._variance[symbol]))
        window = returns[-60:] if len(returns) >= 60 else returns
        baseline_vol = float(np.std(window)) if len(window) > 1 else current_vol

        vol_ratio = current_vol / max(baseline_vol, 1e-10)

        # Sigmoid-like spike probability based on vol_ratio vs threshold
        t = (vol_ratio - 1.0) / max(self._spike_mul - 1.0, 0.01)
        spike_prob = float(1 / (1 + np.exp(-6 * (t - 0.5))))

        # Annualize (ticks per year ≈ 252 days × 6.5 hrs × 7200 ticks/hr at 0.5s)
        ann_factor = np.sqrt(252 * 6.5 * 3600 / 0.5)
        status = (
            "SPIKE"    if vol_ratio > self._spike_mul else
            "ELEVATED" if vol_ratio > 1.5            else
            "NORMAL"
        )

        return {
            "current_vol_ann":  round(current_vol  * ann_factor * 100, 2),
            "baseline_vol_ann": round(baseline_vol * ann_factor * 100, 2),
            "vol_ratio":        round(vol_ratio, 3),
            "spike_probability": round(spike_prob, 4),
            "status":           status,
        }
