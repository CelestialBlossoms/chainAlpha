#!/usr/bin/env python3
"""
Continuous-training anomaly scoring model.
- Accumulates labeled training samples from push outcomes
- Periodically retrains an XGBoost classifier
- Provides real-time scoring at push time

Table: bottom_anomaly_training_samples
  id, address, symbol, event_ts, created_at
  Features: mcap, ath_ratio, vol_1h, vol_24h, sig_pct, pool_liq, pool_ratio,
            holders, age_hours, push_count_24h, candles, peak_min, dd_entry
  Label: gain_pct >= 10 ? 1 : 0
"""
from __future__ import annotations

import json
import os
import pickle
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]

from db_client import db_op

# Feature list (must match training order)
FEATURE_NAMES = [
    "mcap",
    "ath_ratio",
    "vol_1h",
    "vol_24h",
    "sig_pct",
    "pool_liq",
    "pool_ratio",
    "holders",
    "age_hours",
    "push_count_24h",
    "candles",
]

MODEL_PATH = ROOT / "data" / "anomaly_model.pkl"
SCALER_PATH = ROOT / "data" / "anomaly_scaler.pkl"
RETRAIN_MIN_SAMPLES = int(os.getenv("ANOMALY_MODEL_MIN_SAMPLES", "100"))
RETRAIN_INTERVAL_SEC = int(os.getenv("ANOMALY_MODEL_RETRAIN_INTERVAL", "3600"))
MODEL_ENABLED = os.getenv("ANOMALY_MODEL_ENABLED", "1") != "0"
SCORE_THRESHOLD = float(os.getenv("ANOMALY_MODEL_SCORE_THRESHOLD", "0.55"))


def ensure_training_table():
    def _op(conn):
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bottom_anomaly_training_samples (
                id BIGSERIAL PRIMARY KEY,
                address TEXT NOT NULL,
                symbol TEXT,
                event_ts BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now(),
                -- Features
                mcap DOUBLE PRECISION DEFAULT 0,
                ath_ratio DOUBLE PRECISION DEFAULT 0,
                vol_1h DOUBLE PRECISION DEFAULT 0,
                vol_24h DOUBLE PRECISION DEFAULT 0,
                sig_pct DOUBLE PRECISION DEFAULT 0,
                pool_liq DOUBLE PRECISION DEFAULT 0,
                pool_ratio DOUBLE PRECISION DEFAULT 0,
                holders INTEGER DEFAULT 0,
                age_hours DOUBLE PRECISION DEFAULT 0,
                push_count_24h INTEGER DEFAULT 0,
                candles INTEGER DEFAULT 0,
                -- Outcome
                gain_pct DOUBLE PRECISION DEFAULT 0,
                label INTEGER DEFAULT 0,
                -- Metadata
                signal_type TEXT,
                risk_tags TEXT,
                model_version TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_training_samples_address
                ON bottom_anomaly_training_samples(address);
            CREATE INDEX IF NOT EXISTS idx_training_samples_event_ts
                ON bottom_anomaly_training_samples(event_ts);
        """)
    db_op(_op)


def extract_features(extra: dict[str, Any], token: dict[str, Any] | None = None) -> dict[str, float]:
    """Extract normalized feature vector from push extra data."""
    mcap = float(extra.get("current_mcap", 0) or 0)
    ath = float(extra.get("ath_mcap", 0) or 0)
    sig_pct = float(extra.get("price_change_pct", 0) or 0)
    pool_liq = float(extra.get("pool_total_liquidity", 0) or extra.get("pool_liquidity", 0) or 0)
    pool_ratio = float(extra.get("pool_mcap_ratio", 0) or 0)
    holders = int(extra.get("holder_count", 0) or 0)
    age_sec = int(extra.get("age_sec", 0) or 0)
    volume = float(extra.get("breakout_volume_usd", 0) or extra.get("volume_usd", 0) or 0)

    # Derived features
    ath_ratio = ath / max(1, mcap) if ath and mcap else 0
    age_hours = age_sec / 3600 if age_sec else 0

    # Push count in 24h (from extra or default)
    push_count = int(extra.get("abnormal_signal_count", 0) or 0)

    # K-line candles count (approximate from extra)
    candles = int(extra.get("candles", 0) or 0)
    if not candles and token:
        candles = int(token.get("_kline_bars", 0) or 0)

    return {
        "mcap": mcap,
        "ath_ratio": ath_ratio,
        "vol_1h": volume,
        "vol_24h": float(extra.get("volume_24h", volume * 4) or 0),
        "sig_pct": sig_pct,
        "pool_liq": pool_liq,
        "pool_ratio": pool_ratio,
        "holders": holders,
        "age_hours": age_hours,
        "push_count_24h": push_count,
        "candles": candles,
    }


def features_to_array(feats: dict[str, float]) -> np.ndarray:
    return np.array([feats.get(k, 0) for k in FEATURE_NAMES], dtype=np.float64)


def record_training_sample(
    address: str,
    symbol: str,
    event_ts: int,
    features: dict[str, float],
    gain_pct: float,
    signal_type: str = "",
    risk_tags: list[str] | None = None,
):
    """Record a labeled training sample after outcome is known (delayed)."""
    label = 1 if gain_pct >= 10 else 0

    def _op(conn):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bottom_anomaly_training_samples
                (address, symbol, event_ts, mcap, ath_ratio, vol_1h, vol_24h,
                 sig_pct, pool_liq, pool_ratio, holders, age_hours,
                 push_count_24h, candles, gain_pct, label, signal_type, risk_tags)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            address, symbol, event_ts,
            features["mcap"], features["ath_ratio"],
            features["vol_1h"], features["vol_24h"],
            features["sig_pct"], features["pool_liq"], features["pool_ratio"],
            features["holders"], features["age_hours"],
            features["push_count_24h"], features["candles"],
            gain_pct, label, signal_type,
            json.dumps(risk_tags or [], ensure_ascii=False),
        ))
    db_op(_op)


def load_training_data() -> tuple[np.ndarray, np.ndarray]:
    """Load all labeled training samples as (X, y)."""
    def _op(conn):
        cur = conn.cursor()
        cur.execute(f"""
            SELECT {', '.join(FEATURE_NAMES)}, label
            FROM bottom_anomaly_training_samples
            WHERE label IS NOT NULL
            ORDER BY event_ts
        """)
        rows = cur.fetchall()
        if not rows:
            return np.array([]), np.array([])
        X = np.array([list(r[: len(FEATURE_NAMES)]) for r in rows], dtype=np.float64)
        y = np.array([r[len(FEATURE_NAMES)] for r in rows], dtype=np.int32)
        return X, y
    return db_op(_op)


def train_model(force: bool = False) -> dict[str, Any] | None:
    """Train XGBoost model on accumulated data. Returns training stats."""
    if not MODEL_ENABLED:
        return None

    # Check if retrain is needed
    last_train = _last_train_time()
    if not force and last_train and time.time() - last_train < RETRAIN_INTERVAL_SEC:
        return None

    X, y = load_training_data()
    if len(X) < RETRAIN_MIN_SAMPLES:
        print(f"[model] need {RETRAIN_MIN_SAMPLES} samples, have {len(X)}")
        return None

    try:
        from sklearn.preprocessing import StandardScaler
        import xgboost as xgb
    except ImportError:
        print("[model] sklearn/xgboost not installed, using simple logistic regression")
        return _train_simple(X, y)

    # Train/test split
    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train XGBoost
    pos_weight = (len(y_train) - sum(y_train)) / max(1, sum(y_train))
    model = xgb.XGBClassifier(
        n_estimators=100, max_depth=4, learning_rate=0.1,
        scale_pos_weight=pos_weight, random_state=42,
        eval_metric="logloss",
    )
    model.fit(X_train_scaled, y_train, eval_set=[(X_test_scaled, y_test)], verbose=False)

    # Evaluate
    y_pred = model.predict(X_test_scaled)
    y_proba = model.predict_proba(X_test_scaled)[:, 1]
    accuracy = (y_pred == y_test).mean()
    precision = (y_pred[y_pred == 1] == y_test[y_pred == 1]).mean() if sum(y_pred) > 0 else 0
    recall = sum((y_pred == 1) & (y_test == 1)) / max(1, sum(y_test))

    # Feature importance
    importance = dict(zip(FEATURE_NAMES, model.feature_importances_))

    # Save model + scaler
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    with open(SCALER_PATH, "wb") as f:
        pickle.dump(scaler, f)

    stats = {
        "samples": len(X), "train": len(X_train), "test": len(X_test),
        "accuracy": accuracy, "precision": precision, "recall": recall,
        "pos_weight": pos_weight, "importance": importance,
    }
    print(f"[model] trained: {len(X)} samples, acc={accuracy:.2f}, prec={precision:.2f}, recall={recall:.2f}")
    for k, v in sorted(importance.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v:.3f}")

    return stats


def _train_simple(X, y):
    """Fallback: simple logistic regression."""
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import LogisticRegression

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    model = LogisticRegression(max_iter=1000, class_weight="balanced")
    model.fit(X_scaled, y)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    with open(SCALER_PATH, "wb") as f:
        pickle.dump(scaler, f)

    print(f"[model] simple LR trained: {len(X)} samples, acc={model.score(X_scaled, y):.2f}")
    return {"samples": len(X), "model": "logistic"}


def predict_score(features: dict[str, float]) -> tuple[float, bool]:
    """
    Return (success_probability, model_available).
    If model unavailable, returns (0.5, False).
    """
    if not MODEL_ENABLED:
        return 0.5, False

    if not MODEL_PATH.exists():
        return 0.5, False

    try:
        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)
        with open(SCALER_PATH, "rb") as f:
            scaler = pickle.load(f)
    except Exception:
        return 0.5, False

    X = features_to_array(features).reshape(1, -1)
    try:
        X_scaled = scaler.transform(X)
        proba = model.predict_proba(X_scaled)[0, 1]
        return float(proba), True
    except Exception:
        return 0.5, False


def _last_train_time() -> float | None:
    if MODEL_PATH.exists():
        return MODEL_PATH.stat().st_mtime
    return None


def backfill_training_samples():
    """
    Backfill training data from existing push records + performance CSVs.
    Run this once to populate initial training data.
    """
    import csv

    ensure_training_table()

    # Load performance data from all available CSVs
    perf_all = {}
    for fname in sorted((ROOT / "gmgn_outputs").glob("bottom_push_perf_20260*.csv")):
        with fname.open("r", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                addr = r["address"]
                if addr not in perf_all:
                    perf_all[addr] = r

    print(f"Loaded {len(perf_all)} performance records")

    # Query push records and match with performance outcomes
    def _op(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT address, symbol, event_ts, extra, signal_type,
                   current_mcap, ath_mcap, price_change_pct,
                   pool_total_liquidity, pool_mcap_ratio
            FROM bottom_top100_push_records
            ORDER BY event_ts
        """)
        rows = cur.fetchall()

        # Check existing samples to avoid duplicates
        cur.execute("SELECT address, event_ts FROM bottom_anomaly_training_samples")
        existing = {(r[0], r[1]) for r in cur.fetchall()}

        inserted = 0
        for r in rows:
            addr = r[0]
            event_ts = int(r[2] or 0)
            if (addr, event_ts) in existing:
                continue

            extra = r[3] if isinstance(r[3], dict) else {}
            p = perf_all.get(addr, {})
            gain = float(p.get("max_gain_pct", 0) or 0)
            if gain == 0 and not p:
                continue  # No outcome data

            features = extract_features({
                "current_mcap": float(r[5] or 0),
                "ath_mcap": float(r[6] or 0),
                "price_change_pct": float(r[7] or 0),
                "pool_total_liquidity": float(r[8] or 0),
                "pool_mcap_ratio": float(r[9] or 0),
                "holder_count": extra.get("holder_count", 0),
                "age_sec": extra.get("age_sec", 0),
                "breakout_volume_usd": extra.get("breakout_volume_usd", 0),
                "abnormal_signal_count": extra.get("abnormal_signal_count", 0),
                "volume_24h": extra.get("volume_24h", 0),
            })
            record_training_sample(
                addr, r[1] or "?", event_ts, features, gain,
                signal_type=r[4] or "",
                risk_tags=extra.get("risk_tags", []),
            )
            inserted += 1

        print(f"Backfilled {inserted} new training samples (total existing: {len(existing)})")

    db_op(_op)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true", help="Backfill training data from existing records")
    parser.add_argument("--train", action="store_true", help="Train model now")
    parser.add_argument("--stats", action="store_true", help="Show training stats")
    args = parser.parse_args()

    ensure_training_table()

    if args.backfill:
        backfill_training_samples()

    if args.train or args.backfill:
        X, y = load_training_data()
        print(f"Training data: {len(X)} samples, positive={sum(y)} ({sum(y)/max(len(y),1)*100:.0f}%)")
        train_model(force=True)

    if args.stats:
        X, y = load_training_data()
        print(f"Training data: {len(X)} samples, positive={sum(y)} ({sum(y)/max(len(y),1)*100:.0f}%)")
        if MODEL_PATH.exists():
            print(f"Model: {MODEL_PATH.stat().st_size:,} bytes, last trained: {datetime.fromtimestamp(_last_train_time() or 0)}")
        else:
            print("No model file yet. Run --train to create one.")
