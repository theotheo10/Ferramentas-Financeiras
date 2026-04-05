#!/usr/bin/env python3
"""
Gera os arquivos estáticos para o módulo IBOV Breadth.

Produz:
  docs/breadth.json     — histórico completo + objeto latest embutido
  docs/ibov_price.json  — série histórica do ^BVSP

Shape de breadth.json:
  {
    "latest": {
      "date": "YYYY-MM-DD",
      "breadth_20": float,
      "breadth_50": float,
      "breadth_200": float,
      "composite": float,
      "regime": str,
      "n_constituents": int
    },
    "data": [
      {
        "date": "YYYY-MM-DD",
        "breadth_20": float|null,
        "breadth_50": float|null,
        "breadth_200": float|null,
        "count_20": int,
        "count_50": int,
        "count_200": int,
        "n_constituents": int
      },
      ...
    ],
    "count": int
  }

Shape de ibov_price.json:
  {
    "data": [{"date": "YYYY-MM-DD", "close": float}, ...],
    "count": int
  }

Idêntico ao que a FastAPI servia em /api/breadth, /api/breadth/latest e /api/ibov.
"""

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT        = Path(__file__).parent.parent
DATA_DIR    = ROOT / "data"
DOCS_DIR    = ROOT / "docs"

PRICES_PATH      = DATA_DIR / "prices.parquet"
BREADTH_PATH     = DATA_DIR / "breadth.parquet"
IBOV_PRICE_PATH  = DATA_DIR / "ibov_price.parquet"

OUT_BREADTH      = DOCS_DIR / "breadth.json"
OUT_IBOV_PRICE   = DOCS_DIR / "ibov_price.json"

# ── Engine imports (unchanged from IBOV-Breadth) ─────────────────────────────

sys.path.insert(0, str(ROOT))

from app.engine import (
    load_or_fetch_prices,
    load_or_compute_breadth,
    incremental_update,
    backfill_missing_tickers,
    historical_backfill,
    classify_regime,
    BREADTH_PATH as ENGINE_BREADTH_PATH,
    PRICES_PATH  as ENGINE_PRICES_PATH,
)
from data.auto_maintenance import run_maintenance, check_ibov_rebalance

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ── JSON serialisation helpers (identical to main.py) ────────────────────────

def breadth_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert breadth DataFrame to clean JSON-serializable records.
    Replicates the breadth_to_records() logic from app/main.py verbatim."""
    records = []
    for idx, row in df.iterrows():
        r = {"date": idx.strftime("%Y-%m-%d")}
        for col in ["breadth_20", "breadth_50", "breadth_200",
                    "count_20", "count_50", "count_200", "n_constituents"]:
            val = row.get(col)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                r[col] = None
            else:
                r[col] = round(float(val), 4) if "breadth" in col else int(val)
        records.append(r)
    return records


def build_latest(df: pd.DataFrame) -> dict:
    """Build the /api/breadth/latest payload.
    Replicates get_breadth_latest() logic from app/main.py verbatim."""
    valid = df[df["breadth_200"].notna()]
    if valid.empty:
        raise ValueError("No valid breadth_200 data found")

    last = valid.iloc[-1]
    b200 = last.get("breadth_200")
    b50  = last.get("breadth_50")
    b20  = last.get("breadth_20")

    def safe(v):
        return round(float(v), 4) if v is not None and not np.isnan(float(v)) else None

    return {
        "date":           valid.index[-1].strftime("%Y-%m-%d"),
        "breadth_20":     safe(b20),
        "breadth_50":     safe(b50),
        "breadth_200":    safe(b200),
        "regime":         classify_regime(b200),
        "n_constituents": int(last.get("n_constituents", 0)),
        # Weights: MA200×50% · MA50×35% · MA20×15%
        # (same formula used in the frontend comp() function)
        "composite": round(
            0.15 * float(b20  or 0) +
            0.35 * float(b50  or 0) +
            0.50 * float(b200 or 0),
            4
        ),
    }


# ── IBOV price (^BVSP) ────────────────────────────────────────────────────────

def update_ibov_price() -> pd.DataFrame:
    """Fetch ^BVSP close prices and update ibov_price.parquet.
    Replicates update_ibov_price() from jobs/daily_update.py verbatim."""
    start = "2014-01-01"
    existing = None

    if IBOV_PRICE_PATH.exists():
        existing = pd.read_parquet(IBOV_PRICE_PATH)
        last     = existing.index.max()
        start    = (last - pd.Timedelta(days=5)).strftime("%Y-%m-%d")

    end = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Fetching ^BVSP from {start} to {end}...")

    raw = yf.download("^BVSP", start=start, end=end,
                      auto_adjust=True, progress=False, threads=False)

    if raw.empty:
        logger.warning("^BVSP: yfinance returned empty — keeping existing cache")
        if existing is not None:
            return existing
        raise RuntimeError("No ^BVSP data available")

    close = raw["Close"].squeeze()
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    close.name       = "close"
    close.index      = pd.to_datetime(close.index).normalize()
    close.index.name = "date"
    new_df = close.dropna().to_frame()

    if existing is not None:
        combined = pd.concat([existing, new_df])
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    else:
        combined = new_df

    combined.to_parquet(IBOV_PRICE_PATH)
    logger.info(f"^BVSP saved — {len(combined)} rows, up to {combined.index.max().date()}")
    return combined


def ibov_price_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert ibov_price DataFrame to JSON records.
    Replicates get_ibov_price() from app/main.py verbatim."""
    return [
        {"date": idx.strftime("%Y-%m-%d"), "close": round(float(row["close"]), 2)}
        for idx, row in df.iterrows()
        if pd.notna(row["close"])
    ]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    first_run = not ENGINE_BREADTH_PATH.exists() or not ENGINE_PRICES_PATH.exists()

    # ── PASSO 1: rebalanceamento ANTES do fetch ──────────────────────────────
    logger.info("Verificando composição do IBOV...")
    rebalanced = check_ibov_rebalance()
    if rebalanced:
        logger.info("Rebalanceamento aplicado — universo atualizado antes do fetch.")

    # ── PASSO 2: fetch / update de preços ────────────────────────────────────
    if first_run:
        logger.info("═══ PRIMEIRA EXECUÇÃO: construindo histórico completo ═══")
        logger.info("Isso vai levar 5–15 minutos. Aguarde.")
        prices  = load_or_fetch_prices(force_refresh=False)
        breadth = load_or_compute_breadth(force_refresh=False)
        logger.info(f"✓ Preços: {prices.shape}")
        logger.info(f"✓ Breadth: {breadth.shape}")
        logger.info(f"✓ Período: {breadth.index.min().date()} → {breadth.index.max().date()}")
    else:
        logger.info("═══ UPDATE INCREMENTAL ═══")
        breadth = incremental_update()
        logger.info(f"✓ Atualizado até {breadth.index.max().date()}")

    # ── PASSO 3: backfill de tickers novos ───────────────────────────────────
    prices = load_or_fetch_prices()
    n_backfilled = backfill_missing_tickers(prices)
    if n_backfilled > 0:
        logger.info(f"✓ Backfill: {n_backfilled} tickers com histórico retroativo adicionado")
        breadth = load_or_compute_breadth(force_refresh=True)
        logger.info(f"✓ Breadth recomputado após backfill: {breadth.shape}")

    # ── PASSO 3.5: backfill histórico (tickers ausentes do parquet inteiro) ──
    logger.info("═══ BACKFILL HISTÓRICO ═══")
    n_hist = historical_backfill()
    if n_hist > 0:
        logger.info(f"✓ Backfill histórico: {n_hist} tickers adicionados, breadth recomputado.")
        breadth = load_or_compute_breadth()

    # ── PASSO 4: manutenção ──────────────────────────────────────────────────
    logger.info("═══ MANUTENÇÃO AUTOMÁTICA ═══")
    prices = load_or_fetch_prices()
    run_maintenance(prices)

    # ── PASSO 5: atualizar preço do IBOV ─────────────────────────────────────
    logger.info("═══ ATUALIZANDO ^BVSP ═══")
    ibov_df = update_ibov_price()

    # ── PASSO 6: serializar breadth.json ─────────────────────────────────────
    logger.info("Serializando breadth.json...")
    records = breadth_to_records(breadth)
    latest  = build_latest(breadth)

    breadth_payload = {
        "latest": latest,
        "data":   records,
        "count":  len(records),
    }
    OUT_BREADTH.write_text(json.dumps(breadth_payload, separators=(",", ":")))
    logger.info(f"✓ breadth.json gravado — {len(records)} linhas, último: {latest['date']}")

    # ── PASSO 7: serializar ibov_price.json ──────────────────────────────────
    logger.info("Serializando ibov_price.json...")
    ibov_records = ibov_price_to_records(ibov_df)
    ibov_payload = {"data": ibov_records, "count": len(ibov_records)}
    OUT_IBOV_PRICE.write_text(json.dumps(ibov_payload, separators=(",", ":")))
    logger.info(f"✓ ibov_price.json gravado — {len(ibov_records)} registros")

    logger.info("Concluído.")


if __name__ == "__main__":
    main()
