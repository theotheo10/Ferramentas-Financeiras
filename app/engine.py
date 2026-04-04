"""
IBOV Breadth Engine
- Fetches prices for full historical universe
- Computes MA20/50/200 per stock
- Aggregates breadth per date using correct period composition
- Handles: missing data, IPOs, delistings, NaN MAs, backfill of new tickers
"""

import time
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta

from data.ibov_composition import IBOV_COMPOSITION_HISTORY
from data.ticker_normalization import normalize_tickers, get_all_historical_tickers

logger = logging.getLogger(__name__)

DATA_DIR      = Path("data")
PRICES_PATH   = DATA_DIR / "prices.parquet"
BREADTH_PATH  = DATA_DIR / "breadth.parquet"
MA_WINDOWS    = [20, 50, 200]

# Minimum days of price history needed to compute MA200 with warmup
MA200_WARMUP_DAYS = 250


# ── Composition helpers ──────────────────────────────────────────────────────

def get_composition_df() -> pd.DataFrame:
    rows = []
    for p in IBOV_COMPOSITION_HISTORY:
        rows.append({
            "start":   pd.Timestamp(p["start"]),
            "end":     pd.Timestamp(p["end"]),
            "tickers": normalize_tickers(p["tickers"]),
        })
    return pd.DataFrame(rows)


def get_constituents_on_date(comp_df: pd.DataFrame, date: pd.Timestamp) -> list[str]:
    """Return the IBOV constituents valid on a given date."""
    mask = (comp_df["start"] <= date) & (comp_df["end"] >= date)
    rows = comp_df[mask]
    if rows.empty:
        return []
    # Use the most recent matching period (handles overlapping periods)
    return rows.sort_values("start").iloc[-1]["tickers"]


def get_current_composition_tickers() -> list[str]:
    """Return the tickers for the current/most recent IBOV period."""
    today = pd.Timestamp.today().normalize()
    comp_df = get_composition_df()
    mask = comp_df["start"] <= today
    active = comp_df[mask].sort_values("start")
    if active.empty:
        return []
    return active.iloc[-1]["tickers"]


# ── Price fetching ───────────────────────────────────────────────────────────

def fetch_prices(tickers: list[str], start: str, end: str, chunk_size: int = 8) -> pd.DataFrame:
    """
    Download adjusted close prices for all tickers.
    Chunked to avoid rate limits. Retries on failure.
    """
    all_frames = []
    chunks = [tickers[i:i+chunk_size] for i in range(0, len(tickers), chunk_size)]
    total  = len(chunks)

    for idx, chunk in enumerate(chunks):
        logger.info(f"Fetching chunk {idx+1}/{total}: {chunk}")
        for attempt in range(4):
            try:
                raw = yf.download(
                    chunk,
                    start=start,
                    end=end,
                    auto_adjust=True,
                    progress=False,
                    threads=False,
                )
                if raw.empty:
                    break

                # yfinance 1.x always returns MultiIndex (field, ticker)
                if isinstance(raw.columns, pd.MultiIndex):
                    close = raw["Close"]
                    # Single ticker returns Series — convert to DataFrame
                    if isinstance(close, pd.Series):
                        close = close.to_frame(name=chunk[0])
                else:
                    close = raw[["Close"]].rename(columns={"Close": chunk[0]})

                all_frames.append(close)
                break

            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {chunk}: {e}")
                time.sleep(2 ** attempt)

        time.sleep(0.3)

    if not all_frames:
        return pd.DataFrame()

    prices = pd.concat(all_frames, axis=1)
    prices = prices.loc[:, ~prices.columns.duplicated()]
    prices = prices.sort_index()
    prices = prices.asfreq("B").ffill(limit=5)

    return prices


def load_or_fetch_prices(force_refresh: bool = False) -> pd.DataFrame:
    """Load cached prices or fetch from scratch."""
    if PRICES_PATH.exists() and not force_refresh:
        logger.info("Loading cached prices...")
        return pd.read_parquet(PRICES_PATH)

    logger.info("Fetching full price history (this takes a few minutes)...")

    all_tickers = get_all_historical_tickers(IBOV_COMPOSITION_HISTORY)
    logger.info(f"Universe size: {len(all_tickers)} tickers")

    # Fetch from 2013-07 to give MA200 room to warm up before Jan 2014
    start = "2013-07-01"
    end   = datetime.today().strftime("%Y-%m-%d")

    prices = fetch_prices(all_tickers, start, end)

    DATA_DIR.mkdir(exist_ok=True)
    prices.to_parquet(PRICES_PATH)
    logger.info(f"Prices saved to {PRICES_PATH} — shape: {prices.shape}")

    return prices


# ── Backfill of new tickers ──────────────────────────────────────────────────

def backfill_missing_tickers(prices: pd.DataFrame) -> int:
    """
    After a rebalance, new tickers may be in the composition but absent from
    prices.parquet (or present with insufficient history for MA200).
    This function fetches 250 days of history for each such ticker and merges
    them into prices.parquet.

    Returns the number of tickers successfully backfilled.
    """
    current_tickers = get_current_composition_tickers()
    if not current_tickers:
        return 0

    today      = datetime.today()
    start_date = (today - timedelta(days=MA200_WARMUP_DAYS + 30)).strftime("%Y-%m-%d")
    end_date   = today.strftime("%Y-%m-%d")

    # Find tickers that are missing entirely or have <200 days of data
    missing = []
    for t in current_tickers:
        if t not in prices.columns:
            missing.append(t)
            continue
        n_valid = prices[t].notna().sum()
        if n_valid < MA200_WARMUP_DAYS:
            missing.append(t)

    if not missing:
        logger.info("Backfill: todos os tickers da composição atual têm histórico suficiente.")
        return 0

    logger.info(f"Backfill necessário para {len(missing)} tickers: {missing}")

    new_prices = fetch_prices(missing, start=start_date, end=end_date)
    if new_prices.empty:
        logger.warning("Backfill: nenhum dado retornado pelo Yahoo Finance.")
        return 0

    # Merge into existing prices
    # For tickers already present (but with insufficient data), update/extend
    for col in new_prices.columns:
        if col in prices.columns:
            # Prefer new data; keep old data where new is NaN
            prices[col] = new_prices[col].combine_first(prices[col])
        else:
            prices[col] = new_prices[col]

    # Reindex to cover the full date range
    full_index = prices.index.union(new_prices.index)
    prices = prices.reindex(full_index).sort_index()
    prices = prices.asfreq("B").ffill(limit=5)

    prices.to_parquet(PRICES_PATH)
    logger.info(f"Backfill completo — prices.parquet atualizado: {prices.shape}")

    n_success = sum(
        1 for t in missing
        if t in new_prices.columns and new_prices[t].notna().sum() > 0
    )
    if n_success < len(missing):
        failed = [t for t in missing if t not in new_prices.columns or new_prices[t].notna().sum() == 0]
        logger.warning(f"Backfill falhou para {len(failed)} tickers (não disponíveis no Yahoo): {failed}")

    return n_success


# ── MA computation ───────────────────────────────────────────────────────────

def compute_moving_averages(prices: pd.DataFrame) -> dict[int, pd.DataFrame]:
    """
    Compute rolling MAs for each window.
    min_periods=window ensures no MA until enough history exists.
    This prevents IPO distortion.
    """
    mas = {}
    for w in MA_WINDOWS:
        mas[w] = prices.rolling(window=w, min_periods=w).mean()
    return mas


# ── Breadth computation ──────────────────────────────────────────────────────

def compute_breadth(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily breadth for MA20/50/200 using time-correct IBOV composition.

    For each trading day:
    1. Look up valid IBOV constituents for that date
    2. For each MA window, count stocks with price > MA
    3. Divide by valid (non-NaN) count

    Note on NaN breadth_200: tickers with <200 days of history will have NaN
    MA200. This is correct behaviour — we do not impute or interpolate.
    For recently renamed tickers (e.g. EMBJ3 = ex-EMBR3), Yahoo Finance only
    serves data from the rename date, so MA200 will be NaN until 200 trading
    days have elapsed. This is a Yahoo Finance data limitation, not a bug.
    """
    comp_df = get_composition_df()
    mas     = compute_moving_averages(prices)
    results = []

    for date in prices.index:
        constituents = get_constituents_on_date(comp_df, date)
        if not constituents:
            continue

        available = [t for t in constituents if t in prices.columns]
        if len(available) < 10:
            continue

        row = {"date": date}

        for w, ma in mas.items():
            px = prices.loc[date, available]
            m  = ma.loc[date, available]

            valid_mask = px.notna() & m.notna()
            n_valid    = valid_mask.sum()

            if n_valid < 5:
                row[f"breadth_{w}"] = None
                row[f"count_{w}"]   = 0
                continue

            above = (px[valid_mask] > m[valid_mask]).sum()
            row[f"breadth_{w}"] = float(above) / float(n_valid)
            row[f"count_{w}"]   = int(n_valid)

        row["n_constituents"] = len(available)
        results.append(row)

    df = pd.DataFrame(results).set_index("date")
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    return df


def load_or_compute_breadth(force_refresh: bool = False) -> pd.DataFrame:
    """Load cached breadth or compute from scratch."""
    if BREADTH_PATH.exists() and not force_refresh:
        logger.info("Loading cached breadth data...")
        return pd.read_parquet(BREADTH_PATH)

    logger.info("Computing breadth (this is the slow step)...")
    prices  = load_or_fetch_prices()
    breadth = compute_breadth(prices)

    breadth.to_parquet(BREADTH_PATH)
    logger.info(f"Breadth saved — shape: {breadth.shape}")

    return breadth


# ── Incremental daily update ─────────────────────────────────────────────────

def incremental_update() -> pd.DataFrame:
    """
    Update prices and breadth for the latest trading day(s) only.
    Much faster than full recompute.
    """
    logger.info("Running incremental update...")

    prices  = load_or_fetch_prices()
    breadth = load_or_compute_breadth()

    last_price_date = prices.index.max()
    today           = pd.Timestamp.today().normalize()

    all_tickers = get_all_historical_tickers(IBOV_COMPOSITION_HISTORY)

    # Always fetch last 5 days to catch any corrections or late arrivals
    fetch_start = (last_price_date - timedelta(days=5)).strftime("%Y-%m-%d")
    fetch_end   = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    new_px = fetch_prices(all_tickers, start=fetch_start, end=fetch_end)

    if not new_px.empty:
        # Add any new columns (tickers) that appeared after the last fetch
        for col in new_px.columns:
            if col not in prices.columns:
                prices[col] = new_px[col]
            else:
                prices[col].update(new_px[col])

        # Append rows that don't exist yet
        new_rows = new_px[~new_px.index.isin(prices.index)]
        if not new_rows.empty:
            prices = pd.concat([prices, new_rows])

        prices = prices.sort_index()
        prices.to_parquet(PRICES_PATH)
        logger.info(f"Prices updated to {prices.index.max().date()}")

    # Recompute breadth for last 30 days (covers any gaps or corrections)
    cutoff      = today - timedelta(days=30)
    old_breadth = breadth[breadth.index < cutoff]

    # Need 250 days of prices for MA200 warmup
    recent_prices = prices[prices.index >= cutoff - timedelta(days=MA200_WARMUP_DAYS + 10)]
    new_breadth   = compute_breadth(recent_prices)

    breadth = pd.concat([old_breadth, new_breadth])
    breadth = breadth[~breadth.index.duplicated(keep="last")].sort_index()
    breadth.to_parquet(BREADTH_PATH)

    logger.info(f"Breadth updated to {breadth.index.max().date()}")
    return breadth


# ── Regime classification ────────────────────────────────────────────────────

def classify_regime(breadth_200: float | None) -> str:
    if breadth_200 is None or np.isnan(breadth_200):
        return "unknown"
    if breadth_200 >= 0.80:
        return "overbought"
    if breadth_200 >= 0.60:
        return "bull"
    if breadth_200 >= 0.40:
        return "neutral"
    if breadth_200 >= 0.20:
        return "bear"
    return "capitulation"
