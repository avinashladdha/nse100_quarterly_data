"""
fetch_quarterly_financials.py
==============================
Reads the most-recent Nifty 100 CSV from data/, fetches the last 5 quarters
of financial data from yfinance for every symbol, and merges results into
per-company CSVs stored under data/quarterly/<SYMBOL>.csv.

On each run, new quarter rows are appended / updated without overwriting
historical rows already on disk, so the CSVs accumulate history over time.

Usage:
    python scripts/fetch_quarterly_financials.py

Environment variables:
    NIFTY_LIMIT   – optional integer; process only the first N symbols
                    (useful for testing, e.g. NIFTY_LIMIT=3)
"""

import os
import sys
import glob
import time
import random
import datetime
import traceback

import pandas as pd
import yfinance as yf


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
QUARTERLY_DIR = os.path.join(DATA_DIR, "quarterly")
RUN_LOG = os.path.join(DATA_DIR, "run_log.txt")

# Max quarters to fetch per run (yfinance returns up to ~4-5 by default)
MAX_QUARTERS = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def latest_nifty100_csv() -> str:
    """Return the path of the most-recently created nifty100_*.csv file."""
    pattern = os.path.join(DATA_DIR, "nifty100_*.csv")
    files = sorted(glob.glob(pattern))  # lexicographic == chronological (YYYYMMDD)
    if not files:
        raise FileNotFoundError(
            f"No nifty100_*.csv found in {DATA_DIR}. "
            "Run download_nifty100.py first."
        )
    return files[-1]


def load_symbols(csv_path: str) -> list[str]:
    """Load and return the list of NSE symbols from the Nifty 100 CSV."""
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    if "Symbol" not in df.columns:
        raise ValueError(
            f"'Symbol' column not found in {csv_path}. "
            f"Available columns: {list(df.columns)}"
        )
    symbols = df["Symbol"].dropna().str.strip().tolist()
    return symbols


def load_existing(symbol: str) -> pd.DataFrame | None:
    """Load an existing per-company quarterly CSV, or return None."""
    path = os.path.join(QUARTERLY_DIR, f"{symbol}.csv")
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, index_col=0, parse_dates=True)
            return df
        except Exception as e:
            print(f"  Warning: could not read existing {path}: {e}")
    return None


def save_quarterly(symbol: str, df: pd.DataFrame) -> None:
    """Save the quarterly DataFrame to data/quarterly/<SYMBOL>.csv."""
    path = os.path.join(QUARTERLY_DIR, f"{symbol}.csv")
    df.to_csv(path)


def merge_dataframes(existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    """
    Merge new quarter rows into existing data.
    - Index = quarter-end dates (datetime)
    - New rows update existing rows for the same date; otherwise are appended.
    - Result is returned sorted descending by date.
    """
    if existing is None or existing.empty:
        return new.sort_index(ascending=False)

    # Align columns (union)
    combined = pd.concat([existing, new])
    # Keep last occurrence for duplicate dates (new data wins)
    combined = combined[~combined.index.duplicated(keep="last")]
    combined.sort_index(ascending=False, inplace=True)
    return combined


# ---------------------------------------------------------------------------
# Market Cap calculation
# ---------------------------------------------------------------------------

def add_market_cap(q_financials: pd.DataFrame, ticker: yf.Ticker) -> pd.DataFrame:
    """
    Appends a 'Market Cap' row to q_financials (columns = quarter dates).
    Uses per-quarter closing price × basic average shares outstanding.
    """
    try:
        hist = ticker.history(period="10y")
        if hist.empty or "Basic Average Shares" not in q_financials.index:
            return q_financials

        market_caps = []
        for date in q_financials.columns:
            ts_naive = pd.to_datetime(date).tz_localize(None)
            hist_index_naive = hist.index.tz_localize(None) if hist.index.tz else hist.index
            mask = hist_index_naive <= ts_naive
            valid_hist = hist[mask]
            if not valid_hist.empty:
                price = valid_hist.iloc[-1]["Close"]
                shares = q_financials.loc["Basic Average Shares", date]
                market_caps.append(price * shares if pd.notna(shares) else None)
            else:
                market_caps.append(None)

        # Use concat to avoid FutureWarning on loc-assignment with all-NA columns
        mcap_row = pd.DataFrame(
            [market_caps],
            index=pd.Index(["Market Cap"]),
            columns=q_financials.columns,
        )
        q_financials = pd.concat([q_financials, mcap_row])
    except Exception as e:
        print(f"  Warning: Market Cap calculation failed: {e}")

    return q_financials


# ---------------------------------------------------------------------------
# Per-symbol fetch
# ---------------------------------------------------------------------------

def fetch_symbol(symbol: str) -> bool:
    """
    Fetch up to MAX_QUARTERS of quarterly financials for one NSE symbol,
    merge with any existing data, and save to disk.

    Returns True on success, False on failure.
    """
    ns_symbol = f"{symbol}.NS"
    ticker = yf.Ticker(ns_symbol)

    # ---- Fetch quarterly_financials with retries ----
    q_financials = None
    max_retries = 3

    for attempt in range(max_retries):
        try:
            q_financials = ticker.quarterly_financials
            if q_financials is not None and not q_financials.empty:
                break
            if attempt < max_retries - 1:
                wait = random.uniform(10, 20)
                print(
                    f"  Empty result for {symbol}. "
                    f"Retrying in {wait:.1f}s (attempt {attempt + 1}/{max_retries}) …"
                )
                time.sleep(wait)
        except Exception as e:
            if "Too Many Requests" in str(e) and attempt < max_retries - 1:
                wait = random.uniform(30, 60)
                print(f"  Rate limited. Waiting {wait:.1f}s …")
                time.sleep(wait)
            else:
                print(f"  Error on attempt {attempt + 1}: {e}")

    if q_financials is None or q_financials.empty:
        print(f"  ✗ No quarterly financials found for {symbol}")
        return False

    # ---- Keep only the last MAX_QUARTERS columns ----
    q_financials = q_financials.iloc[:, :MAX_QUARTERS]

    # ---- Add Market Cap ----
    q_financials = add_market_cap(q_financials, ticker)

    # ---- Transpose so dates become the row index ----
    q_transposed = q_financials.T
    q_transposed.index = pd.to_datetime(q_transposed.index)

    # ---- Merge with existing data ----
    existing = load_existing(symbol)
    merged = merge_dataframes(existing, q_transposed)

    # ---- Save ----
    save_quarterly(symbol, merged)
    print(f"  ✓ Saved {len(merged)} quarters for {symbol}")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    os.makedirs(QUARTERLY_DIR, exist_ok=True)

    # Discover the latest Nifty 100 CSV
    nifty_csv = latest_nifty100_csv()
    print(f"Using Nifty 100 list: {nifty_csv}")

    symbols = load_symbols(nifty_csv)

    # Optional subset for testing
    limit = os.environ.get("NIFTY_LIMIT")
    if limit:
        symbols = symbols[: int(limit)]
        print(f"[TEST MODE] Processing first {len(symbols)} symbols only.")

    total = len(symbols)
    print(f"\nFetching quarterly financials for {total} companies …\n")

    successes, failures = 0, 0
    failed_symbols: list[str] = []

    for idx, symbol in enumerate(symbols, start=1):
        print(f"[{idx}/{total}] Processing {symbol}.NS …")
        try:
            ok = fetch_symbol(symbol)
            if ok:
                successes += 1
            else:
                failures += 1
                failed_symbols.append(symbol)
        except Exception:
            failures += 1
            failed_symbols.append(symbol)
            print(f"  ✗ Unexpected error for {symbol}:")
            traceback.print_exc()

        # Polite delay between symbols
        time.sleep(random.uniform(2, 5))

    # ---- Run log ----
    run_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_lines = [
        f"Run timestamp : {run_ts}",
        f"Nifty 100 CSV : {nifty_csv}",
        f"Total symbols : {total}",
        f"Successes     : {successes}",
        f"Failures      : {failures}",
    ]
    if failed_symbols:
        log_lines.append(f"Failed symbols: {', '.join(failed_symbols)}")
    log_lines.append("=" * 60)

    with open(RUN_LOG, "a") as f:
        f.write("\n".join(log_lines) + "\n\n")

    print("\n" + "\n".join(log_lines))
    print(f"\nRun log updated: {RUN_LOG}")
    print("Done.")


if __name__ == "__main__":
    main()
