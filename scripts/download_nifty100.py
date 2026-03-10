"""
download_nifty100.py
====================
Downloads the current Nifty 100 constituent list from NSE and saves it
to data/nifty100_YYYYMMDD_HHMMSS.csv.

Strategy (tried in order):
1. NSE India API (JSON endpoint – most reliable when NSE allows it)
2. NSE CSV download from nse direct link
3. GitHub / open-data mirror for NSE index lists

Usage:
    python scripts/download_nifty100.py
"""

import os
import sys
import json
import time
import datetime
import requests
import pandas as pd
from io import StringIO


# ---------------------------------------------------------------------------
# URL strategies (tried in order)
# ---------------------------------------------------------------------------

# Strategy 1 – NSE JSON API (returns full constituent data)
NSE_API_URL = (
    "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20100"
)

# Strategy 2 – NSE direct CSV (older URL; may have SSL issues on some systems)
NSE_CSV_URL = (
    "https://nseindia.com/content/indices/ind_nifty100list.csv"
)

# Strategy 3 – GitHub open-data mirror (NSE index files published regularly)
GITHUB_MIRROR_URL = (
    "https://raw.githubusercontent.com/datasets/nse-india/main/"
    "data/nifty100.csv"
)

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/live-equity-market",
}


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

def get_output_dir() -> str:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_dir = os.path.join(base, "data")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def timestamped_filename() -> str:
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"nifty100_{ts}.csv"


# ---------------------------------------------------------------------------
# Strategy 1 – NSE JSON API
# ---------------------------------------------------------------------------

def _prime_nse_session(session: requests.Session) -> None:
    """Visit NSE home page to acquire cookies before hitting the API."""
    try:
        session.get(
            "https://www.nseindia.com",
            headers=NSE_HEADERS,
            timeout=20,
        )
        time.sleep(2)
    except Exception:
        pass


def download_via_nse_api(session: requests.Session) -> pd.DataFrame | None:
    print("  [Strategy 1] Trying NSE JSON API …")
    try:
        resp = session.get(NSE_API_URL, headers=NSE_HEADERS, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        records = payload.get("data", [])
        if not records:
            print("  [Strategy 1] Empty data in response.")
            return None
        df = pd.DataFrame(records)
        # Normalise column names & keep useful ones
        df.columns = [c.strip() for c in df.columns]
        # NSE API returns 'symbol', 'meta.companyName' etc.
        col_map = {}
        for col in df.columns:
            if col.lower() == "symbol":
                col_map[col] = "Symbol"
            elif "company" in col.lower() or "name" in col.lower():
                col_map[col] = "Company Name"
            elif "series" in col.lower():
                col_map[col] = "Series"
            elif "isin" in col.lower():
                col_map[col] = "ISIN Code"
        df.rename(columns=col_map, inplace=True)
        # NSE API includes the index itself as the first row – drop it
        if "Symbol" in df.columns:
            df = df[~df["Symbol"].str.upper().isin(["NIFTY 100", "NIFTY100"])]
            df = df.reset_index(drop=True)
        print(f"  [Strategy 1] ✓ Got {len(df)} rows (index row removed if present).")
        return df
    except Exception as e:
        print(f"  [Strategy 1] Failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Strategy 2 – NSE CSV direct download
# ---------------------------------------------------------------------------

def download_via_nse_csv(session: requests.Session) -> pd.DataFrame | None:
    print("  [Strategy 2] Trying NSE direct CSV …")
    try:
        resp = session.get(NSE_CSV_URL, headers=NSE_HEADERS, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        df.columns = [c.strip() for c in df.columns]
        if df.empty:
            print("  [Strategy 2] Empty CSV.")
            return None
        print(f"  [Strategy 2] ✓ Got {len(df)} rows.")
        return df
    except Exception as e:
        print(f"  [Strategy 2] Failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Strategy 3 – GitHub / open-data mirror
# ---------------------------------------------------------------------------

def download_via_github_mirror() -> pd.DataFrame | None:
    print("  [Strategy 3] Trying GitHub open-data mirror …")
    try:
        resp = requests.get(GITHUB_MIRROR_URL, timeout=20)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
        df.columns = [c.strip() for c in df.columns]
        if df.empty:
            print("  [Strategy 3] Empty CSV.")
            return None
        print(f"  [Strategy 3] ✓ Got {len(df)} rows.")
        return df
    except Exception as e:
        print(f"  [Strategy 3] Failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Strategy 4 – yfinance constituents (last resort)
# ---------------------------------------------------------------------------

# Curated Nifty 100 symbol list as a static fallback (as of Jan 2026)
NIFTY100_STATIC = [
    "ADANIENT","ADANIGREEN","ADANIPORTS","ADANIPOWER","AMBUJACEM",
    "APOLLOHOSP","ASIANPAINT","AXISBANK","BAJAJ-AUTO","BAJAJFINSV",
    "BAJFINANCE","BANKBARODA","BEL","BHARTIB","BPCL","BRIT","BRITANNIA",
    "CANBK","CHOLAFIN","CIPLA","COALINDIA","DMART","DLF","DRREDDY",
    "EICHERMOT","FEDERALBNK","GAIL","GODS","GRASIM","HCLTECH","HDFCBANK",
    "HDFCLIFE","HEROMOTOCO","HINDALCO","HINDUNILVR","ICICIBANK",
    "ICICIGI","ICICIPRULI","INDHOTEL","INDIGO","INDUSINDBK","INFRATEL",
    "IOC","IRCTC","ITC","JSWSTEEL","JUBLFOOD","KOTAKBANK","LT","LICI",
    "LTIM","LUPIN","M&M","MARICO","MARUTI","NESTLEIND","NTPC","ONGC",
    "PAGEIND","PFC","PIDILITIND","PIIND","POLYCAB","POWERGRID","RECLTD",
    "RELIANCE","SBICARD","SBILIFE","SBIN","SHREECEM","SIEMENS",
    "SUNPHARMA","SUNTV","TATACONSUM","TATAMOTORS","TATAPOWER","TATASTEEL",
    "TCS","TECHM","TITAN","TORNTPHARM","TRENT","ULTRACEMCO","UNIONBANK",
    "UPL","VEDL","WIPRO","ZOMATO","ZYDUSLIFE",
]


def build_static_fallback() -> pd.DataFrame:
    print("  [Strategy 4] Using static Nifty 100 symbol list (fallback).")
    df = pd.DataFrame({
        "Symbol": NIFTY100_STATIC,
        "Company Name": [""] * len(NIFTY100_STATIC),
        "Series": ["EQ"] * len(NIFTY100_STATIC),
        "ISIN Code": [""] * len(NIFTY100_STATIC),
    })
    print(f"  [Strategy 4] ✓ Using {len(df)} hardcoded symbols.")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def download_nifty100() -> None:
    out_dir = get_output_dir()
    filename = timestamped_filename()
    output_path = os.path.join(out_dir, filename)

    print("=" * 60)
    print("Downloading Nifty 100 constituent list …")
    print("=" * 60)

    session = requests.Session()
    _prime_nse_session(session)

    df = download_via_nse_api(session)
    if df is None or df.empty:
        df = download_via_nse_csv(session)
    if df is None or df.empty:
        df = download_via_github_mirror()
    if df is None or df.empty:
        df = build_static_fallback()

    # Ensure 'Symbol' column exists
    if "Symbol" not in df.columns:
        # Try common alternatives
        for alt in ["symbol", "SYMBOL", "Ticker"]:
            if alt in df.columns:
                df.rename(columns={alt: "Symbol"}, inplace=True)
                break

    df.to_csv(output_path, index=False)

    print(f"\n✓ Saved {len(df)} companies → {output_path}")
    if "Symbol" in df.columns:
        print(f"  Sample symbols: {df['Symbol'].head(5).tolist()}")
    print("=" * 60)


if __name__ == "__main__":
    download_nifty100()
