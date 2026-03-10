# Nifty 100 Quarterly Data Pipeline

Automated pipeline that downloads the **Nifty 100 constituent list** from NSE and
fetches **quarterly financial results** from Yahoo Finance for every company.
Data is stored as versioned CSVs and updated automatically every ~4 months via
GitHub Actions.

---

## Directory Layout

```
.
├── .github/
│   └── workflows/
│       └── quarterly_data.yml    # GitHub Actions schedule
├── data/
│   ├── nifty100_YYYYMMDD_HHMMSS.csv   # Timestamped Nifty 100 lists
│   ├── quarterly/
│   │   ├── RELIANCE.csv          # Per-company quarterly financials
│   │   ├── TCS.csv
│   │   └── …
│   └── run_log.txt               # Cumulative run summary
├── scripts/
│   ├── download_nifty100.py      # Step 1: download constituent list
│   └── fetch_quarterly_financials.py   # Step 2: fetch & merge financials
├── requirements.txt
└── README.md
```

---

## Schedule

The GitHub Actions workflow runs automatically on **Jan 1, May 1, and Sep 1**
(roughly every 4 months). You can also trigger it manually via
**Actions → Quarterly Data Pipeline → Run workflow**.

---

## Data Stored

| File | Contents |
|---|---|
| `data/nifty100_YYYYMMDD_HHMMSS.csv` | Nifty 100 constituents at download time |
| `data/quarterly/<SYMBOL>.csv` | Last 5 quarters of P&L metrics + Market Cap per company |
| `data/run_log.txt` | Timestamp, success/failure counts for every run |

Per-company CSVs are **additive**: new quarter rows are merged on top of
existing history — old data is never overwritten.

---

## Running Locally

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Download latest Nifty 100 list
python scripts/download_nifty100.py

# 3. Fetch quarterly financials (all ~100 symbols — takes ~10–15 min)
python scripts/fetch_quarterly_financials.py

# Quick test: only first 3 symbols
NIFTY_LIMIT=3 python scripts/fetch_quarterly_financials.py
```

---

## Data Source

- **Company list**: [NSE India](https://www.nseindia.com/) – Nifty 100 index constituents
- **Financial data**: [Yahoo Finance](https://finance.yahoo.com/) via the
  [`yfinance`](https://github.com/ranaroussi/yfinance) library

---

## Notes

- NSE occasionally throttles automated downloads. The download script
  uses a cookie-primed session and auto-retries.
- yfinance enforces rate limits. The fetch script includes randomised delays
  (2–5 s between stocks, up to 60 s back-off on 429 errors).
