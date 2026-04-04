
import os
import time
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta

SAVE_DIR = "data/raw"
os.makedirs(SAVE_DIR, exist_ok=True)

def fetch_one_day(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    yyyy = dt.strftime("%Y")
    mm   = dt.strftime("%m")
    dd   = dt.strftime("%d")
    url = (
        f"https://nsearchives.nseindia.com/content/fo/"
        f"BhavCopy_NSE_FO_0_0_0_{yyyy}{mm}{dd}_F_0000.csv.zip"
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            print(f"  No data for {date_str} (status {r.status_code})")
            return None

        z = zipfile.ZipFile(io.BytesIO(r.content))
        fname = z.namelist()[0]
        df = pd.read_csv(z.open(fname))

        # FIXED FILTERS — IDO = Index Options, NIFTY = Nifty 50
        df = df[df["FinInstrmTp"] == "IDO"]
        df = df[df["TckrSymb"] == "NIFTY"]

        if df.empty:
            print(f"  No NIFTY options for {date_str}")
            return None

        df = df.rename(columns={
            "StrkPric":    "Strike",
            "XpryDt":      "Expiry",
            "OptnTp":      "OptionType",
            "OpnIntrst":   "OI",
            "TtlTradgVol": "Volume",
            "SttlmPric":   "SettlementPrice",
            "TradDt":      "Date"
        })

        keep = ["Date", "Strike", "Expiry", "OptionType", "OI", "Volume", "SettlementPrice"]
        existing = [c for c in keep if c in df.columns]
        df = df[existing]

        # clean up date column
        df["Date"] = date_str

        return df

    except Exception as e:
        print(f"  Error on {date_str}: {e}")
        return None


def get_trading_days(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end   = datetime.strptime(end_str,   "%Y-%m-%d")
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days


def fetch_all_history(start_str, end_str):
    trading_days = get_trading_days(start_str, end_str)
    total = len(trading_days)
    print(f"Found {total} weekdays to process")
    success = 0
    skipped = 0
    failed  = 0
    for i, date_str in enumerate(trading_days):
        save_path = os.path.join(SAVE_DIR, f"{date_str}.csv")
        if os.path.exists(save_path):
            skipped += 1
            continue
        print(f"[{i+1}/{total}] Fetching {date_str}...", end=" ")
        df = fetch_one_day(date_str)
        if df is not None:
            df.to_csv(save_path, index=False)
            print(f"saved ({len(df)} rows)")
            success += 1
        else:
            failed += 1
        time.sleep(1.5)
    print(f"\nDone. Success:{success}  Skipped:{skipped}  Failed:{failed}")
