import requests
import pandas as pd
import time

from datetime import date, timedelta
from calendar import monthrange

from storage import OptionStorage
from config.settings import BASE_ASSET


DERIBIT_API = "https://www.deribit.com/api/v2"


# =========================================================
# EXPIRY CALCULATION
# =========================================================

def to_deribit_expiry(dt: date) -> str:
    """Convert date -> DDMMMYY (Deribit format)"""
    return dt.strftime("%d%b%y").upper()


def calculate_target_expiries(today: date | None = None) -> list[str]:
    if today is None:
        today = date.today()

    expiries = {}

    # 1️⃣ Near-term: today + 3 days
    expiries["near"] = today + timedelta(days=3)

    # 2️⃣ Current month end
    y, m = today.year, today.month
    expiries["month_end"] = date(y, m, monthrange(y, m)[1])

    # 3️⃣ Next month end
    if m == 12:
        ny, nm = y + 1, 1
    else:
        ny, nm = y, m + 1
    expiries["next_month_end"] = date(ny, nm, monthrange(ny, nm)[1])

    # 4️⃣ Quarter end
    q_end_month = ((m - 1) // 3 + 1) * 3
    expiries["quarter_end"] = date(y, q_end_month, monthrange(y, q_end_month)[1])

    return [to_deribit_expiry(d) for d in expiries.values()]


# =========================================================
# DERIBIT FETCH
# =========================================================

def get_deribit_price(asset):
    url = f"{DERIBIT_API}/public/get_index_price"
    params = {"index_name": f"{asset.lower()}_usd"}

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return float(r.json()["result"]["index_price"])
    except Exception as e:
        print(f"[ERROR] Price fetch failed ({asset}): {e}")
        return None


def get_deribit_options(asset, expiry, sleep_sec=0.05):
    inst_resp = requests.get(
        f"{DERIBIT_API}/public/get_instruments",
        params={"currency": asset, "kind": "option"},
        timeout=10
    ).json()

    instruments = [
        i for i in inst_resp.get("result", [])
        if expiry in i["instrument_name"]
        and i["instrument_name"].count("-") == 3
    ]

    rows = []

    for inst in instruments:
        name = inst["instrument_name"]

        try:
            bs = requests.get(
                f"{DERIBIT_API}/public/get_book_summary_by_instrument",
                params={"instrument_name": name},
                timeout=10
            ).json()

            if not bs.get("result"):
                continue

            oi = bs["result"][0].get("open_interest", 0)

            tk = requests.get(
                f"{DERIBIT_API}/public/ticker",
                params={"instrument_name": name},
                timeout=10
            ).json()

            greeks = tk.get("result", {}).get("greeks", {})

            rows.append({
                "Expiry": expiry,
                "Instrument": name,
                "Strike": inst["strike"],
                "Type": inst["option_type"].lower(),
                "OI": oi,
                "Delta": greeks.get("delta", 0.0),
                "Gamma": greeks.get("gamma", 0.0),
            })

            time.sleep(sleep_sec)

        except Exception as e:
            print(f"[WARN] Skip {name}: {e}")

    return pd.DataFrame(rows)


# =========================================================
# MAIN FETCH LOOP
# =========================================================

def fetch_and_store_all_expiries():
    asset = BASE_ASSET
    expiries = calculate_target_expiries()

    print(f"📅 Target expiries: {expiries}")

    spot_price = get_deribit_price(asset)
    if spot_price is None:
        return

    storage = OptionStorage()

    for expiry in expiries:
        print(f"📡 Fetching {asset} options ({expiry})")

        df = get_deribit_options(asset, expiry)

        if df.empty:
            print(f"[WARN] No data for {expiry}")
            continue

        storage.save_snapshot(
            df=df,
            asset=asset,
            spot_price=spot_price
        )


if __name__ == "__main__":
    fetch_and_store_all_expiries()


