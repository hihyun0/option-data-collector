import requests
import pandas as pd
import time

from storage import OptionStorage
from config.settings import BASE_ASSET, TARGET_EXPIRY


DERIBIT_API = "https://www.deribit.com/api/v2"


def get_deribit_price(asset):
    """Deribit index price"""
    url = f"{DERIBIT_API}/public/get_index_price"
    params = {"index_name": f"{asset.lower()}_usd"}

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return float(resp.json()["result"]["index_price"])
    except Exception as e:
        print(f"[ERROR] Failed to fetch index price ({asset}): {e}")
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


def fetch_and_store():
    asset = BASE_ASSET
    expiry = TARGET_EXPIRY

    print(f"📡 Fetching {asset} options ({expiry})")

    spot_price = get_deribit_price(asset)
    if spot_price is None:
        return

    df = get_deribit_options(asset, expiry)

    if df.empty:
        print("[WARN] No option data fetched")
        return

    storage = OptionStorage()
    storage.save_snapshot(
        df=df,
        asset=asset,
        spot_price=spot_price
    )


if __name__ == "__main__":
    fetch_and_store()

