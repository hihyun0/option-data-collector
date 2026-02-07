import requests
import pandas as pd
import time
from datetime import date, timedelta, datetime, timezone, time as dtime
from calendar import monthrange
from collections import defaultdict

from storage import OptionStorage
from config.settings import ASSETS


DERIBIT_API = "https://www.deribit.com/api/v2"


# =========================================================
# EXPIRY CALCULATION (TARGET, CALENDAR-BASED)
# =========================================================

def to_deribit_expiry(dt: date) -> str:
    """Convert date -> DDMMMYY (Deribit format)"""
    return dt.strftime("%d%b%y").upper()


def calculate_target_expiries(today_dt: date | None = None) -> list[str]:
    if today_dt is None:
        today_dt = datetime.now(timezone.utc)

    today_date = today_dt.date()
    expiries = {}

    days_until_friday = (4 - today_date.weekday() + 7) % 7
    settlement_time = dtime(8, 0)

    if days_until_friday == 0 and today_dt.time() >= settlement_time:
        days_until_friday = 7

    this_friday = today_date + timedelta(days=days_until_friday)
    expiries["near"] = this_friday

    next_friday = this_friday + timedelta(days=7)
    expiries["next_week"] = next_friday

    y, m = today_date.year, today_date.month
    expiries["month_end"] = date(y, m, monthrange(y, m)[1])

    if m == 12:
        ny, nm = y + 1, 1
    else:
        ny, nm = y, m + 1
    expiries["next_month_end"] = date(ny, nm, monthrange(ny, nm)[1])

    q_end_month = ((m - 1) // 3 + 1) * 3
    expiries["quarter_end"] = date(y, q_end_month, monthrange(y, q_end_month)[1])

    return [to_deribit_expiry(d) for d in sorted(set(expiries.values()))]


# =========================================================
# DERIBIT HELPERS
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


def get_available_expiries_with_oi(asset):
    """
    모든 악기의 요약 정보를 한 번에 가져와서
    만기별 전체 OI 합계를 효율적으로 계산합니다.
    """
    # 개별 악기가 아닌 자산(BTC, ETH) 전체 요약을 한 번에 요청
    url = f"{DERIBIT_API}/public/get_book_summary_by_currency"
    params = {"currency": asset, "kind": "option"}
    
    try:
        resp = requests.get(url, params=params, timeout=10).json()
        results = resp.get("result", [])
    except Exception as e:
        print(f"[ERROR] Failed to fetch book summary: {e}")
        return {}

    expiry_oi = defaultdict(float)

    for item in results:
        try:
            # instrument_name 예: "BTC-27MAR26-80000-C"
            name = item["instrument_name"]
            expiry = name.split("-")[1]
            oi = item.get("open_interest", 0)
            
            expiry_oi[expiry] += oi
        except (IndexError, KeyError):
            continue

    return dict(expiry_oi)


def select_best_expiry(target_expiry: str, expiry_oi_map: dict) -> str | None:
    try:
        target_dt = datetime.strptime(target_expiry, "%d%b%y").date()
    except Exception:
        return None

    candidates = []
    for expiry, oi in expiry_oi_map.items():
        try:
            dt = datetime.strptime(expiry, "%d%b%y").date()
            delta_days = abs((dt - target_dt).days)
            candidates.append((delta_days, -oi, expiry))
        except Exception:
            continue

    if not candidates:
        return None

    candidates.sort()
    return candidates[0][2]


def get_deribit_options(asset, expiry, sleep_sec=0.01):
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

            if not bs.get("result"): continue
            oi = bs["result"][0].get("open_interest", 0)

            tk = requests.get(
                f"{DERIBIT_API}/public/ticker",
                params={"instrument_name": name},
                timeout=10
            ).json()

            greeks = tk.get("result", {}).get("greeks", {})
            rows.append({
                "Expiry": expiry, "Instrument": name, "Strike": inst["strike"],
                "Type": inst["option_type"].lower(), "OI": oi,
                "Delta": greeks.get("delta", 0.0), "Gamma": greeks.get("gamma", 0.0),
                "Theta": greeks.get("theta", 0.0), "Vega": greeks.get("vega", 0.0),
                "IV": tk.get("result", {}).get("mark_iv", 0.0)
            })
            if sleep_sec > 0: time.sleep(sleep_sec)
        except Exception as e:
            print(f"[WARN] Skip {name}: {e}")

    return pd.DataFrame(rows)


# =========================================================
# MAIN FETCH LOOP (MARKET-AWARE)
# =========================================================

def fetch_and_store_all_expiries():
    storage = OptionStorage()
    
    # 🚀 1. 수집 시작 전 DB부터 청소 (순서 변경)
    print("🧹 Running pre-fetch database maintenance...")
    storage.maintain_db()

    for asset in ASSETS:
        print(f"--- 🚀 Starting Fetch for {asset} ---")
        
        target_expiries = calculate_target_expiries()
        expiry_oi_map = get_available_expiries_with_oi(asset)

        resolved_expiries = []
        for target in target_expiries:
            best = select_best_expiry(target, expiry_oi_map)
            if best:
                resolved_expiries.append(best)

        # 🚀 2. 만기 지난 날짜 리스트에서 한 번 더 필터링 (견고함 추가)
        today_str = datetime.now(timezone.utc).date().isoformat()
        resolved_expiries = sorted(
            {e for e in resolved_expiries if datetime.strptime(e, "%d%b%y").date().isoformat() >= today_str},
            key=lambda x: datetime.strptime(x, "%d%b%y")
        )

        spot_price = get_deribit_price(asset)
        if spot_price is None:
            print(f"[ERROR] Could not get spot price for {asset}")
            continue

        for expiry in resolved_expiries:
            print(f"📡 Fetching {asset} options ({expiry})")
            df = get_deribit_options(asset, expiry)
            if df.empty:
                print(f"[WARN] No data for {asset} - {expiry}")
                continue

            storage.save_snapshot(df=df, asset=asset, spot_price=spot_price)
            time.sleep(0.5)


if __name__ == "__main__":
    fetch_and_store_all_expiries()



