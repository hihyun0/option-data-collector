import requests
import pandas as pd
import time

from datetime import date, timedelta, datetime, timezone
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

    # 1️⃣ Near-term: this Friday
    # weekday: 월(0), 화(1), 수(2), 목(3), 금(4), 토(5), 일(6)
    days_until_friday = (4 - today_date.weekday() + 7) % 7

    # 3. 만기일(금요일) 당일 처리 로직
    if days_until_friday == 0:
        # Deribit 정산 시간: UTC 08:00
        settlement_time = time(8, 0)
        
        # UTC 08:00 이후라면 이미 만기 데이터가 소멸 중이므로 차주 금요일(+7일) 선택
        if today_dt.time() >= settlement_time:
            days_until_friday = 7
        else:
            # 08:00 전이라면 오늘(0일 뒤) 만기 데이터 유지
            days_until_friday = 0

    
    target_friday = today_date + timedelta(days=days_until_friday)
    expiries["near"] = target_friday

    # 2️⃣ Current month end
    y, m = today_date.year, today_date.month
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
    실제 Deribit에 존재하는 expiry들과
    expiry별 전체 OI 합계를 반환
    """
    inst = requests.get(
        f"{DERIBIT_API}/public/get_instruments",
        params={"currency": asset, "kind": "option"},
        timeout=10
    ).json().get("result", [])

    expiry_oi = defaultdict(float)

    for i in inst:
        try:
            expiry = i["instrument_name"].split("-")[1]

            bs = requests.get(
                f"{DERIBIT_API}/public/get_book_summary_by_instrument",
                params={"instrument_name": i["instrument_name"]},
                timeout=10
            ).json()

            if not bs.get("result"):
                continue

            oi = bs["result"][0].get("open_interest", 0)
            expiry_oi[expiry] += oi

        except Exception:
            continue

    return dict(expiry_oi)


def select_best_expiry(target_expiry: str, expiry_oi_map: dict) -> str | None:
    """
    target_expiry (calendar-based) 에 가장 가까우면서
    OI가 가장 큰 실제 expiry 선택
    """
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
                "Theta": greeks.get("theta", 0.0),
                "Vega": greeks.get("vega", 0.0),
                "IV": tk.get("mark_iv", 0)
            })

            if sleep_sec > 0:
                time.sleep(sleep_sec)

        except Exception as e:
            print(f"[WARN] Skip {name}: {e}")

    return pd.DataFrame(rows)


# =========================================================
# MAIN FETCH LOOP (MARKET-AWARE)
# =========================================================

def fetch_and_store_all_expiries():
    storage = OptionStorage()
    
    # 0️⃣ 자산 리스트(BTC, ETH)를 순회하도록 반복문 추가
    for asset in ASSETS:
        print(f"--- 🚀 Starting Fetch for {asset} ---")
        
        # 1️⃣ 해당 자산에 맞는 목표 만기 계산
        target_expiries = calculate_target_expiries()

        # 2️⃣ 해당 자산의 실제 Deribit 만기 + OI 정보 가져오기
        expiry_oi_map = get_available_expiries_with_oi(asset)

        # 3️⃣ 만기 매칭 로직 (기존과 동일하되 asset 변수 활용)
        resolved_expiries = []
        for target in target_expiries:
            best = select_best_expiry(target, expiry_oi_map)
            if best:
                resolved_expiries.append(best)

        resolved_expiries = sorted(set(resolved_expiries), key=lambda x: datetime.strptime(x, "%d%b%y"))

        # 4️⃣ 해당 자산의 현재가 가져오기
        spot_price = get_deribit_price(asset)
        if spot_price is None:
            print(f"[ERROR] Could not get spot price for {asset}")
            continue

        # 5️⃣ 만기별 데이터 수집 및 저장
        for expiry in resolved_expiries:
            print(f"📡 Fetching {asset} options ({expiry})")
            df = get_deribit_options(asset, expiry)

            if df.empty:
                print(f"[WARN] No data for {asset} - {expiry}")
                continue

            # storage.py의 save_snapshot은 이미 asset 인자를 받으므로 그대로 사용
            storage.save_snapshot(df=df, asset=asset, spot_price=spot_price)
            
            # API 과부하 방지를 위한 짧은 휴식
            time.sleep(0.5)


if __name__ == "__main__":
    fetch_and_store_all_expiries()



