import sqlite3
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
import pandas as pd


class OptionStorage:
    def __init__(self, db_dir="database"):
        self.live_path = Path(db_dir) / "live.db"
        self.archive_path = Path(db_dir) / "archive.db"
        self.live_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_live_db()
        self._init_archive_db()

    # -----------------------------
    # DB INIT
    # -----------------------------
    def _init_live_db(self):
        with sqlite3.connect(self.live_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS oi_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    spot_price REAL NOT NULL,
                    expiry TEXT NOT NULL,
                    expiry_iso TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    strike REAL NOT NULL,
                    type TEXT CHECK(type IN ('call','put')) NOT NULL,
                    oi REAL NOT NULL,
                    delta REAL NOT NULL,
                    gamma REAL NOT NULL,
                    theta REAL NOT NULL,  
                    vega REAL NOT NULL,  
                    iv REAL NOT NULL,
                    UNIQUE(timestamp, instrument)
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ts_asset ON oi_snapshots (timestamp, asset)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_expiry_iso ON oi_snapshots (expiry_iso)")
            conn.commit()

    def _init_archive_db(self):
        with sqlite3.connect(self.archive_path) as conn:
            # 보관용 DB이므로 UNIQUE 제약조건은 제거하여 유연하게 저장합니다.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS oi_snapshots_archive (
                    id INTEGER,
                    timestamp TEXT,
                    asset TEXT,
                    spot_price REAL,
                    expiry TEXT,
                    expiry_iso TEXT,
                    instrument TEXT,
                    strike REAL,
                    type TEXT,
                    oi REAL,
                    delta REAL,
                    gamma REAL,
                    theta REAL,  
                    vega REAL,
                    iv REAL
                );
            """)
            conn.commit()

    # -----------------------------
    # MAINTENANCE (ARCHIVE & CLEANUP)
    # -----------------------------
    def maintain_db(self, live_days=7, archive_retain_days=30):
        """
        Theta 분석을 위해 최근 7일치 데이터는 live.db에 유지.
        만기가 지났거나 7일이 넘은 데이터는 archive.db로 이동 후 삭제.
        """
        today_dt = datetime.now(timezone.utc)
        today_str = today_dt.date().isoformat()
        # 7일 전 기준 시각 (Theta 분석을 위한 데이터 보존 기간)
        cutoff_ts = (today_dt - timedelta(days=live_days)).isoformat()

        # 1. Live -> Archive 이동 및 삭제
        with sqlite3.connect(self.live_path) as conn_live:
            # 만기가 지났거나 수집한 지 7일이 넘은 데이터 선택
            query = "SELECT * FROM oi_snapshots WHERE expiry_iso < ? OR timestamp < ?"
            to_move_df = pd.read_sql(query, conn_live, params=[today_str, cutoff_ts])
            
            if not to_move_df.empty:
                with sqlite3.connect(self.archive_path) as conn_arch:
                    to_move_df.to_sql("oi_snapshots_archive", conn_arch, if_exists="append", index=False)
                
                conn_live.execute("DELETE FROM oi_snapshots WHERE expiry_iso < ? OR timestamp < ?", [today_str, cutoff_ts])
                print(f"📦 Archived and deleted {len(to_move_df)} rows from live.db")

        # 2. Old Archive Data 삭제 (영구 삭제)
        archive_limit = (today_dt - timedelta(days=archive_retain_days)).isoformat()
        with sqlite3.connect(self.archive_path) as conn_arch:
            cursor = conn_arch.execute("DELETE FROM oi_snapshots_archive WHERE timestamp < ?", [archive_limit])
            if cursor.rowcount > 0:
                print(f"🗑️ Deleted {cursor.rowcount} old rows from archive.db")

        # 3. 🚀 VACUUM 처리 (물리적 파일 크기 축소 핵심)
        for path in [self.live_path, self.archive_path]:
            try:
                conn = sqlite3.connect(path)
                conn.isolation_level = None  
                conn.execute("VACUUM")
                conn.close()
                print(f"🧹 Vacuumed: {path.name}")
            except Exception as e:
                print(f"[WARN] Vacuum failed for {path}: {e}")
                
    # -----------------------------
    # SAVE
    # -----------------------------
    def save_snapshot(self, df, asset, spot_price):
        ts = datetime.utcnow().isoformat(timespec="milliseconds")
        df = df.copy()
        df["timestamp"] = ts
        df["asset"] = asset
        df["spot_price"] = spot_price
        df["expiry_iso"] = df["Expiry"].apply(
            lambda x: datetime.strptime(x, "%d%b%y").date().isoformat()
        )

        df = df.rename(columns={
            "Expiry": "expiry",
            "Instrument": "instrument",
            "Strike": "strike",
            "Type": "type",
            "OI": "oi",
            "Delta": "delta",
            "Gamma": "gamma",
            "Theta": "theta",
            "Vega": "vega",
            "IV": "iv",
        })

        cols = ["timestamp", "asset", "spot_price", "expiry", "expiry_iso", "instrument", "strike", "type", "oi", "delta", "gamma", "theta", "vega", "iv"]

        with sqlite3.connect(self.live_path) as conn:
            df[cols].to_sql("oi_snapshots", conn, if_exists="append", index=False)

        print(f"📦 Saved {len(df)} rows @ {ts}")
        # self.maintain_db()

    # -----------------------------
    # LOAD
    # -----------------------------
    def load_latest(self, asset="BTC", expiry=None):
        query = "SELECT * FROM oi_snapshots WHERE asset = ? AND timestamp = (SELECT MAX(timestamp) FROM oi_snapshots WHERE asset = ?)"
        params = [asset, asset]
        if expiry:
            query += " AND expiry = ?"
            params.append(expiry)
        with sqlite3.connect(self.live_path) as conn:
            return pd.read_sql(query, conn, params=params)

    def load_timeseries(self, asset="BTC", expiry=None):
        query = "SELECT * FROM oi_snapshots WHERE asset = ?"
        params = [asset]
        if expiry:
            query += " AND expiry = ?"
            params.append(expiry)
        query += " ORDER BY timestamp ASC"
        with sqlite3.connect(self.live_path) as conn:
            return pd.read_sql(query, conn, params=params)

