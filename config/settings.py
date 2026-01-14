# === Target ===
BASE_ASSET = "BTC"
QUOTE_ASSET = "USDT"
TARGET_EXPIRY = "30JAN26"  # 분석하고 싶은 만기일

# === Parameters ===
STEP_PCT = 0.01  # 시뮬레이션 가격 변동 폭 (1%)
STEPS = 5        # 상하방 시뮬레이션 단계 수
WINDOW = 3       # 감마 계산 범위

NEGATIVE_GAMMA_THRESHOLD = -1e9
HIGH_OI_CHANGE_THRESHOLD = 0.05  # 5%

LAYER2_LOOKBACK = 5 # ‘Δt를 몇 개로 쪼갤 것인가’를 결정
LAYER2_LAMBDA = 0.5

# === AI Behavior ===
ALLOW_PRICE_PREDICTION = False
DEFAULT_MODEL = "gemini"

# Log
DATA_LOG_DIR = "logs"
