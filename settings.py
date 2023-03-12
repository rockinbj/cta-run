RUN_NAME = "SuperTrendM1"
EXCHANGE_ID = "binance"
IS_TEST = True
IS_TRADE = False

# 假定时间，会产生当时的计算结果，如果不用请置None
CHEAT_TIME = None
# CHEAT_TIME = "2023-03-05 10:00:31"

# 页面杠杆
PAGE_LEVERAGE = 5
# 最大资金比例, 页面杠杆 * 最大比例 = 实际杠杆
MAX_BALANCE = 24 / 100

QUOTE_COIN = "USDT"
SLEEP_LEVEL = "1h"

REPORT_INTERVAL = "30m"
CALL_ALARM = True

from pathlib import Path
ROOT_PATH = Path(__file__).resolve().parent
LOG_LEVEL_CONSOLE = "debug"
LOG_LEVEL_FILE = "debug"
LOG_PATH = ROOT_PATH / "log"

SLEEP_LONG = 3
SLEEP_MEDIUM = 0.5
SLEEP_SHORT = 0.05
