RUN_NAME = "SuperTrendM1"
IS_TEST = True
IS_TRADE = False

# 假定时间，会产生当时的计算结果，如果不用请置None
# CHEAT_TIME = None
CHEAT_TIME = "2023-03-01 06:00:10"

# 页面杠杆
PAGE_LEVERAGE = 5
# 最大资金比例, 页面杠杆 * 最大比例 = 实际杠杆
MAX_BALANCE = 20 / 100

QUOTE_COIN = "USDT"
SLEEP_LEVEL = "1h"

REPORT_INTERVAL = 30
CALL_ALARM = True

LOG_LEVEL_CONSOLE = "debug"
LOG_LEVEL_FILE = "debug"
LOG_PATH = "log/"


