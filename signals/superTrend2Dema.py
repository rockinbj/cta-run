import math
import pandas_ta as ta


def signal(*args):
    # args = [df, para=[144,10]]
    _df = args[0].copy()
    n1 = int(args[1][0])
    n2 = math.ceil(n1 * 1.1736)
    n3 = float(args[1][1])

    lenDema1 = n1
    lenDema2 = n2
    atrPeriod = n3
    atrFactor = 3.0

    _df["dema1"] = ta.dema(_df["close"], lenDema1)
    _df["dema2"] = ta.dema(_df["close"], lenDema2)
    _df["upper"] = _df[["dema1", "dema2"]].max(axis=1)
    _df["lower"] = _df[["dema1", "dema2"]].min(axis=1)
    _df[["superT_trend", "superT_direction", "superT_long", "superT_short"]] = \
        ta.supertrend(_df["high"], _df["low"], _df["close"], atrPeriod, atrFactor)
    # superTrend direction 1: long, -1: short

    # ==计算信号
    close = _df.iloc[-1]["close"]
    upper = _df.iloc[-1]["upper"]
    lower = _df.iloc[-1]["lower"]
    directionNow = _df.iloc[-1]["superT_direction"]
    directionPre = _df.iloc[-2]["superT_direction"]

    if (directionNow == 1) \
            and (directionPre == -1) \
            and (close > upper):
        signal = 1

    elif (directionNow == -1) \
            and (directionPre == 1) \
            and (close < lower):
        signal = -1

    else:
        signal = None

    return signal
