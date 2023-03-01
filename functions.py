import time
import datetime as dt

import ccxt
import pandas as pd
from joblib import Parallel, delayed
import requests

from exchangeConfig import *
from symbolsConfig import *
from logger import *
from settings import *

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)
logger = logging.getLogger("app.func")


def sendReport(exchangeId, symbolsConfig, interval=REPORT_INTERVAL):
    exchange = getattr(ccxt, exchangeId)(EXCHANGE_CONFIG)
    symbols = list(symbolsConfig.keys())

    nowMinute = dt.datetime.now().minute
    nowSecond = dt.datetime.now().second

    if (nowMinute % interval == 0) and (nowSecond == 59):
        logger.debug("开始发送报告")

        pos = getOpenPosition(exchange)
        bTot, bBal, bPos = getAccountBalance(exchange)
        bal = round(float(bTot.iloc[0]["availableBalance"]), 2)
        wal = round(float(bTot.iloc[0]["totalMarginBalance"]), 2)

        msg = f"### {RUN_NAME} - 策略报告\n\n"

        if pos.shape[0] > 0:
            pos = pos[
                [
                    "side",
                    "notional",
                    "percentage",
                    "unrealizedPnl",
                    "entryPrice",
                    "markPrice",
                    "liquidationPrice",
                    "datetime",
                    "leverage",
                ]
            ]

            pos.rename(
                columns={
                    "side": "持仓方向",
                    "notional": "持仓价值(U)",
                    "percentage": "盈亏比例(%)",
                    "unrealizedPnl": "未实现盈亏(U)",
                    "entryPrice": "开仓价格(U)",
                    "markPrice": "当前价格(U)",
                    "liquidationPrice": "爆仓价格(U)",
                    "datetime": "开仓时间",
                    "leverage": "页面杠杆",
                },
                inplace=True,
            )

            pos.sort_values(by="盈亏比例(%)", ascending=False, inplace=True)
            d = pos.to_dict(orient="index")

            msg += f"#### 账户权益 : {wal}U\n"
            msg += f'#### 当前持币 : {", ".join(list(d.keys()))}'

            for k, v in d.items():
                msg += f"""
##### {k}
 - 持仓方向    : {v["持仓方向"]}
 - 持仓价值(U) : {v["持仓价值(U)"]}
 - 盈亏比例(%) : {v["盈亏比例(%)"]}
 - 未实现盈亏(U) : {v["未实现盈亏(U)"]}
 - 开仓价格(U) : {v["开仓价格(U)"]}
 - 当前价格(U) : {v["当前价格(U)"]}
 - 爆仓价格(U) : {v["爆仓价格(U)"]}
 - 开仓时间 : {v["开仓时间"]}
 - 页面杠杆 : {v["页面杠杆"]}
"""
        else:
            msg += "#### 当前空仓\n"

        msg += f"#### 账户余额 : {round(bal, 2)}U\n"
        msg += f"#### 页面杠杆 : {PAGE_LEVERAGE}\n"
        msg += f"#### 资金上限 : {MAX_BALANCE * 100}%\n"
        msg += f"#### 实际杠杆 : {round(PAGE_LEVERAGE * MAX_BALANCE, 2)}\n"

        r = sendMixin(msg, _type="PLAIN_POST")


def sendMixin(msg, _type="PLAIN_TEXT"):
    token = MIXIN_TOKEN
    url = f"https://webhook.exinwork.com/api/send?access_token={token}"

    value = {
        'category': _type,
        'data': msg,
    }

    try:
        r = requests.post(url, data=value, timeout=2).json()
    except Exception as err:
        logger.exception(err)


def callAlarm(strategyName=RUN_NAME, content="存在严重风险项, 请立即检查"):
    url = "http://api.aiops.com/alert/api/event"
    apiKey = CALLKEY
    eventId = str(int(time.time()))
    stragetyName = strategyName
    content = content
    para = f"?app={apiKey}&eventType=trigger&eventId={eventId}&priority=3&host={stragetyName}&alarmContent={content}"

    try:
        r = requests.post(url + para)
        if r.json()["result"] != "success":
            sendAndPrintError(f"电话告警触发失败, 可能有严重风险, 请立即检查！{r.text}")
    except Exception as e:
        logger.error(f"电话告警触发失败, 可能有严重风险, 请立即检查！{e}")
        logger.exception(e)


def sendAndPrintInfo(msg):
    logger.info(msg)
    sendMixin(msg)


def sendAndPrintError(msg):
    logger.error(msg)
    sendMixin(msg)


def sendAndCritical(msg):
    logger.critical(msg)
    if CALL_ALARM:
        callAlarm(strategyName=RUN_NAME, content=msg)
    sendMixin(msg)


def sendAndRaise(msg):
    logger.error(msg)
    sendMixin(msg)
    raise RuntimeError(msg)


def retryy(func, _name="retryy", _wait=1, _times=3, critical=False, **kwargs):
    error = ""
    for i in range(_times):
        try:
            return func(**kwargs)
        except ccxt.MarginModeAlreadySet:
            pass
        except Exception as e:
            error = str(e)
            logger.error(f"{_name} raised a error: {e}")
            logger.exception(e)
            time.sleep(_wait)
    else:
        f = f"{RUN_NAME} {_name} 重试{_times}次无效, 程序退出: {error}"
        if critical:
            sendAndCritical("！严重级别告警！" + f)
        else:
            sendAndPrintError(f)
        raise RuntimeError(f)


def getAccountBalance(exchange):
    # positions:
    # initialMargin maintMargin unrealizedProfit positionInitialMargin openOrderInitialMargin leverage  isolated entryPrice maxNotional positionSide positionAmt notional isolatedWallet updateTime bidNotional askNotional
    try:
        b = retryy(exchange.fetchBalance, _name=f"获取账户资金信息getBalances()")["info"]
        balances = pd.DataFrame(b["assets"])
        balances.set_index("asset", inplace=True)
        balances.index.name = None
        positions = pd.DataFrame(b["positions"])
        positions.set_index("symbol", inplace=True)
        positions.index.name = None
        b.pop("assets")
        b.pop("positions")
        total = pd.DataFrame(b, index=[0])
        return total, balances, positions
    except Exception as e:
        logger.exception(e)
        sendAndPrintError(f"{RUN_NAME}: getAccountBalance()错误: {e}")


def getPositions(exchange):
    # positions:
    # info    id  contracts  contractSize  unrealizedPnl  leverage liquidationPrice  collateral  notional markPrice  entryPrice timestamp  initialMargin  initialMarginPercentage  maintenanceMargin  maintenanceMarginPercentage marginRatio datetime marginMode marginType  side  hedged percentage
    try:
        p = exchange.fetchPositions()
        p = pd.DataFrame(p)
        p.set_index("symbol", inplace=True)
        p.index.name = None
        return p
    except Exception as e:
        logger.exception(e)
        sendAndRaise(f"{RUN_NAME}: getPositions()错误, 程序退出。{e}")


def getOpenPosition(exchange):
    pos = getPositions(exchange)
    op = pd.DataFrame()
    op = pos.loc[pos["contracts"] != 0]
    op = op.astype(
        {
            "contracts": float,
            "unrealizedPnl": float,
            "leverage": float,
            "liquidationPrice": float,
            "collateral": float,
            "notional": float,
            "markPrice": float,
            "entryPrice": float,
            "marginType": str,
            "side": str,
            "percentage": float,
            "timestamp": "datetime64[ms]",
        }
    )
    op = op[["side", "contracts", "notional", "percentage", "unrealizedPnl", "leverage", "entryPrice", "markPrice",
             "liquidationPrice", "marginType", "datetime", "timestamp"]]
    return op


def getBalance(exchange, asset="usdt"):
    # 返回不包含uPNL的账户权益，即 已开仓占用的保证金 和 可用余额 的总和
    asset = asset.upper()
    r = exchange.fapiPrivateGetAccount()["assets"]
    r = pd.DataFrame(r)
    bal = float(r.loc[r["asset"] == asset, "walletBalance"])
    return bal


def nextStartTime(level, ahead_seconds=3, offsetSec=0):
    # ahead_seconds为预留秒数,
    # 当离开始时间太近, 本轮可能来不及下单, 因此当离开始时间的秒数小于预留秒数时,
    # 就直接顺延至下一轮开始
    if level.endswith('m') or level.endswith('h'):
        pass
    elif level.endswith('T'):
        level = level.replace('T', 'm')
    elif level.endswith('H'):
        level = level.replace('H', 'h')
    else:
        sendAndRaise(f"{RUN_NAME}: level格式错误。程序退出。")

    ti = pd.to_timedelta(level)
    now_time = dt.datetime.now()
    # now_time = dt.datetime(2019, 5, 9, 23, 50, 30)  # 修改now_time, 可用于测试
    this_midnight = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = dt.timedelta(minutes=1)

    target_time = now_time.replace(second=0, microsecond=0)

    while True:
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if (
                delta.seconds % ti.seconds == 0
                and (target_time - now_time).seconds >= ahead_seconds
        ):
            # 当符合运行周期, 并且目标时间有足够大的余地, 默认为60s
            break

    target_time -= dt.timedelta(seconds=offsetSec)
    return target_time


def sleepToClose(level, aheadSeconds, isTest=False, offsetSec=0):
    nextTime = nextStartTime(level, ahead_seconds=aheadSeconds, offsetSec=offsetSec)
    testStr = f"(测试轮, 跳过等待时间)" if isTest else ""
    logger.info(f"等待开始时间: {nextTime} {testStr}")
    if isTest is False:
        time.sleep(max(0, (nextTime - dt.datetime.now()).seconds))
        while True:  # 在靠近目标时间时
            if dt.datetime.now() > nextTime:
                break
    logger.info(f"吉时已到, 开炮!")
    return nextTime


def getKline(exchange, symbolConfig):
    symbol = symbolConfig[0]
    level = symbolConfig[1]["level"]
    limit = symbolConfig[1]["klinesNum"]
    if "/" in symbol: symbol = symbol.replace("/", "")
    data = retryy(
        exchange.fapiPublic_get_klines,
        _name=f"获取k线 getKline({symbol})",
        _wait=1, _times=3,
        critical=False,
        params={'symbol': symbol, 'interval': level, 'limit': limit},
    )[:-1]

    df = pd.DataFrame(data, dtype=float)
    df.rename(columns={1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
    df['candle_begin_time'] = pd.to_datetime(df[0], unit='ms') + dt.timedelta(hours=8)
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
    logger.debug(f"{symbol} 获取到k线 {len(df)} 根")

    return df


def getKlinesForSymbols(exchange, symbolsConfig, isTest=True):
    symbols = list(symbolsConfig.keys())
    num = 1 if isTest else len(symbolsConfig)
    dfList = Parallel(n_jobs=num, backend="threading")(
        delayed(getKline)(exchange, symbolConfig) for symbolConfig in symbolsConfig.items()
    )

    klinesDict = dict(zip(symbols, dfList))

    return klinesDict


def calSignal(symbol, symbolsConfig, klinesDict):
    symbolConfig = symbolsConfig[symbol]
    klinesDf = klinesDict[symbol]
    strategyName = symbolConfig["strategy"]
    para = symbolConfig["para"]
    _cls = __import__(f"signals.{strategyName}", fromlist=('',))
    signal = getattr(_cls, "signal")(klinesDf, para)
    logger.debug(f"{symbol} 策略: {strategyName} 参数: {para} 信号结果: {signal}")

    return symbol, signal


def calSignalForSymbols(symbolsConfig, klinesDict, isTest=True):
    symbols = list(klinesDict.keys())
    num = 1 if isTest else len(symbols)
    signals = Parallel(n_jobs=num, backend="threading")(
        delayed(calSignal)(symbol, symbolsConfig, klinesDict) for symbol in symbols
    )

    signals = {i[0]: i[1] for i in signals}

    return signals


def getTicker(exchange, markets, symbol):
    try:
        symbolId = markets[symbol]["id"]
        tk = exchange.fapiPublicGetTickerBookticker({"symbol": symbolId})
        tk = pd.DataFrame(tk, index=[0])
        tk = tk.astype(
            {
                "bidPrice": float,
                "bidQty": float,
                "askPrice": float,
                "askQty": float,
            }
        )
        # symbol  bidPrice bidQty  askPrice  askQty           time
        # 0  BTCUSDT  20896.70  6.719  20896.80  12.708  1673800340925
        return tk
    except Exception as e:
        logger.exception(e)
        sendAndRaise(f"{RUN_NAME}: getTicker()错误，程序退出。{e}")


def setLeverage(exchange, symbol, leverage):
    retryy(exchange.setLeverage, _name=f"设置页面杠杆setLeverage({symbol})", symbol=symbol, leverage=leverage)


def setMarginMode(exchange, symbol, mode="cross"):
    # mode: cross, isolated
    retryy(exchange.setMarginMode, _name=f"设置保证金模式setMarginMode({symbol})", symbol=symbol, marginMode=mode)


def calOrder(exchange, markets, signalInfo, symbolsConfig, positions, balance):
    # signalInfo: ("ETH/USDT", 1)
    symbol = signalInfo[0]
    symbolId = markets[symbol]["id"]
    signal = signalInfo[1]

    weight = symbolsConfig[symbol]["weight"]
    orderValue = balance * weight

    # 如果当前symbol的signal不为None
    order = {symbol: None}
    if signal is not None:
        # 检查是否已经有symbol的持仓
        posNow = positions[positions.index.str.contains(symbol)]

        # 如果当前无该symbol的持仓，直接开仓
        # 本次开仓 应分配资金 = 实际开仓价值
        if posNow.empty:
            costAim = orderValue

        # 如果已经有symbol持仓:
        #   如果是同向持仓，则本轮不动
        #   如果是反向持仓，则本次实际开仓的金额 首先要能覆盖掉反向仓位，还要够反向开仓，即 实际开仓 = 目标仓位 - 已有仓位
        if not posNow.empty:
            posDirction = 1 if posNow.iloc[0]["side"] == "long" else -1
            if posDirction == signal:
                logger.debug(f"{symbol} 已经有同向持仓, 本轮跳过")
                return order
            else:
                # 当前持仓价值
                posCostNow = posNow.iloc[0]["notional"]
                # 本次开仓价值 = 目标持仓价值 * 目标方向 - 当前持仓 * 当前方向
                costAim = abs(orderValue * signal - posCostNow * posDirction)

        logger.debug(f"{symbol} 目标持仓价值{QUOTE_COIN}: {round(costAim,2)} 实际分配资金: {round(costAim/PAGE_LEVERAGE,2)}")

        # 获取实时价格，与costAim计算下单参数
        t = retryy(exchange.fetchTicker, _name=f"计算订单信息时获取实时价格fetchTicker({symbol})", symbol=symbol)
        price = t["last"] * 1.0015 if signal == 1 else t["last"] * 0.9985
        amount = float(costAim) / float(price)
        logger.debug(f"{symbol} original order price: {price} amount: {amount}")

        try:
            price = exchange.priceToPrecision(symbol, price)
            amount = exchange.amountToPrecision(symbol, amount)
        except ccxt.ArgumentsRequired as e:
            sendAndPrintError(f"{symbol} 下单金额({costAim})、价格({price})、数量({amount})触发限制, 本轮跳过: {e}")
            return order

        order[symbol] = {
                "symbol": symbolId,
                "price": price,
                "quantity": amount,
                "type": "LIMIT",
                "side": "BUY" if signal == 1 else "SELL",
                "timeInForce": "GTC",
        }

    return order


def calOrderForSymbols(exchange, markets, signals, symbolsConfig, positions, balance, isTest=True):
    num = 1 if isTest else len(signals)
    orders = Parallel(n_jobs=num, backend="threading")(
        delayed(calOrder)(exchange, markets, signalInfo, symbolsConfig, positions, balance) for signalInfo in signals.items()
    )

    return orders


def placeOrder(exchange, orderInfo, isTrade=False):
    symbol = list(orderInfo.keys())[0]
    order = orderInfo[symbol]

    if isTrade is False:
        return {symbol: "This is a test result."}

    orderResp = {symbol: None}
    if order:
        setMarginMode(exchange, symbol, mode="cross")
        setLeverage(exchange, symbol, PAGE_LEVERAGE)

        r = retryy(
            exchange.fapiPrivatePostOrder,
            _name=f"下单ex.fapiPrivatePostOrder({symbol}) 订单参数: {order}",
            critical=True,
            params=order,
        )
        orderResp[symbol] = r

    return orderResp


def placeOrderForSymbols(exchange, orderInfos, isTest=True, isTrade=False):
    num = 1 if isTest else len(orderInfos)
    ordersResp = Parallel(n_jobs=num, backend="threading")(
        delayed(placeOrder)(exchange, order, isTrade) for order in orderInfos
    )

    return ordersResp


def main():
    ex = ccxt.binance(EXCHANGE_CONFIG)
    mkts = ex.load_markets()
    klines = getKlinesForSymbols(ex, symbolsConfig)
    positions = getOpenPosition(ex)
    print(f"positions:\n{positions}")
    balance = getBalance(ex, "USDT")
    print(f"balance: {balance}")
    signals = calSignalForSymbols(symbolsConfig, klines, isTest=True)
    print(f"signals: {signals}")
    orders = calOrderForSymbols(ex, mkts, signals, symbolsConfig, positions, balance, isTest=True)
    print(f"orders: {orders}")
    ordersResp = placeOrderForSymbols(ex, orders, isTest=True, isTrade=False)
    print(f"ordersResp:\n{ordersResp}")


if __name__ == "__main__":
    main()
