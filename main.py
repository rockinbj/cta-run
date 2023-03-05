import time
from multiprocessing import Pool as PPool
from multiprocessing import current_process
from functions import *
from exchangeConfig import *
from symbolsConfig import *
from settings import *
from logger import *

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)

logger = logging.getLogger("app.main")


def reporter(exchangeId, interval):
    process = current_process()
    process.name = "Reporter"
    while True:
        try:
            sendReport(exchangeId, symbolsConfig, interval)
        except Exception as e:
            sendAndPrintError(f"{RUN_NAME} 发送报告错误: {e}")
            logger.exception(e)
        time.sleep(0.5)


def main():
    _n = "\n"
    logger.info(f"实盘代码: {RUN_NAME}")
    logger.info(f"开始建立交易所信息、市场信息...")
    ex = getattr(ccxt, EXCHANGE)(EXCHANGE_CONFIG)
    mkts = retryy(ex.loadMarkets, _name="建立市场信息ex.loadMarkets()")
    logger.info(f"建立市场信息完成, 准备开始实盘")

    # 开启一个非阻塞的报告进程
    rptpool = PPool(1)
    rptpool.apply_async(reporter, args=(EXCHANGE, REPORT_INTERVAL))
    logger.info(f"开启报告进程成功")

    while True:
        balance = getBalance(ex, asset=QUOTE_COIN)
        logger.info(f"当前余额 {QUOTE_COIN}: {round(balance,2)} "
                    f"最大可用{round(MAX_BALANCE*100,2)}%: {round(balance * MAX_BALANCE,2)} "
                    f"页面杠杆: {PAGE_LEVERAGE} "
                    f"实际杠杆: {round(PAGE_LEVERAGE * MAX_BALANCE,2)}")

        positions = getOpenPosition(ex)
        logger.info(f"当前持仓:\n{positions}")

        nextStartTime = sleepToClose(SLEEP_LEVEL, aheadSeconds=3, isTest=IS_TEST, offsetSec=0)
        _delay = SLEEP_LONG * 10 if IS_TEST is False else 0
        logger.debug(f"为防止k线不闭合，延迟 {_delay}s 拉取k线")
        time.sleep(_delay)

        klinesDict = getKlinesForSymbols(ex, symbolsConfig, isTest=IS_TEST, cheatTime=CHEAT_TIME)
        logger.info(f"获取 {len(klinesDict)} 个币种k线, 共 {sum([len(k) for k in list(klinesDict.values())])} 根完成")

        signals = calSignalForSymbols(symbolsConfig, klinesDict, isTest=IS_TEST)
        logger.info(f"本轮交易信号: {signals}")

        ordersInfo = calOrderForSymbols(ex, mkts, signals, symbolsConfig, positions, balance, isTest=IS_TEST)
        logger.info(f"本轮下单信息:\n{_n.join(str(s) for s in ordersInfo)}")

        ordersResp = placeOrderForSymbols(ex, ordersInfo, isTest=IS_TEST, isTrade=IS_TRADE)
        logger.info(f"本轮下单回执:\n{_n.join(str(s) for s in ordersResp)}")

        # 如果本轮有订单就汇报
        if not all(o is None for o in ordersInfo):
            _str = ""
            for i in ordersInfo:
                if list(i.values())[0]:
                    _str += str({list(i.keys())[0]: list(i.values())[0]["side"]}) + "\n"
            sendMixin(f"本轮有交易信号:\n"
                      f"{_str}")

        if IS_TEST: exit()


if __name__ == "__main__":
    main()
