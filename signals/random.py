import random


def signal(*args):
    # 产生随机信号(-1, 1, None)，用于测试
    ra = random.random()

    if 0 <= ra <= 0.4:
        signal = -1
    elif 0.4 < ra <= 0.8:
        signal = 1
    else:
        signal = None

    return signal
