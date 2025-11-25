import pandas as pd


def caculate_indicators(data: pd.DataFrame, new_tick: dict) -> pd.DataFrame:
    new_row = {
        "time": pd.to_datetime(new_tick["time"]),
        "symbol": new_tick["symbol"],
        "open": new_tick["open"],
        "high": new_tick["high"],
        "low": new_tick["low"],
        "close": new_tick["close"],
        "volume": new_tick["totalVol"],
    }

    data = pd.concat([data, pd.DataFrame([new_row])], ignore_index=True)

    # tính MA 10,20,50
    data['MA10'] = round(data['close'].rolling(window=10).mean(),2)
    data['MA20'] = round(data['close'].rolling(window=20).mean(),2)
    data['MA50'] = round(data['close'].rolling(window=50).mean(),2)

    #tính MACD
    data['EMA_12'] = data['close'].ewm(span=12, adjust=False).mean()
    data['EMA_26'] = data['close'].ewm(span=26, adjust=False).mean()
    data['MACD'] = data['EMA_12'] - data['EMA_26']
    data['Signal_Line'] = data['MACD'].ewm(span=9, adjust=False).mean()
    data['Histogram'] = data['MACD'] - data['Signal_Line']

    #tính volume TB 10,20,50
    data['volume_10'] = round(data['volume'].rolling(20).mean(),2)
    data['volume_20'] = round(data['volume'].rolling(20).mean(),2)
    data['volume_50'] = round(data['volume'].rolling(50).mean(),2)
    
    # tính RSI
    delta = data['close'].diff()
    avg_gain = delta.where(delta > 0, 0).ewm(alpha=1/14, min_periods=1, adjust=False).mean()
    avg_loss = -delta.where(delta < 0, 0).ewm(alpha=1/14, min_periods=1, adjust=False).mean()
    rs = avg_gain / avg_loss
    data['RSI'] = round((100 - (100/(1 + rs))),2)

    # tính MFI
    tp = (data['close'] + data['close'] + data['close']) / 3
    mf = tp * data['volume'] 
    positive_mf = mf.where(tp.diff() > 0, 0)
    negative_mf = mf.where(tp.diff() < 0, 0)
    positive_mf_sum = positive_mf.rolling(window=14).sum()
    negative_mf_sum = negative_mf.rolling(window=14).sum()
    mfr = positive_mf_sum / negative_mf_sum
    data['MFI'] = round((100 - (100 / (1 + mfr))),2)


    close_up_ma10 = False
    close_up_ma20 = False
    close_up_ma50 = False

    close_down_ma10 = False
    close_down_ma20 = False
    close_down_ma50 = False

    macd_cross_up = False
    macd_cross_down = False

    # MA cắt lên
    if data['close'].iloc[-2] < data['MA10'].iloc[-2] and data['close'].iloc[-1] >= data['MA10'].iloc[-1]:
        close_up_ma10 = True
    if data['close'].iloc[-2] < data['MA20'].iloc[-2] and data['close'].iloc[-1]  >= data['MA20'].iloc[-1]:
        close_up_ma20 = True
    if data['close'].iloc[-2] < data['MA50'].iloc[-2] and data['close'].iloc[-1]  >= data['MA50'].iloc[-1]:
        close_up_ma50 = True

    # MA cắt xuống
    if data['close'].iloc[-2] > data['MA10'].iloc[-2] and data['close'].iloc[-1]  <= data['MA10'].iloc[-1]:
        close_down_ma10 = True
    if data['close'].iloc[-2] > data['MA20'].iloc[-2] and data['close'].iloc[-1]  <= data['MA20'].iloc[-1]:
        close_down_ma20 = True
    if data['close'].iloc[-2] > data['MA50'].iloc[-2] and data['close'].iloc[-1]  <= data['MA50'].iloc[-1]:
        close_down_ma50 = True

    #MACD cắt lên
    if data['MACD'].iloc[-2] < data['Signal_Line'].iloc[-2] and data['MACD'].iloc[-1] >= data['Signal_Line'].iloc[-1]:
        macd_cross_up = True

    #MACD cắt xuống
    if data['MACD'].iloc[-2] > data['Signal_Line'].iloc[-2] and data['MACD'].iloc[-1] <= data['Signal_Line'].iloc[-1]:
        macd_cross_down = True

    # status MA
    status_up_ma10 = bool(data['close'].iloc[-1] >= data['MA10'].iloc[-1])
    status_up_ma20 = bool(data['close'].iloc[-1] >= data['MA20'].iloc[-1])
    status_up_ma50 = bool(data['close'].iloc[-1] >= data['MA50'].iloc[-1])

    # status MACD
    status_up_macd = bool(data['MACD'].iloc[-1] >= data['Signal_Line'].iloc[-1])

    return (data, close_up_ma10, close_up_ma20, close_up_ma50, close_down_ma10, close_down_ma20,
    close_down_ma50, macd_cross_up, macd_cross_down, status_up_ma10, status_up_ma20, status_up_ma50, status_up_macd)

