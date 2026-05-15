import pandas as pd

def calculate_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    avg_gain = delta.where(delta > 0, 0).ewm(alpha=1 / period, min_periods=1, adjust=False).mean()
    avg_loss = -delta.where(delta < 0, 0).ewm(alpha=1 / period, min_periods=1, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_mfi(data: pd.DataFrame, period: int) -> pd.Series:
    tp = (data['high'] + data['low'] + data['close']) / 3
    mf = tp * data['volume']
    positive_mf = mf.where(tp.diff() > 0, 0)
    negative_mf = mf.where(tp.diff() < 0, 0)
    positive_mf_sum = positive_mf.rolling(window=period).sum()
    negative_mf_sum = negative_mf.rolling(window=period).sum()
    mfr = positive_mf_sum / negative_mf_sum
    return 100 - (100 / (1 + mfr))


def calculate_stochastic1413(data: pd.DataFrame) -> tuple[pd.DataFrame, float, float]:
    data['L14'] = data['low'].rolling(window=14).min()
    data['H14'] = data['high'].rolling(window=14).max()
    data['%K1413'] = (data['close'] - data['L14']) / (data['H14'] - data['L14']) * 100
    data['%D1413'] = data['%K1413'].rolling(window=3).mean()
    K1413 = data['%K1413'].iloc[-1]
    D1413 = data['%D1413'].iloc[-1]
    return data, K1413, D1413


def calculate_stochastic1433(data: pd.DataFrame) -> tuple[pd.DataFrame, float, float]:
    data['L14'] = data['low'].rolling(window=14).min()
    data['H14'] = data['high'].rolling(window=14).max()

    range_14 = data['H14'] - data['L14']
    data['raw_%K1433'] = (data['close'] - data['L14']) / range_14 * 100
    data['%K1433'] = data['raw_%K1433'].rolling(window=3).mean()
    data['%D1433'] = data['%K1433'].rolling(window=3).mean()
    K1433 = data['%K1433'].iloc[-1]
    D1433 = data['%D1433'].iloc[-1]
    return data, K1433, D1433

def calculate_indicators(data: pd.DataFrame, new_tick: dict) -> pd.DataFrame:
    new_row = {
        "time": pd.to_datetime(new_tick["time"]),
        "symbol": new_tick["symbol"],
        "open": new_tick["open"],
        "high": new_tick["high"],
        "low": new_tick["low"],
        "close": new_tick["close"],
        "volume": new_tick["totalVol"],
    }
    try:
        data = pd.concat([data, pd.DataFrame([new_row])], ignore_index=True)
    except Exception as e:
        symbol = new_tick.get("symbol")
        print(f"Lỗi indicator {symbol}: {e}")

    # ===== Pivot =====
    data["pivot"] = round((data["high"] + data["low"] + data["close"]) / 3, 2)

    # SMA 10,20,50
    data['SMA10'] = data['close'].rolling(window=10).mean()
    data['SMA20'] = data['close'].rolling(window=20).mean()
    data['SMA50'] = data['close'].rolling(window=50).mean()
    SMA10 = data['SMA10'].iloc[-1]
    SMA20 = data['SMA20'].iloc[-1]
    SMA50 = data['SMA50'].iloc[-1]

    # EMA 10,20,50
    data['EMA10'] = data['close'].ewm(span=10, adjust=False).mean()
    data['EMA20'] = data['close'].ewm(span=20, adjust=False).mean()
    data['EMA50'] = data['close'].ewm(span=50, adjust=False).mean()
    data["EMA100"] = data["close"].ewm(span=100, adjust=False).mean()
    data["EMA200"] = data["close"].ewm(span=200, adjust=False).mean()

    EMA10 = data['EMA10'].iloc[-1]
    EMA20 = data['EMA20'].iloc[-1]
    EMA50 = data['EMA50'].iloc[-1]
    EMA100 = data["EMA100"].iloc[-1]
    EMA200 = data["EMA200"].iloc[-1]

    # MACD
    data['EMA_12'] = data['close'].ewm(span=12, adjust=False).mean()
    data['EMA_26'] = data['close'].ewm(span=26, adjust=False).mean()
    data['MACD'] = data['EMA_12'] - data['EMA_26']
    data['Signal_Line'] = data['MACD'].ewm(span=9, adjust=False).mean()
    data['Histogram'] = data['MACD'] - data['Signal_Line']
    macd= data['MACD'].iloc[-1]
    signal_line= data['Signal_Line'].iloc[-1]
    macd2 = data['MACD'].iloc[-2]
    signal_line2 = data['Signal_Line'].iloc[-2]

    # volume 10, 20, 50
    data['volume_10'] = data['volume'].rolling(10).mean()
    data['volume_20'] = data['volume'].rolling(20).mean()
    data['volume_50'] = data['volume'].rolling(50).mean()
    volume_10 = data['volume_10'].iloc[-1]
    volume_20 = data['volume_20'].iloc[-1]
    volume_50 = data['volume_50'].iloc[-1]

    # ichimuko
    data['tenkan_sen'] = (data['high'].rolling(window=9).max() + data['low'].rolling(window=9).min()) / 2
    data['kijun_sen'] = (data['high'].rolling(window=26).max() + data['low'].rolling(window=26).min()) / 2
    data['senkou_span_a'] = ((data['tenkan_sen'] + data['kijun_sen']) / 2).shift(26)
    data['senkou_span_b'] = ((data['high'].rolling(window=52).max() + data['low'].rolling(window=52).min()) / 2).shift(26)
    tk= data['tenkan_sen'].iloc[-1]
    ks= data['kijun_sen'].iloc[-1]

    # RSI (10 -> 20)
    data['RSI10'] = calculate_rsi(data['close'], 10)
    data['RSI11'] = calculate_rsi(data['close'], 11)
    data['RSI12'] = calculate_rsi(data['close'], 12)
    data['RSI13'] = calculate_rsi(data['close'], 13)
    data['RSI14'] = calculate_rsi(data['close'], 14)
    data['RSI15'] = calculate_rsi(data['close'], 15)
    data['RSI16'] = calculate_rsi(data['close'], 16)
    data['RSI17'] = calculate_rsi(data['close'], 17)
    data['RSI18'] = calculate_rsi(data['close'], 18)
    data['RSI19'] = calculate_rsi(data['close'], 19)
    data['RSI20'] = calculate_rsi(data['close'], 20)
    
    RSI10 = data['RSI10'].iloc[-1]
    RSI11 = data['RSI11'].iloc[-1]
    RSI12 = data['RSI12'].iloc[-1]
    RSI13 = data['RSI13'].iloc[-1]
    RSI14 = data['RSI14'].iloc[-1]
    RSI15 = data['RSI15'].iloc[-1]
    RSI16 = data['RSI16'].iloc[-1]
    RSI17 = data['RSI17'].iloc[-1]
    RSI18 = data['RSI18'].iloc[-1]
    RSI19 = data['RSI19'].iloc[-1]
    RSI20 = data['RSI20'].iloc[-1]

    # MFI (10 -> 20)
    data['MFI10'] = calculate_mfi(data, 10)
    data['MFI11'] = calculate_mfi(data, 11)
    data['MFI12'] = calculate_mfi(data, 12)
    data['MFI13'] = calculate_mfi(data, 13)
    data['MFI14'] = calculate_mfi(data, 14)
    data['MFI15'] = calculate_mfi(data, 15)
    data['MFI16'] = calculate_mfi(data, 16)
    data['MFI17'] = calculate_mfi(data, 17)
    data['MFI18'] = calculate_mfi(data, 18)
    data['MFI19'] = calculate_mfi(data, 19)
    data['MFI20'] = calculate_mfi(data, 20)

    MFI10 = data['MFI10'].iloc[-1]
    MFI11 = data['MFI11'].iloc[-1]
    MFI12 = data['MFI12'].iloc[-1]
    MFI13 = data['MFI13'].iloc[-1]
    MFI14 = data['MFI14'].iloc[-1]
    MFI15 = data['MFI15'].iloc[-1]
    MFI16 = data['MFI16'].iloc[-1]
    MFI17 = data['MFI17'].iloc[-1]
    MFI18 = data['MFI18'].iloc[-1]
    MFI19 = data['MFI19'].iloc[-1]
    MFI20 = data['MFI20'].iloc[-1]


    #  --- Các điều kiện liên quan đến SMA ----
    ##---------close--------------
    close_cross_up_sma10 = bool(data['close'].iloc[-2] < data['SMA10'].iloc[-2] and data['close'].iloc[-1] >= data['SMA10'].iloc[-1])
    close_cross_up_sma20 = bool(data['close'].iloc[-2] < data['SMA20'].iloc[-2] and data['close'].iloc[-1] >= data['SMA20'].iloc[-1])
    close_cross_up_sma50 = bool(data['close'].iloc[-2] < data['SMA50'].iloc[-2] and data['close'].iloc[-1] >= data['SMA50'].iloc[-1])

    close_cross_down_sma10 = bool(data['close'].iloc[-2] > data['SMA10'].iloc[-2] and data['close'].iloc[-1] <= data['SMA10'].iloc[-1])
    close_cross_down_sma20 = bool(data['close'].iloc[-2] > data['SMA20'].iloc[-2] and data['close'].iloc[-1] <= data['SMA20'].iloc[-1])
    close_cross_down_sma50 = bool(data['close'].iloc[-2] > data['SMA50'].iloc[-2] and data['close'].iloc[-1] <= data['SMA50'].iloc[-1])

    ##-------------pivot----------------
    pivot_cross_up_sma10 = bool(data['pivot'].iloc[-2] < data['SMA10'].iloc[-2] and data['pivot'].iloc[-1] >= data['SMA10'].iloc[-1])
    pivot_cross_up_sma20 = bool(data['pivot'].iloc[-2] < data['SMA20'].iloc[-2] and data['pivot'].iloc[-1] >= data['SMA20'].iloc[-1])
    pivot_cross_up_sma50 = bool(data['pivot'].iloc[-2] < data['SMA50'].iloc[-2] and data['pivot'].iloc[-1] >= data['SMA50'].iloc[-1])

    pivot_cross_down_sma10 = bool(data['pivot'].iloc[-2] > data['SMA10'].iloc[-2] and data['pivot'].iloc[-1] <= data['SMA10'].iloc[-1])
    pivot_cross_down_sma20 = bool(data['pivot'].iloc[-2] > data['SMA20'].iloc[-2] and data['pivot'].iloc[-1] <= data['SMA20'].iloc[-1])
    pivot_cross_down_sma50 = bool(data['pivot'].iloc[-2] > data['SMA50'].iloc[-2] and data['pivot'].iloc[-1] <= data['SMA50'].iloc[-1])

    # SMA10 cắt lên SMA20
    sma10_cross_up_sma20 = bool(data['SMA10'].iloc[-2] < data['SMA20'].iloc[-2] and data['SMA10'].iloc[-1] >= data['SMA20'].iloc[-1])
    sma10_cross_down_sma20 = bool(data['SMA10'].iloc[-2] > data['SMA20'].iloc[-2] and data['SMA10'].iloc[-1] <= data['SMA20'].iloc[-1])
    # SMA10 cắt lên SMA50
    sma10_cross_up_sma50 = bool(data['SMA10'].iloc[-2] < data['SMA50'].iloc[-2] and data['SMA10'].iloc[-1] >= data['SMA50'].iloc[-1])
    sma10_cross_down_sma50 = bool(data['SMA10'].iloc[-2] > data['SMA50'].iloc[-2] and data['SMA10'].iloc[-1] <= data['SMA50'].iloc[-1])
    # SMA20 cắt lên SMA50
    sma20_cross_up_sma50 = bool(data['SMA20'].iloc[-2] < data['SMA50'].iloc[-2] and data['SMA20'].iloc[-1] >= data['SMA50'].iloc[-1])
    sma20_cross_down_sma50 = bool(data['SMA20'].iloc[-2] > data['SMA50'].iloc[-2] and data['SMA20'].iloc[-1] <= data['SMA50'].iloc[-1])

    # --- Các điều kiện liên quan đến EMA ---
    ##---------close--------------
    close_cross_up_ema10 = bool(data['close'].iloc[-2] < data['EMA10'].iloc[-2] and data['close'].iloc[-1] >= data['EMA10'].iloc[-1])
    close_cross_up_ema20 = bool(data['close'].iloc[-2] < data['EMA20'].iloc[-2] and data['close'].iloc[-1] >= data['EMA20'].iloc[-1])
    close_cross_up_ema50 = bool(data['close'].iloc[-2] < data['EMA50'].iloc[-2] and data['close'].iloc[-1] >= data['EMA50'].iloc[-1])

    close_cross_down_ema10 = bool(data['close'].iloc[-2] > data['EMA10'].iloc[-2] and data['close'].iloc[-1] <= data['EMA10'].iloc[-1])
    close_cross_down_ema20 = bool(data['close'].iloc[-2] > data['EMA20'].iloc[-2] and data['close'].iloc[-1] <= data['EMA20'].iloc[-1])
    close_cross_down_ema50 = bool(data['close'].iloc[-2] > data['EMA50'].iloc[-2] and data['close'].iloc[-1] <= data['EMA50'].iloc[-1])

    ##-------------pivot----------------
    pivot_cross_up_ema10 = bool(data['pivot'].iloc[-2] < data['EMA10'].iloc[-2] and data['pivot'].iloc[-1] >= data['EMA10'].iloc[-1])
    pivot_cross_up_ema20 = bool(data['pivot'].iloc[-2] < data['EMA20'].iloc[-2] and data['pivot'].iloc[-1] >= data['EMA20'].iloc[-1])
    pivot_cross_up_ema50 = bool(data['pivot'].iloc[-2] < data['EMA50'].iloc[-2] and data['pivot'].iloc[-1] >= data['EMA50'].iloc[-1])

    pivot_cross_down_ema10 = bool(data['pivot'].iloc[-2] > data['EMA10'].iloc[-2] and data['pivot'].iloc[-1] <= data['EMA10'].iloc[-1])
    pivot_cross_down_ema20 = bool(data['pivot'].iloc[-2] > data['EMA20'].iloc[-2] and data['pivot'].iloc[-1] <= data['EMA20'].iloc[-1])
    pivot_cross_down_ema50 = bool(data['pivot'].iloc[-2] > data['EMA50'].iloc[-2] and data['pivot'].iloc[-1] <= data['EMA50'].iloc[-1])

    # EMA10 cắt EMA20
    ema10_cross_up_ema20 = bool(data['EMA10'].iloc[-2] < data['EMA20'].iloc[-2] and data['EMA10'].iloc[-1] >= data['EMA20'].iloc[-1])
    ema10_cross_down_ema20 = bool(data['EMA10'].iloc[-2] > data['EMA20'].iloc[-2] and data['EMA10'].iloc[-1] <= data['EMA20'].iloc[-1])
    # EMA10 cắt EMA50
    ema10_cross_up_ema50 = bool(data['EMA10'].iloc[-2] < data['EMA50'].iloc[-2] and data['EMA10'].iloc[-1] >= data['EMA50'].iloc[-1])
    ema10_cross_down_ema50 = bool(data['EMA10'].iloc[-2] > data['EMA50'].iloc[-2] and data['EMA10'].iloc[-1] <= data['EMA50'].iloc[-1])
    # EMA20 cắt EMA50
    ema20_cross_up_ema50 = bool(data['EMA20'].iloc[-2] < data['EMA50'].iloc[-2] and data['EMA20'].iloc[-1] >= data['EMA50'].iloc[-1])
    ema20_cross_down_ema50 = bool(data['EMA20'].iloc[-2] > data['EMA50'].iloc[-2] and data['EMA20'].iloc[-1] <= data['EMA50'].iloc[-1])

    # --- Các điều kiện liên quan đến MACD ---
    macd_cross_down_signal = bool(data['MACD'].iloc[-2] > data['Signal_Line'].iloc[-2] and data['MACD'].iloc[-1] <= data['Signal_Line'].iloc[-1])
    macd_cross_up_signal = bool(data['MACD'].iloc[-2] < data['Signal_Line'].iloc[-2] and data['MACD'].iloc[-1] >= data['Signal_Line'].iloc[-1])
    macd_cross_up_zero = bool(data['MACD'].iloc[-2] < 0 and data['MACD'].iloc[-1] >= 0)
    macd_cross_down_zero = bool(data['MACD'].iloc[-2] > 0 and data['MACD'].iloc[-1] <= 0)

        
    data, K1413, D1413 = calculate_stochastic1413(data)
    data, K1433, D1433 = calculate_stochastic1433(data)

    # --- Các điều kiện liên quan đến stochastic oscillator ---
    stoch1413_cross_up = bool(data['%K1413'].iloc[-2] < data['%D1413'].iloc[-2] and data['%K1413'].iloc[-1] >= data['%D1413'].iloc[-1])
    stoch1413_cross_down = bool(data['%K1413'].iloc[-2] > data['%D1413'].iloc[-2] and data['%K1413'].iloc[-1] <= data['%D1413'].iloc[-1])

    stoch1433_cross_up = bool(data['%K1433'].iloc[-2] < data['%D1433'].iloc[-2] and data['%K1433'].iloc[-1] >= data['%D1433'].iloc[-1])
    stoch1433_cross_down = bool(data['%K1433'].iloc[-2] > data['%D1433'].iloc[-2] and data['%K1433'].iloc[-1] <= data['%D1433'].iloc[-1])

    # ===== Bollinger Bands (20) =====
    data["BB_MID_20"] = data["close"].rolling(20).mean()
    data["BB_STD_20"] = data["close"].rolling(20).std()
    data["BB_UPPER_20"] = data["BB_MID_20"] + 2 * data["BB_STD_20"]
    data["BB_LOWER_20"] = data["BB_MID_20"] - 2 * data["BB_STD_20"]
    BB_upper= data["BB_UPPER_20"].iloc[-1]
    BB_lower= data["BB_LOWER_20"].iloc[-1]

    # --- Các điều kiện liên quan đến BB ---
    ##---------close với BB--------------
    close_cross_up_bb_upper = bool(data['close'].iloc[-2] < data['BB_UPPER_20'].iloc[-2] and data['close'].iloc[-1] >= data['BB_UPPER_20'].iloc[-1])
    close_cross_down_bb_upper = bool(data['close'].iloc[-2] > data['BB_UPPER_20'].iloc[-2] and data['close'].iloc[-1] <= data['BB_UPPER_20'].iloc[-1])

    close_cross_up_bb_lower = bool(data['close'].iloc[-2] < data['BB_LOWER_20'].iloc[-2] and data['close'].iloc[-1] >= data['BB_LOWER_20'].iloc[-1])
    close_cross_down_bb_lower = bool(data['close'].iloc[-2] > data['BB_LOWER_20'].iloc[-2] and data['close'].iloc[-1] <= data['BB_LOWER_20'].iloc[-1])

    ##---------pivot với BB--------------
    pivot_cross_up_bb_upper = bool(data['pivot'].iloc[-2] < data['BB_UPPER_20'].iloc[-2] and data['pivot'].iloc[-1] >= data['BB_UPPER_20'].iloc[-1])
    pivot_cross_down_bb_upper = bool(data['pivot'].iloc[-2] > data['BB_UPPER_20'].iloc[-2] and data['pivot'].iloc[-1] <= data['BB_UPPER_20'].iloc[-1])

    pivot_cross_up_bb_lower = bool(data['pivot'].iloc[-2] < data['BB_LOWER_20'].iloc[-2] and data['pivot'].iloc[-1] >= data['BB_LOWER_20'].iloc[-1])
    pivot_cross_down_bb_lower = bool(data['pivot'].iloc[-2] > data['BB_LOWER_20'].iloc[-2] and data['pivot'].iloc[-1] <= data['BB_LOWER_20'].iloc[-1])

    # --- Các điều kiện liên quan đến ichimoku ---
    # Tenkan-sen cắt lên Kijun-sen
    tk_cross_up_ks = bool(data['tenkan_sen'].iloc[-2] < data['kijun_sen'].iloc[-2] and data['tenkan_sen'].iloc[-1] >= data['kijun_sen'].iloc[-1])
    # Tenkan-sen cắt xuống Kijun-sen
    tk_cross_down_ks = bool(data['tenkan_sen'].iloc[-2] > data['kijun_sen'].iloc[-2] and data['tenkan_sen'].iloc[-1] <= data['kijun_sen'].iloc[-1])

    # Giá cắt lên từ dưới lên trên mây
    close_cross_up_cloud = bool(
                            data['close'].iloc[-2] <= max(data['senkou_span_a'].iloc[-2], data['senkou_span_b'].iloc[-2]) and
                            data['close'].iloc[-1] > max(data['senkou_span_a'].iloc[-1], data['senkou_span_b'].iloc[-1])
                        )
    pivot_cross_up_cloud = bool(
                            data['pivot'].iloc[-2] <= max(data['senkou_span_a'].iloc[-2], data['senkou_span_b'].iloc[-2]) and
                            data['pivot'].iloc[-1] > max(data['senkou_span_a'].iloc[-1], data['senkou_span_b'].iloc[-1])
                        )
    # Giá cắt xuống từ trên xuống dưới mây
    close_cross_down_cloud = bool(
                            data['close'].iloc[-2] >= min(data['senkou_span_a'].iloc[-2], data['senkou_span_b'].iloc[-2]) and
                            data['close'].iloc[-1] < min(data['senkou_span_a'].iloc[-1], data['senkou_span_b'].iloc[-1])
                        )
    pivot_cross_down_cloud = bool(
                            data['pivot'].iloc[-2] >= min(data['senkou_span_a'].iloc[-2], data['senkou_span_b'].iloc[-2]) and
                            data['pivot'].iloc[-1] < min(data['senkou_span_a'].iloc[-1], data['senkou_span_b'].iloc[-1])
                        )

    return (data, EMA10, EMA20, EMA50, EMA100, EMA200, SMA10, SMA20, SMA50,
            
            RSI10, RSI11, RSI12, RSI13, RSI14, RSI15, RSI16, RSI17, RSI18, RSI19, RSI20,
            MFI10, MFI11, MFI12, MFI13, MFI14, MFI15, MFI16, MFI17, MFI18, MFI19, MFI20,

            K1433, D1433, K1413, D1413,
            
            BB_upper, BB_lower,

            tk, ks,

            volume_10, volume_20, volume_50,

            close_cross_up_sma10, close_cross_up_sma20, close_cross_up_sma50,
            close_cross_down_sma10, close_cross_down_sma20, close_cross_down_sma50,

            pivot_cross_up_sma10, pivot_cross_up_sma20, pivot_cross_up_sma50,
            pivot_cross_down_sma10, pivot_cross_down_sma20, pivot_cross_down_sma50,

            sma10_cross_up_sma20,sma10_cross_down_sma20, 
            sma10_cross_up_sma50, sma10_cross_down_sma50,
            sma20_cross_up_sma50, sma20_cross_down_sma50, 

            close_cross_up_ema10, close_cross_up_ema20, close_cross_up_ema50,
            close_cross_down_ema10, close_cross_down_ema20, close_cross_down_ema50,

            pivot_cross_up_ema10, pivot_cross_up_ema20, pivot_cross_up_ema50,
            pivot_cross_down_ema10, pivot_cross_down_ema20, pivot_cross_down_ema50,
            
            ema10_cross_up_ema20, ema10_cross_down_ema20,
            ema10_cross_up_ema50, ema10_cross_down_ema50,
            ema20_cross_up_ema50, ema20_cross_down_ema50,

            macd, signal_line, macd2, signal_line2,
            macd_cross_up_signal, macd_cross_down_signal, 
            macd_cross_up_zero, macd_cross_down_zero,

            close_cross_up_bb_upper, close_cross_down_bb_upper,
            close_cross_up_bb_lower, close_cross_down_bb_lower,

            pivot_cross_up_bb_upper, pivot_cross_down_bb_upper,
            pivot_cross_up_bb_lower, pivot_cross_down_bb_lower,

            stoch1413_cross_up, stoch1413_cross_down,
            stoch1433_cross_up, stoch1433_cross_down,

            tk_cross_up_ks, tk_cross_down_ks, 
            
            close_cross_up_cloud, close_cross_down_cloud,

            pivot_cross_up_cloud, pivot_cross_down_cloud
            )

