import pandas as pd


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
    EMA10 = data['EMA10'].iloc[-1]
    EMA20 = data['EMA20'].iloc[-1]
    EMA50 = data['EMA50'].iloc[-1]

    # MACD
    data['EMA_12'] = data['close'].ewm(span=12, adjust=False).mean()
    data['EMA_26'] = data['close'].ewm(span=26, adjust=False).mean()
    data['MACD'] = data['EMA_12'] - data['EMA_26']
    data['Signal_Line'] = data['MACD'].ewm(span=9, adjust=False).mean()
    data['Histogram'] = data['MACD'] - data['Signal_Line']

    # stochastic oscillator
    data['L14'] = data['low'].rolling(window=14).min()
    data['H14'] = data['high'].rolling(window=14).max()
    data['%K'] = (data['close'] - data['L14']) / (data['H14'] - data['L14']) * 100
    data['%D'] = data['%K'].rolling(window=3).mean()

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
    
    # tính RSI
    delta = data['close'].diff()
    avg_gain = delta.where(delta > 0, 0).ewm(alpha=1/14, min_periods=1, adjust=False).mean()
    avg_loss = -delta.where(delta < 0, 0).ewm(alpha=1/14, min_periods=1, adjust=False).mean()
    rs = avg_gain / avg_loss
    data['RSI'] = (100 - (100/(1 + rs)))
    RSI = data['RSI'].iloc[-1]

    # tính MFI
    tp = (data['high'] + data['low'] + data['close']) / 3
    mf = tp * data['volume']
    positive_mf = mf.where(tp.diff() > 0, 0)
    negative_mf = mf.where(tp.diff() < 0, 0)
    positive_mf_sum = positive_mf.rolling(window=14).sum()
    negative_mf_sum = negative_mf.rolling(window=14).sum()
    mfr = positive_mf_sum / negative_mf_sum
    data['MFI'] = (100 - (100 / (1 + mfr)))
    MFI = data['MFI'].iloc[-1]


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

    # --- Các điều kiện liên quan đến stochastic oscillator ---
    stoch_cross_up = bool(data['%K'].iloc[-2] < data['%D'].iloc[-2] and data['%K'].iloc[-1] >= data['%D'].iloc[-1])
    stoch_cross_down = bool(data['%K'].iloc[-2] > data['%D'].iloc[-2] and data['%K'].iloc[-1] <= data['%D'].iloc[-1])

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

    return (data, EMA10, EMA20, EMA50, SMA10, SMA20, SMA50, RSI, MFI,
            
            BB_upper, BB_lower,

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

            macd_cross_up_signal, macd_cross_down_signal, 
            macd_cross_up_zero, macd_cross_down_zero,

            close_cross_up_bb_upper, close_cross_down_bb_upper,
            close_cross_up_bb_lower, close_cross_down_bb_lower,

            pivot_cross_up_bb_upper, pivot_cross_down_bb_upper,
            pivot_cross_up_bb_lower, pivot_cross_down_bb_lower,

            stoch_cross_up, stoch_cross_down,

            tk_cross_up_ks, tk_cross_down_ks, 
            
            close_cross_up_cloud, close_cross_down_cloud,

            pivot_cross_up_cloud, pivot_cross_down_cloud
            )

