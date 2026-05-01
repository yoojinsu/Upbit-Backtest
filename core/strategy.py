import pandas as pd
import numpy as np

def prep_base_proj_vol(raw_base_df, interval, base_type):
    df_base = raw_base_df.copy()
    if interval == 'day':
        df_base['tf_index'] = (df_base.index - pd.Timedelta(hours=9)).floor('1D') + pd.Timedelta(hours=9)
        tf_units = 1440 if base_type == '1m' else 86400
    elif interval == 'minute240':
        df_base['tf_index'] = (df_base.index - pd.Timedelta(hours=1)).floor('4H') + pd.Timedelta(hours=1)
        tf_units = 240 if base_type == '1m' else 14400
    elif interval == 'minute60':
        df_base['tf_index'] = df_base.index.floor('1H')
        tf_units = 60 if base_type == '1m' else 3600
    else:
        df_base['tf_index'] = df_base.index
        tf_units = 1
        
    df_base['elapsed_units'] = df_base.groupby('tf_index').cumcount() + 1
    df_base['cumsum_vol'] = df_base.groupby('tf_index')['volume'].cumsum()
    df_base['proj_vol'] = df_base['cumsum_vol'] * (tf_units / df_base['elapsed_units'])
    
    return df_base

def compute_breakout_proj_vol(df, df_base, k_values, vol_multipliers=None):
    if vol_multipliers is None:
        vol_multipliers = []
    numeric_vol_mults = [float(v) for v in vol_multipliers if v != "X"]

    df_target = df[['open', 'range_prev', 'noise_prev']].copy()
    df_target['vol_ma5_prev'] = df['vol_ma5'].shift(1)

    join_cols = ['tf_index', 'high', 'close', 'proj_vol']
    if 'is_traded' in df_base.columns:
        join_cols.append('is_traded')

    df_base_merged = df_base[join_cols].join(
        df_target, on='tf_index', how='inner'
    )

    for k in k_values:
        if k == "동적K":
            buy_target = np.maximum(
                df_base_merged['open'] + (df_base_merged['range_prev'] * df_base_merged['noise_prev']),
                df_base_merged['open'])
            k_label = 'dynamic'
        else:
            buy_target = np.maximum(
                df_base_merged['open'] + (df_base_merged['range_prev'] * k),
                df_base_merged['open'])
            k_label = k

        base_price_mask = df_base_merged['high'] >= buy_target
        if 'is_traded' in df_base_merged.columns:
            price_mask = base_price_mask & (df_base_merged['is_traded'] == 1)
        else:
            price_mask = base_price_mask

        max_proj_vols = df_base_merged[price_mask].groupby('tf_index')['proj_vol'].max()
        pv_col = f'proj_vol_K_{k_label}'
        df[pv_col] = max_proj_vols.reindex(df.index).fillna(0)

        touched_df = df_base_merged[price_mask]
        if not touched_df.empty:
            touched_first_time = touched_df.groupby('tf_index').apply(lambda x: x.index[0])
            touched_first_price = touched_df.groupby('tf_index')['close'].first()
        else:
            touched_first_time = pd.Series(dtype='datetime64[ns]')
            touched_first_price = pd.Series(dtype='float64')

        df[f'first_touch_time_K_{k_label}'] = touched_first_time.reindex(df.index)
        df[f'first_touch_price_K_{k_label}'] = touched_first_price.reindex(df.index)

        for vol_mult in numeric_vol_mults:
            vol_threshold = df_base_merged['vol_ma5_prev'] * vol_mult
            simul_mask = price_mask & (df_base_merged['proj_vol'] > vol_threshold)
            simul_df = df_base_merged[simul_mask]

            if not simul_df.empty:
                hit = simul_df.groupby('tf_index').size().gt(0)
                first_time = simul_df.groupby('tf_index').apply(lambda x: x.index[0])
                first_price = simul_df.groupby('tf_index')['close'].first()
            else:
                hit = pd.Series(dtype='bool')
                first_time = pd.Series(dtype='datetime64[ns]')
                first_price = pd.Series(dtype='float64')

            simul_col = f'simul_K_{k_label}_V_{vol_mult}'
            buy_time_col = f'buy_time_K_{k_label}_V_{vol_mult}'
            buy_px_col = f'buy_px_K_{k_label}_V_{vol_mult}'

            df[simul_col] = hit.reindex(df.index).fillna(False)
            df[buy_time_col] = first_time.reindex(df.index)
            df[buy_px_col] = first_price.reindex(df.index)

    return df

def add_sell_price(df_valid, raw_base_df, interval, base_type):
    if interval == 'day':
        td = pd.Timedelta(days=1)
    elif interval == 'minute240':
        td = pd.Timedelta(hours=4)
    elif interval == 'minute60':
        td = pd.Timedelta(hours=1)
    else:
        td = pd.Timedelta(days=1)
        
    if base_type == '1s':
        sell_times = df_valid.index + td - pd.Timedelta(seconds=10)
        
        raw_closes = raw_base_df[['close']].copy()
        raw_closes['actual_sell_time'] = raw_closes.index
        if not raw_closes.index.is_monotonic_increasing:
            raw_closes.sort_index(inplace=True)
            
        target_df = pd.DataFrame({'target_time': sell_times}, index=df_valid.index)
        target_df_sorted = target_df.sort_values('target_time')
        
        merged = pd.merge_asof(target_df_sorted, raw_closes, left_on='target_time', right_index=True, direction='backward')
        merged.sort_index(inplace=True)
        
        df_valid['sell_price'] = merged['close'].fillna(df_valid['close'])
        
        fallback_times = pd.Series(sell_times, index=df_valid.index)
        df_valid['sell_time'] = merged['actual_sell_time'].fillna(fallback_times)
    else:
        df_valid['sell_price'] = df_valid['close']
        df_valid['sell_time'] = df_valid.index + td - pd.Timedelta(minutes=1)
        
    return df_valid

def calculate_opt_indicators(df):
    df['range_prev'] = df['high'].shift(1) - df['low'].shift(1)
    noise = 1 - abs(df['open'] - df['close']) / (df['high'] - df['low'])
    df['noise_prev'] = noise.rolling(window=20).mean().shift(1)

    for ma in [3, 5, 10, 20, 50, 60]:
        df[f'MA_{ma}'] = df['close'].rolling(window=ma).mean()
    
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / np.where(loss == 0, 1e-9, loss)
    df['rsi'] = np.where(loss == 0, 100, 100 - (100 / (1 + rs)))
    
    df['vol_ma5'] = df['volume'].rolling(window=5).mean()
    
    df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = df['ema12'] - df['ema26']
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()

    df['bb_ma20'] = df['close'].rolling(window=20).mean()
    df['bb_std'] = df['close'].rolling(window=20).std()
    df['bb_lower'] = df['bb_ma20'] - 2 * df['bb_std']
    df['bb_upper'] = df['bb_ma20'] + 2 * df['bb_std']

    typical_price = (df['high'] + df['low'] + df['close']) / 3
    raw_money_flow = typical_price * df['volume']
    money_flow_direction = np.where(typical_price > typical_price.shift(1), 1, -1)
    positive_flow = np.where(money_flow_direction == 1, raw_money_flow, 0)
    negative_flow = np.where(money_flow_direction == -1, raw_money_flow, 0)
    pos_flow_sum = pd.Series(positive_flow).rolling(window=14).sum()
    neg_flow_sum = pd.Series(negative_flow).rolling(window=14).sum()
    money_ratio = pos_flow_sum / np.where(neg_flow_sum == 0, 1e-9, neg_flow_sum)
    df['mfi'] = np.where(neg_flow_sum == 0, 100, 100 - (100 / (1 + money_ratio)))

    tr = np.maximum(df['high'] - df['low'], 
                    np.maximum(abs(df['high'] - df['close'].shift(1)), abs(df['low'] - df['close'].shift(1))))
    atr = tr.rolling(10).mean()
    hl2 = (df['high'] + df['low']) / 2
    
    upperband = (hl2 + (3 * atr)).values.copy()
    lowerband = (hl2 - (3 * atr)).values.copy()
    close_val = df['close'].values.copy()
    
    st_dir = np.ones(len(df))
    for i in range(1, len(df)):
        if close_val[i] > upperband[i-1]: st_dir[i] = 1
        elif close_val[i] < lowerband[i-1]: st_dir[i] = -1
        else: st_dir[i] = st_dir[i-1]
        
        if st_dir[i] == 1: 
            lowerband[i] = max(lowerband[i], lowerband[i-1])
        else: 
            upperband[i] = min(upperband[i], upperband[i-1])
            
    df['supertrend_up'] = st_dir == 1

    return df

def build_k_values_from_range(start_value, end_value, step_value):
    start_value = float(start_value)
    end_value = float(end_value)
    step_value = float(step_value)

    if step_value <= 0: raise ValueError("K 간격(Step)은 0보다 커야 합니다.")
    if end_value < start_value: raise ValueError("K 종료 값은 K 시작 값보다 크거나 같아야 합니다.")

    values = []
    current = start_value
    while current <= end_value + (step_value / 10):
        values.append(round(current, 4))
        current += step_value

    return values

def parse_optimizer_filter_text(text, field_name, upper=False):
    values = []
    for part in str(text).split(','):
        part = part.strip()
        if not part: continue
        if upper: part = part.upper()
        values.append(part)
    return values

def get_optimizer_filters_from_config(config):
    ma_filters = parse_optimizer_filter_text(config.get('opt_ma_filters', '0,3,5,10'), 'MA') if config.get('opt_use_ma', True) else ['0']
    rsi_filters = parse_optimizer_filter_text(config.get('opt_rsi_filters', '100,70,80'), 'RSI') if config.get('opt_use_rsi', True) else ['100']
    mfi_filters = parse_optimizer_filter_text(config.get('opt_mfi_filters', '100,80'), 'MFI') if config.get('opt_use_mfi', True) else ['100']
    vol_filters = parse_optimizer_filter_text(config.get('opt_vol_filters', 'X,1.0,1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8,1.9,2.0,2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,3.0'), '거래량', upper=True) if config.get('opt_use_vol', True) else ['X']
    macd_filters = parse_optimizer_filter_text(config.get('opt_macd_filters', 'O,X'), 'MACD', upper=True) if config.get('opt_use_macd', True) else ['X']
    bb_filters = parse_optimizer_filter_text(config.get('opt_bb_filters', 'O,X'), '볼린저', upper=True) if config.get('opt_use_bb', True) else ['X']
    st_filters = parse_optimizer_filter_text(config.get('opt_st_filters', 'O,X'), '슈퍼트렌드', upper=True) if config.get('opt_use_st', True) else ['X']
    return ma_filters, rsi_filters, mfi_filters, vol_filters, macd_filters, bb_filters, st_filters

def evaluate_strategy(params, df, fee, slippage):
    k, f_ma, f_rsi, f_mfi, f_vol, f_macd, f_bb, f_st = params

    buy_target = np.maximum(df['open'] + (df['range_prev'] * k), df['open'])

    cond_ma = (df['open'] > df[f'MA_{f_ma}'].shift(1)) if f_ma != "0" else True
    cond_rsi = (df['rsi'].shift(1) < int(f_rsi)) if f_rsi != "100" else True
    cond_mfi = (df['mfi'].shift(1) < int(f_mfi)) if f_mfi != "100" else True
    cond_macd = (df['macd'].shift(1) > df['macd_signal'].shift(1)) if f_macd == "O" else True
    cond_bb = (df['open'] > df['bb_lower'].shift(1)) if f_bb == "O" else True
    cond_st = (df['supertrend_up'].shift(1) == True) if f_st == "O" else True

    static_cond = cond_ma & cond_rsi & cond_mfi & cond_macd & cond_bb & cond_st

    if f_vol == "X":
        trigger_cond = (df['high'] >= buy_target)
        touch_col = f'first_touch_price_K_{k}'
        entry_price = df[touch_col].fillna(buy_target) if touch_col in df.columns else buy_target
    else:
        simul_col = f'simul_K_{k}_V_{float(f_vol)}'
        buy_px_col = f'buy_px_K_{k}_V_{float(f_vol)}'
        trigger_cond = df[simul_col].fillna(False) if simul_col in df.columns else pd.Series(False, index=df.index)
        entry_price = df[buy_px_col].fillna(buy_target) if buy_px_col in df.columns else buy_target

    is_buy = static_cond & trigger_cond
    applied_entry = pd.Series(np.where(is_buy, entry_price, np.nan), index=df.index)

    ror = np.where(
        is_buy,
        ((df['sell_price'] * (1 - slippage)) / (applied_entry * (1 + slippage))) * (1 - fee) * (1 - fee),
        1.0
    )

    total_trade = is_buy.sum()
    if total_trade > 0:
        hpr = pd.Series(ror, index=df.index).cumprod()
        total_return = (hpr.iloc[-1] - 1) * 100
        trade_returns = pd.Series(ror, index=df.index)[is_buy]
        win_trade = (trade_returns > 1.0).sum()
        lose_trade = total_trade - win_trade
        win_rate = (win_trade / total_trade) * 100
        cum_max = hpr.cummax()
        drawdown = (cum_max - hpr) / cum_max * 100
        mdd = drawdown.max()
    else:
        total_return, win_trade, lose_trade, win_rate, mdd = 0.0, 0, 0, 0.0, 0.0

    simple_ror = np.where(is_buy, ror - 1.0, 0.0)
    simple_hpr = 1.0 + pd.Series(simple_ror, index=df.index).cumsum()
    simple_return = (simple_hpr.iloc[-1] - 1) * 100 if total_trade > 0 else 0.0
    simple_cummax = simple_hpr.cummax()
    simple_drawdown = (simple_cummax - simple_hpr) / np.where(simple_cummax == 0, 1, simple_cummax) * 100
    simple_mdd = simple_drawdown.max() if total_trade > 0 else 0.0

    return {
        "K_Value": k,
        "이평선": f_ma,
        "RSI": f_rsi,
        "MFI": f_mfi,
        "거래량": f_vol,
        "MACD": f_macd,
        "볼린저": f_bb,
        "슈퍼트렌드": f_st,
        "복리 누적수익률(%)": round(total_return, 2),
        "단리 누적수익률(%)": round(simple_return, 2),
        "총 거래횟수": int(total_trade),
        "승률(%)": round(win_rate, 2),
        "승": int(win_trade),
        "패": int(lose_trade),
        "복리 MDD(%)": round(mdd, 2),
        "단리 MDD(%)": round(simple_mdd, 2)
    }