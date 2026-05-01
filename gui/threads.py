import os
import time
import pandas as pd
import numpy as np
import itertools
from multiprocessing import Pool, cpu_count
from functools import partial
from PyQt5.QtCore import pyqtSignal, QThread

# 다른 폴더(core)의 로직들 불러오기
from core.data_updater import COINDATA_DIR, CryptoDataUpdater, _get_coin_symbol_from_ticker, load_1s_data_parquet, sync_and_load_db, resample_ohlcv
from core.strategy import (
    calculate_opt_indicators, prep_base_proj_vol, compute_breakout_proj_vol, 
    add_sell_price, build_k_values_from_range, get_optimizer_filters_from_config, evaluate_strategy
)

class BacktestThread(QThread):
    log_signal = pyqtSignal(str)
    summary_signal = pyqtSignal(float, float, float, float, int, float, float, float)
    chart_signal = pyqtSignal(object, str)
    finished_signal = pyqtSignal(object, str)
    error_signal = pyqtSignal(str)

    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        try:
            ticker = self.config['ticker']
            mode = self.config['mode']
            interval = self.config['interval']
            k_value = self.config['k_value']
            fee = float(self.config['fee']) / 100
            slippage = float(self.config['slippage']) / 100
            base_type = self.config['base_type']
            
            buffer_days = 65 
            
            end_dt = pd.Timestamp.now().floor('min')
            if mode == 'days':
                days = int(self.config['days'])
                start_dt = end_dt - pd.Timedelta(days=days + buffer_days)
            else:
                start_date_str = self.config['start_date']
                end_date_str = self.config['end_date']
                start_dt = pd.to_datetime(start_date_str) - pd.Timedelta(days=buffer_days)
                end_dt = pd.to_datetime(end_date_str) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
                
            # [추가됨] 1s 데이터인 경우 파이프라인(다운로드 및 Parquet 정규화) 실행
            if base_type == '1s':
                coin_symbol = _get_coin_symbol_from_ticker(ticker)
                updater = CryptoDataUpdater(
                    base_dir=COINDATA_DIR, 
                    start_date=start_dt, 
                    end_date=end_dt, 
                    coin=coin_symbol, 
                    log_callback=self.log_signal.emit
                )
                updater.run_pipeline()

            self.log_signal.emit(f"데이터 로드: {ticker} ({base_type})")
            
            if base_type == '1s':
                raw_base_df = load_1s_data_parquet(ticker, start_dt, end_dt, self.log_signal.emit)
            else:
                raw_base_df = sync_and_load_db(ticker, start_dt, end_dt, self.log_signal.emit)
                
            if raw_base_df is None or raw_base_df.empty: 
                raise ValueError("데이터 로드 실패.")

            self.log_signal.emit(f"데이터 리샘플링: {interval}")
            raw_df = resample_ohlcv(raw_base_df, interval)

            self.log_signal.emit("보조지표 계산 중.")
            df_ind = calculate_opt_indicators(raw_df) 
            df_valid = df_ind.dropna().copy()
            
            df_base_proj = prep_base_proj_vol(raw_base_df, interval, base_type)
            try: k_list = [float(k_value)]
            except: k_list = ["동적K"]
            vol_mults = [self.config.get('vol', 'X')]
            df_valid = compute_breakout_proj_vol(df_valid, df_base_proj, k_list, vol_mults)
            
            df_valid = add_sell_price(df_valid, raw_base_df, interval, base_type)
            
            if mode == 'days': 
                valid_start = pd.Timestamp.now() - pd.Timedelta(days=days)
                df_valid = df_valid[df_valid.index >= valid_start]
            else:
                target_start = pd.to_datetime(self.config['start_date'])
                target_end = pd.to_datetime(self.config['end_date']) + pd.Timedelta(days=1, seconds=-1)
                df_valid = df_valid[(df_valid.index >= target_start) & (df_valid.index <= target_end)]
            
            if df_valid.empty: raise ValueError("유효한 데이터가 부족합니다.")

            result_df = self.run_backtest(df_valid, k_value, fee, slippage)
            
            if 'buy_time' not in result_df.columns:
                result_df['buy_time'] = result_df.index
            else:
                fallback_buy_time = pd.Series(result_df.index, index=result_df.index)
                result_df['buy_time'] = pd.to_datetime(result_df['buy_time']).where(result_df['is_buy'], pd.NaT)
                result_df.loc[result_df['is_buy'] & result_df['buy_time'].isna(), 'buy_time'] = fallback_buy_time[result_df['is_buy'] & result_df['buy_time'].isna()]
            
            total_trade = result_df['is_buy'].sum()
            win_trade = (result_df[result_df['is_buy']]['ror'] > 1.0).sum()
            win_rate = (win_trade / total_trade * 100) if total_trade > 0 else 0
            compound_return = (result_df['hpr'].iloc[-1] - 1) * 100 if not result_df.empty else 0
            compound_mdd = result_df['dd'].max() if not result_df.empty else 0
            simple_return = (result_df['simple_hpr'].iloc[-1] - 1) * 100 if not result_df.empty else 0
            simple_mdd = result_df['simple_dd'].max() if not result_df.empty else 0

            trades_ror = result_df[result_df['is_buy']]['ror']
            profits = trades_ror[trades_ror > 1.0] - 1.0
            losses = trades_ror[trades_ror < 1.0] - 1.0

            avg_profit = (profits.mean() * 100) if not profits.empty else 0.0
            avg_loss = (losses.mean() * 100) if not losses.empty else 0.0

            self.summary_signal.emit(compound_return, compound_mdd, simple_return, simple_mdd, total_trade, win_rate, avg_profit, avg_loss)
            self.chart_signal.emit(result_df, interval)
            self.finished_signal.emit(result_df, ticker)
            
        except Exception as e:
            self.error_signal.emit(str(e))

    def run_backtest(self, df, k_val, fee, slippage):
        if k_val == "동적K":
            df['target'] = df['open'] + (df['range_prev'] * df['noise_prev'])
            k_label = 'dynamic'
        else:
            k_float = float(k_val)
            df['target'] = df['open'] + (df['range_prev'] * k_float)
            k_label = k_float

        df['buy_target'] = np.maximum(df['target'], df['open'])
        df['is_price_touched'] = df['high'] >= df['buy_target']

        c = self.config
        df['pass_ma'] = (df['open'] > df[f"MA_{c['ma']}"].shift(1)) if c['ma'] != "0" else True
        df['pass_rsi'] = (df['rsi'].shift(1) < int(c['rsi'])) if c['rsi'] != "100" else True
        df['pass_mfi'] = (df['mfi'].shift(1) < int(c['mfi'])) if c['mfi'] != "100" else True
        df['pass_macd'] = (df['macd'].shift(1) > df['macd_signal'].shift(1)) if c['macd'] == "O" else True
        df['pass_bb'] = (df['open'] > df['bb_lower'].shift(1)) if c['bb'] == "O" else True
        df['pass_st'] = (df['supertrend_up'].shift(1) == True) if c['st'] == "O" else True

        if c['vol'] == "X":
            df['pass_trigger'] = df['is_price_touched']
            touch_time_col = f'first_touch_time_K_{k_label}'
            touch_price_col = f'first_touch_price_K_{k_label}'
            df['entry_time'] = df[touch_time_col] if touch_time_col in df.columns else pd.NaT
            fallback_entry = df[touch_price_col] if touch_price_col in df.columns else df['buy_target']
            df['entry_price'] = fallback_entry.fillna(df['buy_target'])
            df['pass_vol'] = True
        else:
            simul_col = f'simul_K_{k_label}_V_{float(c["vol"])}'
            buy_time_col = f'buy_time_K_{k_label}_V_{float(c["vol"])}'
            buy_px_col = f'buy_px_K_{k_label}_V_{float(c["vol"])}'
            df['pass_trigger'] = df[simul_col].fillna(False) if simul_col in df.columns else pd.Series(False, index=df.index)
            df['entry_time'] = df[buy_time_col] if buy_time_col in df.columns else pd.NaT
            fallback_entry = df[buy_px_col] if buy_px_col in df.columns else df['buy_target']
            df['entry_price'] = fallback_entry.fillna(df['buy_target'])
            df['pass_vol'] = df['pass_trigger']

        active_cond_count = sum(1 for key, val in c.items() if key in ['ma', 'rsi', 'vol', 'macd', 'bb', 'mfi', 'st'] and val not in ["0", "100", "X"]) + 1
        met_cond_count = pd.Series(0, index=df.index)
        if c['ma'] != "0": met_cond_count += df['pass_ma'].astype(int)
        if c['rsi'] != "100": met_cond_count += df['pass_rsi'].astype(int)
        if c['vol'] != "X": met_cond_count += df['pass_trigger'].astype(int)
        if c['macd'] == "O": met_cond_count += df['pass_macd'].astype(int)
        if c['bb'] == "O": met_cond_count += df['pass_bb'].astype(int)
        if c['mfi'] != "100": met_cond_count += df['pass_mfi'].astype(int)
        if c['st'] == "O": met_cond_count += df['pass_st'].astype(int)
        if c['vol'] == "X":
            met_cond_count += df['is_price_touched'].astype(int)

        df['buy_probability'] = (met_cond_count / active_cond_count) * 100
        df['strategy_cond'] = df['pass_ma'] & df['pass_rsi'] & df['pass_mfi'] & df['pass_macd'] & df['pass_bb'] & df['pass_st']
        df['is_buy'] = df['strategy_cond'] & df['pass_trigger']

        df['buy_time'] = pd.to_datetime(df['entry_time'])
        df.loc[~df['is_buy'], 'buy_time'] = pd.NaT
        df['entry_price'] = np.where(df['is_buy'], df['entry_price'], np.nan)

        df['ror'] = np.where(
            df['is_buy'],
            ((df['sell_price'] * (1 - slippage)) / (df['entry_price'] * (1 + slippage))) * (1 - fee) * (1 - fee),
            1.0
        )
        df['hpr'] = df['ror'].cumprod()
        df['dd'] = (df['hpr'].cummax() - df['hpr']) / df['hpr'].cummax() * 100
        df['simple_return'] = np.where(df['is_buy'], df['ror'] - 1.0, 0.0)
        df['simple_hpr'] = 1.0 + df['simple_return'].cumsum()
        df['simple_dd'] = (df['simple_hpr'].cummax() - df['simple_hpr']) / df['simple_hpr'].cummax() * 100
        return df

class OptimizerThread(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(str, str, object)
    error_signal = pyqtSignal(str)

    def __init__(self, config):
        super().__init__()
        self.config = config

    def run(self):
        try:
            ticker = self.config['ticker']
            mode = self.config['mode']
            interval = self.config['interval']
            fee = float(self.config['fee']) / 100
            slippage = float(self.config['slippage']) / 100
            base_type = self.config['base_type']
            
            buffer_days = 65 
            
            end_dt = pd.Timestamp.now().floor('min')
            if mode == 'days':
                days = int(self.config['days'])
                start_dt = end_dt - pd.Timedelta(days=days + buffer_days)
            else:
                start_date_str = self.config['start_date']
                end_date_str = self.config['end_date']
                start_dt = pd.to_datetime(start_date_str) - pd.Timedelta(days=buffer_days)
                end_dt = pd.to_datetime(end_date_str) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)

            # [추가됨] 1s 데이터인 경우 파이프라인(다운로드 및 Parquet 정규화) 실행
            if base_type == '1s':
                coin_symbol = _get_coin_symbol_from_ticker(ticker)
                updater = CryptoDataUpdater(
                    base_dir=COINDATA_DIR, 
                    start_date=start_dt, 
                    end_date=end_dt, 
                    coin=coin_symbol, 
                    log_callback=self.log_signal.emit
                )
                updater.run_pipeline()

            self.log_signal.emit(f"데이터 로드: {ticker} ({base_type})")
            if base_type == '1s':
                raw_base_df = load_1s_data_parquet(ticker, start_dt, end_dt, self.log_signal.emit)
            else:
                raw_base_df = sync_and_load_db(ticker, start_dt, end_dt, self.log_signal.emit)
                
            if raw_base_df is None or raw_base_df.empty: 
                raise ValueError("데이터 로드 실패")

            self.log_signal.emit(f"데이터 리샘플링: {interval}")
            raw_df = resample_ohlcv(raw_base_df, interval)

            self.log_signal.emit("보조지표 계산 중.")
            df_ind = calculate_opt_indicators(raw_df)
            df_valid = df_ind.dropna().copy()

            if mode == 'days':
                valid_start = pd.Timestamp.now() - pd.Timedelta(days=days)
                df_valid = df_valid[df_valid.index >= valid_start]
            else:
                target_start = pd.to_datetime(self.config['start_date'])
                target_end = pd.to_datetime(self.config['end_date']) + pd.Timedelta(days=1, seconds=-1)
                df_valid = df_valid[(df_valid.index >= target_start) & (df_valid.index <= target_end)]

            if df_valid.empty:
                raise ValueError("유효한 데이터가 부족합니다.")

            k_values = build_k_values_from_range(
                self.config.get('opt_k_start', '0.01'),
                self.config.get('opt_k_end', '1.00'),
                self.config.get('opt_k_step', '0.01')
            )
            ma_filters, rsi_filters, mfi_filters, vol_filters, macd_filters, bb_filters, st_filters = get_optimizer_filters_from_config(self.config)

            self.log_signal.emit(
                f"거래량 조건 스캔 (실전 모드). K {k_values[0]}~{k_values[-1]} 간격 {self.config.get('opt_k_step', '0.01')} / "
                f"MA={ma_filters} RSI={rsi_filters} MFI={mfi_filters} VOL={vol_filters}"
            )
            df_base_proj = prep_base_proj_vol(raw_base_df, interval, base_type)
            df_valid = compute_breakout_proj_vol(df_valid, df_base_proj, k_values, vol_filters)

            df_valid = add_sell_price(df_valid, raw_base_df, interval, base_type)

            actual_start_dt = df_valid.index[0].strftime('%Y-%m-%d %H:%M')
            actual_end_dt = df_valid.index[-1].strftime('%Y-%m-%d %H:%M')
            self.log_signal.emit(f"최적화 준비 완료: 총 {len(df_valid)}개 캔들 ({actual_start_dt} ~ {actual_end_dt})")

            combinations = list(itertools.product(
                k_values, ma_filters, rsi_filters, mfi_filters,
                vol_filters, macd_filters, bb_filters, st_filters
            ))

            cores = cpu_count()
            self.log_signal.emit(f"최적화 연산 시작! (총 조합 수: {len(combinations):,} / 사용 코어: {cores})")
            
            start_time = time.time()
            eval_func = partial(evaluate_strategy, df=df_valid, fee=fee, slippage=slippage)
            
            with Pool(processes=cores) as pool:
                results = pool.map(eval_func, combinations)
                
            end_time = time.time()
            self.log_signal.emit(f"연산 완료! ({end_time - start_time:.2f}초)")

            result_df = pd.DataFrame(results)
            result_df = result_df.sort_values(by=["복리 누적수익률(%)", "단리 누적수익률(%)"], ascending=False).reset_index(drop=True)
            result_df.insert(0, "복리순위", np.arange(1, len(result_df) + 1))
            result_df["단리순위"] = result_df["단리 누적수익률(%)"].rank(method='min', ascending=False).astype(int)

            desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
            day_suffix = f"_{self.config['days']}일" if mode == 'days' else f"_{self.config['start_date']}_{self.config['end_date']}"
            file_name = f"{ticker}_{interval}_{base_type}{day_suffix}_Simple_Compound_BEST.xlsx"
            file_path = os.path.join(desktop_path, file_name)

            result_df.to_excel(file_path, index=False, engine='openpyxl')
            
            top_result = result_df.iloc[0]
            msg = (f"전체 최적화 완료.\n\n"
                   f"복리 1위 수익률: {top_result['복리 누적수익률(%)']}%\n"
                   f"단리 순위: {top_result['단리순위']}위 / 단리 수익률: {top_result['단리 누적수익률(%)']}%\n"
                   f"K={top_result['K_Value']}, MA={top_result['이평선']}, RSI={top_result['RSI']}")
            
            self.finished_signal.emit(msg, file_path, result_df)
            
        except Exception as e:
            self.error_signal.emit(str(e))