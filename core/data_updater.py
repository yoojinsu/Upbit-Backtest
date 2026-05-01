import sys
import os
import time
import datetime
from datetime import timedelta
import sqlite3
import pandas as pd
import pyupbit
import zipfile
import requests

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # 프로젝트 루트 기준

DB_PATH = os.path.join(BASE_DIR, "upbit_1m.db")
COINDATA_DIR = os.path.join(BASE_DIR, "Coindata")

if not os.path.exists(COINDATA_DIR):
    os.makedirs(COINDATA_DIR, exist_ok=True)

class CryptoDataUpdater:
    def __init__(self, base_dir, start_date, end_date, coin, log_callback=None):
        self.base_dir = base_dir
        self.start_date = start_date.to_pydatetime() if isinstance(start_date, pd.Timestamp) else start_date
        self.end_date = end_date.to_pydatetime() if isinstance(end_date, pd.Timestamp) else end_date
        self.coin = coin
        self.log_callback = log_callback
        
    def log(self, msg):
        if self.log_callback:
            self.log_callback(msg)
        else:
            print(msg)

    def get_paths(self):
        zip_dir = os.path.join(self.base_dir, self.coin, "1S", "ZIP")
        excel_dir = os.path.join(self.base_dir, self.coin, "1S", "Excel")
        parquet_dir = os.path.join(self.base_dir, self.coin, "parquet")
        
        for d in [zip_dir, excel_dir, parquet_dir]:
            os.makedirs(d, exist_ok=True)
            
        return zip_dir, excel_dir, parquet_dir

    def run_pipeline(self):
        self.log(f"▶️ [{self.coin}] 1초봉 데이터 자동 수집 시작 ({self.start_date.strftime('%Y-%m-%d')} ~ {self.end_date.strftime('%Y-%m-%d')})")
        self.log(f"저장 경로: {self.base_dir}")
        
        zip_dir, excel_dir, parquet_dir = self.get_paths()
        
        self.log(f"🚀 [1단계] 업비트 1초봉 ZIP 다운로드 점검")
        self.step1_download(zip_dir)
        
        self.log(f"🚀 [2단계] ZIP 압축 해제 점검")
        self.step3_unzip(zip_dir, excel_dir)
        
        self.log(f"🚀 [3단계] 일별 Parquet 정규화 변환 (86400행)")
        self.step4_to_parquet(excel_dir, parquet_dir)
        
        self.log("🎉 1초봉 데이터 준비 완료!")

    def step1_download(self, zip_dir):
        current_date = self.start_date
        while current_date <= self.end_date:
            year = current_date.strftime("%Y")
            date_str = current_date.strftime("%Y%m%d")
            
            filename = f"KRW-{self.coin}_candle-1s_{date_str}.zip"
            url = f"https://crix-data.upbit.com/candle/KRW-{self.coin}/daily/1s/{year}/{filename}"
            save_path = os.path.join(zip_dir, filename)
            
            if os.path.exists(save_path):
                current_date += timedelta(days=1)
                continue
                
            try:
                self.log(f"데이터 다운로드 중... {filename}")
                response = requests.get(url, stream=True)
                if response.status_code == 200:
                    with open(save_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                elif response.status_code == 404:
                    pass 
                else:
                    self.log(f"[에러] 서버 응답 코드 {response.status_code}: {filename}")
                    
            except requests.exceptions.RequestException as e:
                self.log(f"[오류 발생] {filename} 다운로드 중 에러: {e}")
            
            time.sleep(0.5) 
            current_date += timedelta(days=1)

    def step3_unzip(self, zip_dir, extract_dir):
        zip_files = sorted([f for f in os.listdir(zip_dir) if f.endswith('.zip')])
        count = 0
        for file in zip_files:
            zip_path = os.path.join(zip_dir, file)
            expected_csv = file.replace('.zip', '.csv')
            csv_path = os.path.join(extract_dir, expected_csv)
            
            if os.path.exists(csv_path):
                continue 
                
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    csv_files_in_zip = [name for name in zip_ref.namelist() if name.endswith('.csv')]
                    for csv_file in csv_files_in_zip:
                        zip_ref.extract(csv_file, extract_dir)
                self.log(f"압축 해제 완료: {file}")
                count += 1
            except zipfile.BadZipFile:
                self.log(f"[오류] 손상된 ZIP 파일입니다: {file}")

    def step4_to_parquet(self, extract_dir, parquet_dir):
        csv_files = sorted([f for f in os.listdir(extract_dir) if f.endswith('.csv')])
        prefix = f"KRW-{self.coin}_candle-1s_"
        count = 0
        
        for csv_file in csv_files:
            if not csv_file.startswith(prefix):
                continue
                
            date_str = csv_file.replace(prefix, "").replace(".csv", "")
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            parquet_filename = f"{self.coin}_{formatted_date}.parquet"
            parquet_path = os.path.join(parquet_dir, parquet_filename)
            
            if os.path.exists(parquet_path):
                continue 
                
            csv_path = os.path.join(extract_dir, csv_file)
            try:
                self.log(f"Parquet 정규화 변환 중... {csv_file}")
                df = pd.read_csv(csv_path)
                
                time_col = next((c for c in ['date_time_utc', 'candle_date_time_utc', 'candle_date_time_kst'] if c in df.columns), None)
                
                if time_col is None:
                    self.log(f"[오류] 시간 컬럼을 찾을 수 없습니다: {csv_file}")
                    continue

                df[time_col] = pd.to_datetime(df[time_col])
                df = df.drop_duplicates(subset=[time_col])
                df['is_traded'] = 1
                df.set_index(time_col, inplace=True)
                df.sort_index(inplace=True)
                
                base_date = df.index.date[len(df)//2] 
                start_ts = pd.Timestamp(base_date)
                end_ts = start_ts + pd.Timedelta(days=1, seconds=-1)
                full_index = pd.date_range(start=start_ts, end=end_ts, freq='1s')
                
                df = df.reindex(full_index)
                df['is_traded'] = df['is_traded'].fillna(0).astype(int)
                
                for vol_col in ['acc_trade_volume', 'acc_trade_price', 'candle_acc_trade_price', 'candle_acc_trade_volume']:
                    if vol_col in df.columns:
                        df[vol_col] = df[vol_col].fillna(0)
                        
                close_col = 'close' if 'close' in df.columns else 'trade_price'
                df[close_col] = df[close_col].ffill().bfill()
                
                no_trade_mask = df['is_traded'] == 0
                for px_col in ['open', 'high', 'low']:
                    if px_col in df.columns:
                        df.loc[no_trade_mask, px_col] = df.loc[no_trade_mask, close_col]
                        df[px_col] = df[px_col].ffill().bfill() 
                
                df.reset_index(names=[time_col], inplace=True)
                df.to_parquet(parquet_path, index=False)
                count += 1
                
            except Exception as e:
                self.log(f"[오류 발생] {csv_file} 변환 중 에러 발생: {e}")

def sync_and_load_db(ticker, start_dt, end_dt, log_callback=None):
    conn = sqlite3.connect(DB_PATH, timeout=10)
    cursor = conn.cursor()
    table_name = ticker.replace('-', '_') 
    
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} (
                        timestamp DATETIME PRIMARY KEY,
                        open REAL, high REAL, low REAL, close REAL, volume REAL)''')
    conn.commit()

    start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    df_existing = pd.read_sql(
        f"SELECT * FROM {table_name} WHERE timestamp >= '{start_str}' AND timestamp <= '{end_str}'", 
        conn, index_col='timestamp', parse_dates=['timestamp']
    )
    
    full_idx = pd.date_range(start=start_dt, end=end_dt, freq='1min')
    if df_existing.empty:
        missing_idx = full_idx
    else:
        missing_idx = full_idx.difference(df_existing.index)

    if len(missing_idx) > 0:
        if log_callback: 
            log_callback(f"DB 동기화: 1분봉 {len(missing_idx)}개 누락.")
            log_callback(f"저장 경로: {DB_PATH}")
        
        blocks = []
        block_start = missing_idx[0]
        block_end = missing_idx[0]
        
        for i in range(1, len(missing_idx)):
            if (missing_idx[i] - missing_idx[i-1]) == pd.Timedelta(minutes=1):
                block_end = missing_idx[i]
            else:
                blocks.append((block_start, block_end))
                block_start = missing_idx[i]
                block_end = missing_idx[i]
        blocks.append((block_start, block_end))
        
        total_missing_duration = sum((b_end - b_start).total_seconds() for b_start, b_end in blocks)
        if total_missing_duration == 0: total_missing_duration = 60 
        
        duration_processed_in_prev_blocks = 0
        
        for b_start, b_end in reversed(blocks):
            current_to = b_end + pd.Timedelta(minutes=1)
            block_duration = (b_end - b_start).total_seconds()
            
            while current_to > b_start:
                df_fetch = pyupbit.get_ohlcv(ticker, interval="minute1", to=current_to.strftime('%Y-%m-%d %H:%M:%S'), count=200)
                
                if df_fetch is None or df_fetch.empty:
                    current_to -= pd.Timedelta(minutes=200)
                    continue 
                
                oldest_time = df_fetch.index[0]
                
                full_fetch_idx = pd.date_range(start=oldest_time, end=df_fetch.index[-1], freq='1min')
                df_fetch = df_fetch.reindex(full_fetch_idx)
                df_fetch['close'] = df_fetch['close'].ffill().bfill()
                df_fetch['open'] = df_fetch['open'].fillna(df_fetch['close'])
                df_fetch['high'] = df_fetch['high'].fillna(df_fetch['close'])
                df_fetch['low'] = df_fetch['low'].fillna(df_fetch['close'])
                df_fetch['volume'] = df_fetch['volume'].fillna(0)
                
                records = []
                for ts, row in df_fetch.iterrows():
                    records.append((ts.strftime('%Y-%m-%d %H:%M:%S'), row['open'], row['high'], row['low'], row['close'], row['volume']))
                    
                cursor.executemany(f'''
                    INSERT OR IGNORE INTO {table_name} (timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', records)
                conn.commit()

                next_to = oldest_time - pd.Timedelta(minutes=1)
                if next_to >= current_to: next_to = current_to - pd.Timedelta(minutes=200)
                current_to = next_to

                eff_current_to = max(current_to, b_start) 
                processed_in_this_block = max((b_end - eff_current_to).total_seconds(), 0)
                
                total_processed = duration_processed_in_prev_blocks + processed_in_this_block
                progress_pct = min((total_processed / total_missing_duration) * 100, 100.0)
                
                if log_callback:
                    bar_len = 25
                    filled_len = int(bar_len * progress_pct // 100)
                    bar = '█' * filled_len + '░' * (bar_len - filled_len)
                    log_callback(f"데이터 다운로드: [{bar}] {progress_pct:.2f}% ({eff_current_to.strftime('%y-%m-%d %H:%M')})")

                time.sleep(0.12)  
                
            duration_processed_in_prev_blocks += block_duration
        
        if log_callback: log_callback("DB 동기화 완료.")
        
    df_final = pd.read_sql(
        f"SELECT * FROM {table_name} WHERE timestamp >= '{start_str}' AND timestamp <= '{end_str}'", 
        conn, index_col='timestamp', parse_dates=['timestamp']
    )
    conn.close()
    df_final.sort_index(inplace=True)
    return df_final

def _get_coin_symbol_from_ticker(ticker):
    parts = str(ticker).split('-')
    if len(parts) >= 2 and parts[-1]:
        return parts[-1].upper()
    return str(ticker).upper()

def load_1s_data_parquet(ticker, start_dt, end_dt, log_callback=None):
    root_dir = COINDATA_DIR

    start_dt = pd.to_datetime(start_dt)
    end_dt = pd.to_datetime(end_dt)

    coin_symbol = _get_coin_symbol_from_ticker(ticker)
    parquet_dir = os.path.join(root_dir, coin_symbol, "parquet")

    if not os.path.exists(parquet_dir):
        if log_callback:
            log_callback(f"1초봉 Parquet 에러: {parquet_dir} 폴더가 없습니다.")
        return pd.DataFrame()

    date_range = pd.date_range(start=start_dt.normalize(), end=end_dt.normalize(), freq='1D')
    parquet_paths = []
    missing_dates = []

    for current_date in date_range:
        file_name = f"{coin_symbol}_{current_date.strftime('%Y-%m-%d')}.parquet"
        file_path = os.path.join(parquet_dir, file_name)
        if os.path.exists(file_path):
            parquet_paths.append(file_path)
        else:
            missing_dates.append(current_date.strftime('%Y-%m-%d'))

    if missing_dates and log_callback:
        pass 

    if not parquet_paths:
        if log_callback:
            log_callback(
                f"1초봉 Parquet 에러: {start_dt.strftime('%Y-%m-%d')} ~ "
                f"{end_dt.strftime('%Y-%m-%d')} 범위의 데이터가 없습니다."
            )
        return pd.DataFrame()

    dfs = []
    for file_path in parquet_paths:
        try:
            df_day = pd.read_parquet(file_path)
            
            rename_dict = {
                'opening_price': 'open',
                'high_price': 'high',
                'low_price': 'low',
                'trade_price': 'close',
                'candle_acc_trade_volume': 'volume',
                'acc_trade_volume': 'volume'
            }
            df_day.rename(columns=rename_dict, inplace=True)
            
            time_cols = ['candle_date_time_kst', 'candle_date_time_utc', 'date_time_utc', 'timestamp']
            t_col = next((c for c in time_cols if c in df_day.columns), None)
            
            if t_col:
                if pd.api.types.is_numeric_dtype(df_day[t_col]) and t_col == 'timestamp':
                    df_day['timestamp_idx'] = pd.to_datetime(df_day[t_col], unit='ms', errors='coerce')
                else:
                    df_day['timestamp_idx'] = pd.to_datetime(df_day[t_col], errors='coerce')
                df_day.set_index('timestamp_idx', inplace=True)
                
            dfs.append(df_day)
        except Exception as e:
            if log_callback:
                log_callback(f"1초봉 Parquet 읽기 오류: {os.path.basename(file_path)} / {e}")

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, axis=0)

    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = pd.to_datetime(df.index, errors='coerce')
        except Exception:
            if log_callback:
                log_callback("오류: 날짜 인덱스를 생성할 수 없습니다. 데이터 확인 필요.")
            return pd.DataFrame()

    df = df[df.index.notnull()]

    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df.index = df.index.tz_convert('Asia/Seoul').tz_localize(None)

    df.sort_index(inplace=True)
    df = df[(df.index >= start_dt) & (df.index <= end_dt)]

    return df

def resample_ohlcv(df, interval):
    resample_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
    if interval == 'day':
        df_shifted = df.copy()
        df_shifted.index = df_shifted.index - pd.Timedelta(hours=9)
        df_res = df_shifted.resample('1D').agg(resample_dict).dropna()
        df_res.index = df_res.index + pd.Timedelta(hours=9)
    elif interval == 'minute240':
        df_shifted = df.copy()
        df_shifted.index = df_shifted.index - pd.Timedelta(hours=1)
        df_res = df_shifted.resample('4H').agg(resample_dict).dropna()
        df_res.index = df_res.index + pd.Timedelta(hours=1)
    elif interval == 'minute60':
        df_res = df.resample('1H').agg(resample_dict).dropna()
    else:
        df_res = df.copy()
    return df_res