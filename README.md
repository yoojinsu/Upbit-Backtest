# 가상화폐 지표를 조합한 변동성 돌파 전략 백테스트 프로그램 (업비트)

비트코인, 이더리움, 리플을 대상으로 변동성 돌파 전략을 지표와 조합하여 백테스팅하는 프로그램입니다. 

## 주요 기능 

1분봉과 1초봉으로 선택하여 백테스트할 수 있습니다. 이를 분석에 적합한 Parquet 형식으로 변환하여 백테스트 합니다. 

(1초봉은 업비트API센터에서 2023년9월1일부터 제공)

업비트 마켓데이터 주소: https://www.upbit.com/historical_data/download?prefix=candle

다양한 보조지표: 이동평균선, RSI, MFI, MACD, 볼린저 밴드, 슈퍼트렌드(SuperTrend) 등 다양한 기술적 지표를 필터링 조건으로 활용합니다.

예상 거래량: 1초 단위 데이터를 기반으로 해당 봉이 마감되기 전의 예상 거래량을 계산하여 매수 진입 시점 감지.

CPU 코어를 모두 활용하여 수만 가지의 파라미터 조합(K값, 보조지표 조합 등)을 동시에 시뮬레이션함으로써 최적의 수익률 모델을 빠르게 찾아냅니다.

수수료와 슬리피지를 반영하여 실전에 가까운 복리 및 단리 수익률, MDD(최대 낙폭)를 산출합니다.

---

파일 구조

* **`main.py`**: 프로그램 실행 시작점
* **`data_updater.py`**: 마켓 데이터 호출 및 업데이트
* **`strategy.py`**: 매매 알고리즘 핵심 로직
* **`app.py`**: 사용자 인터페이스(UI)
* **`threads.py`**: 비동기 처리를 위한 스레드 관리

---

## 🚀 실행 방법 (CMD)

아래 순서대로 명령어를 실행해 주세요.

```bash
git clone [https://github.com/yoojinsu/Upbit-Backtest.git](https://github.com/yoojinsu/Upbit-Backtest.git)
cd Upbit-Backtest
pip install -r requirements.txt
python main.py
