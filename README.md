# 가상화폐 지표를 조합한 변동성 돌파 전략 백테스트 프로그램 (업비트)

비트코인, 이더리움, 리플을 대상으로 변동성 돌파 전략을 지표와 조합하여 백테스팅하는 프로그램입니다. 

## ✨ 주요 기능 및 특징

* [cite_start]**정밀한 데이터 분석**: 1분봉 및 1초봉 데이터를 선택하여 백테스트할 수 있으며, 분석 효율을 위해 Parquet 형식으로 변환하여 처리합니다. [cite: 2]
    * [cite_start]*참고: 1초봉 데이터는 업비트 API 센터에서 2023년 9월 1일부터 제공하는 데이터를 기반으로 합니다.* 
* [cite_start]**다양한 보조지표 필터링**: 이동평균선(MA), RSI, MFI, MACD, 볼린저 밴드, 슈퍼트렌드(SuperTrend) 등을 전략 진입 조건으로 활용합니다. [cite: 1]
* [cite_start]**예상 거래량(Projected Volume)**: 1초 단위 데이터를 분석하여 해당 봉이 마감되기 전의 예상 거래량을 계산하고 매수 시점을 감지합니다. [cite: 4]
* [cite_start]**고속 최적화**: CPU 코어를 모두 활용하여 수만 가지의 파라미터 조합(K값, 지표 조합 등)을 동시에 시뮬레이션함으로써 최적의 모델을 빠르게 찾아냅니다. [cite: 5]
* [cite_start]**정밀 시뮬레이션**: 수수료와 슬리피지(Slippage)를 반영하여 실전에 가까운 복리/단리 수익률 및 MDD(최대 낙폭)를 산출합니다. [cite: 6]

---

## [cite_start]📂 프로젝트 구조 [cite: 7]

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
