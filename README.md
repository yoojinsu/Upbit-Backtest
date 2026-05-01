# 📈 Upbit Crypto Backtester

본 프로젝트는 업비트(Upbit) API 기반의 암호화폐 알고리즘 트레이딩 전략 검증 및 파라미터 최적화를 위한 GUI 백테스트 프레임워크입니다. PyQt5를 활용하여 직관적인 인터페이스를 제공하며, 멀티프로세싱 기법을 적용하여 대규모 데이터 셋에 대한 연산 병목을 최소화하고 고속 최적화를 지원합니다.

## ✨ 핵심 기능 (Key Features)

- **단일 전략 검증 및 시각화 (Single Strategy Backtesting)**: 벤치마크 지표(누적 수익률, MDD, 승률, 평균 손익비 등) 산출 및 매수/매도 타점의 직관적인 차트 시각화
- **다중 지표 결합 기반 파라미터 최적화 (Parameter Optimization)**: MA, RSI, MFI, MACD, Bollinger Bands, SuperTrend 등 다양한 기술적 보조지표 조합을 통한 최적의 파라미터 탐색 (복리/단리 기준 랭킹 시스템)
- **고해상도 데이터 파이프라인 (High-Resolution Data Processing)**: 1초봉 틱(Tick) 수준의 데이터 병렬 다운로드 및 Parquet 포맷 변환을 통해 슬리피지(Slippage) 및 시장 노이즈(휩쏘)에 대한 초정밀 검증 환경 제공
- **백테스트 리포트 추출 (Data Export)**: 체결 로그, 진입 지연 시간 및 상세 매매 결과를 엑셀(Excel) 리포트 포맷으로 자동 추출

## 🚀 설치 및 실행 가이드 (Getting Started)

### 1. 저장소 클론
로컬 환경에 프로젝트 저장소를 복제하고 해당 디렉토리로 이동합니다.
```bash
git clone [https://github.com/yoojinsu/Upbit-Backtest.git](https://github.com/yoojinsu/Upbit-Backtest.git)
cd Upbit-Backtest

###2. 의존성 패키지 설치 
프로젝트 구동에 필요한 필수 파이썬 라이브러리들을 설치합니다.
'''bash
pip install pandas numpy pyupbit PyQt5 matplotlib openpyxl pyarrow requests

3. 프로그램 실행
'''bash
python Backtest.py
