# 📈 Upbit Crypto Backtester

업비트(Upbit) API를 활용한 암호화폐 백테스트 및 전략 최적화 프로그램입니다. PyQt5를 사용하여 직관적인 GUI를 제공하며 멀티프로세싱을 통해 최적화 연산을 빠르게 수행합니다.

## ✨ 기능 (Features)
- 단일 전략 백테스트 및 시각화 (수익률, MDD 등)
- 다중 지표(MA, RSI, MFI, MACD, 볼린저 밴드 등) 조합 전체 최적화
- 1초봉 데이터 병렬 다운로드 및 Parquet 고속 처리
- 결과 데이터 엑셀(Excel) 자동 추출 기능

## 🚀 설치 및 실행 방법 (How to Run)

1. 저장소 클론 (Clone repository)
```bash
git clone [https://github.com/본인아이디/my_crypto_backtester.git](https://github.com/본인아이디/my_crypto_backtester.git)
cd my_crypto_backtester

2. 필수 라이브러리 설치 (Install requirements)
pip install -r requirements.txt

3. 프로그램 실행 (Run)
python main.py