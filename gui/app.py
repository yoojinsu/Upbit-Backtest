import sys
import os
import datetime
import pandas as pd
import numpy as np
import platform

from PyQt5.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, 
                             QLineEdit, QComboBox, QPushButton, QGroupBox, QTextEdit, 
                             QMessageBox, QFileDialog, QDateEdit, QRadioButton, QTabWidget, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView, 
                             QScrollArea, QCheckBox)
from PyQt5.QtCore import Qt, QDate
from PyQt5.QtGui import QFont, QColor

import matplotlib
matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

# 분리한 쓰레드 불러오기
from gui.threads import BacktestThread, OptimizerThread

if platform.system() == 'Darwin':
    plt.rcParams['font.family'] = 'AppleGothic'
else:
    plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

class BacktestApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("업비트 백테스트 by Jinsu")
        self.resize(1400, 950)
        
        self.setStyleSheet("""
            QWidget { background-color: #131722; color: #d1d4dc; font-family: 'Segoe UI', 'Malgun Gothic'; font-size: 13px; }
            QTabWidget::pane { border: 1px solid #2a2e39; background: #131722; border-radius: 6px; }
            QTabBar::tab { background: #1e222d; color: #b2b5be; padding: 10px 20px; border: 1px solid #2a2e39; border-bottom: none; border-top-left-radius: 6px; border-top-right-radius: 6px; margin-right: 2px;}
            QTabBar::tab:selected { background: #2962ff; color: #ffffff; font-weight: bold; }
            QGroupBox { border: 1px solid #2a2e39; border-radius: 6px; margin-top: 15px; background-color: #1e222d; }
            QGroupBox::title { subcontrol-origin: margin; subcontrol-position: top left; padding: 0 10px; color: #ffffff; font-weight: bold; font-size: 14px; }
            QLabel { background-color: transparent; }
            QLineEdit, QComboBox { background-color: #2a2e39; border: 1px solid #363c4e; border-radius: 4px; padding: 6px; color: #d1d4dc; }
            QLineEdit:focus, QComboBox:focus { border: 1px solid #2962ff; }
            QLineEdit:disabled, QDateEdit:disabled { background-color: #1a1e28; color: #50535e; border: 1px solid #2a2e39; }
            QPushButton { background-color: #2962ff; border-radius: 6px; padding: 10px; font-weight: bold; color: #ffffff; border: none; }
            QPushButton:hover { background-color: #1e53e5; }
            QPushButton:disabled { background-color: #2a2e39; color: #787b86; }
            QTextEdit { background-color: #1e222d; border: 1px solid #2a2e39; border-radius: 6px; padding: 8px; color: #b2b5be; }
            QDateEdit { background-color: #2a2e39; border: 1px solid #363c4e; border-radius: 4px; padding: 6px; color: #d1d4dc; }
            QDateEdit::drop-down { border: 0px; }
            QDateEdit::down-arrow { image: none; }
            QRadioButton { color: #b2b5be; font-weight: bold; }
            QRadioButton::indicator:checked { background-color: #2962ff; border: 2px solid #2962ff; border-radius: 5px; }
            QRadioButton::indicator:unchecked { background-color: #2a2e39; border: 2px solid #363c4e; border-radius: 5px; }
            
            QTableWidget { background-color: #1e222d; color: #d1d4dc; gridline-color: #2a2e39; border: 1px solid #2a2e39; border-radius: 6px; }
            QHeaderView::section { background-color: #2a2e39; color: #ffffff; font-weight: bold; border: 1px solid #1e222d; padding: 6px; }
            QTableWidget::item:selected { background-color: #2962ff; color: #ffffff; }
        """)

        self.last_result_df = None
        self.current_ticker = ""
        self.current_run_label = ""
        self.backtest_history = {}
        self.backtest_queue = []
        self.optimizer_queue = []
        self._single_run_active = False

        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        
        self.tab1 = QWidget()
        self.tab2 = QWidget()
        
        self.tabs.addTab(self.tab1, "단일 전략 백테스트")
        self.tabs.addTab(self.tab2, "전체 기간 수익률 최적화")
        
        self.init_tab1_ui()
        self.init_tab2_ui()

    def _create_ticker_combo(self, default="KRW-XRP"):
        cb = QComboBox()
        cb.setEditable(True)
        cb.addItems(["KRW-BTC", "KRW-ETH", "KRW-XRP"])
        cb.setCurrentText(default)
        return cb

    def _parse_days_input(self, days_text):
        day_values = []
        for part in str(days_text).split(','):
            part = part.strip()
            if not part:
                continue
            day_values.append(str(int(part)))
        if not day_values:
            raise ValueError("기간(Days) 값을 입력하세요. 예: 30 또는 30,60,90")
        return day_values

    def _build_backtest_label(self, config):
        mode_label = f"{config['days']}일" if config.get('mode') == 'days' else f"{config.get('start_date')}~{config.get('end_date')}"
        filters = [
            f"K={config.get('k_value')}",
            f"MA={config.get('ma')}",
            f"RSI={config.get('rsi')}",
            f"MFI={config.get('mfi')}",
            f"VOL={config.get('vol')}",
            f"MACD={config.get('macd')}",
            f"BB={config.get('bb')}",
            f"ST={config.get('st')}"
        ]
        return f"{config.get('ticker')} | {mode_label} | {config.get('interval')} | " + ', '.join(filters)

    def _ensure_unique_label(self, label):
        if label not in self.backtest_history:
            return label
        idx = 2
        while f"{label} ({idx})" in self.backtest_history:
            idx += 1
        return f"{label} ({idx})"

    def _refresh_backtest_buttons(self):
        while self.result_buttons_layout.count():
            item = self.result_buttons_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        for label in self.backtest_history.keys():
            btn = QPushButton(label)
            btn.setStyleSheet("text-align:left; padding:8px; background-color:#2a2e39; color:#d1d4dc;")
            btn.clicked.connect(lambda checked=False, key=label: self.show_backtest_result(key))
            self.result_buttons_layout.addWidget(btn)
        self.result_buttons_layout.addStretch()

    def show_backtest_result(self, label):
        data = self.backtest_history.get(label)
        if not data:
            return
        self.current_ticker = data['ticker']
        self.current_run_label = label
        self.last_result_df = data['df']
        self.update_summary(*data['summary'])
        self.update_chart(data['df'], data['interval'])
        self.export_btn.setEnabled(True)

    def _start_next_backtest(self):
        if not self.backtest_queue:
            self.run_btn.setEnabled(True)
            self.export_btn.setEnabled(self.last_result_df is not None)
            self._single_run_active = False
            return
        config = self.backtest_queue.pop(0)
        self._single_run_active = True
        self.current_ticker = config['ticker']
        self.current_run_label = self._ensure_unique_label(self._build_backtest_label(config))
        self.log(f"백테스트 시작: {self.current_run_label}")
        self.thread = BacktestThread(config)
        self.thread.log_signal.connect(self.log)
        self.thread.summary_signal.connect(self.update_summary)
        self.thread.chart_signal.connect(self.update_chart)
        self.thread.finished_signal.connect(self.on_backtest_finished)
        self.thread.error_signal.connect(self.on_error)
        self.thread.start()

    def _start_next_optimizer(self):
        if not self.optimizer_queue:
            self.opt_run_btn.setEnabled(True)
            return
        config = self.optimizer_queue.pop(0)
        self.opt_log(f"최적화 시작: {config['ticker']} / {config['days']}일")
        self.opt_thread = OptimizerThread(config)
        self.opt_thread.log_signal.connect(self.opt_log)
        self.opt_thread.finished_signal.connect(self.on_optimizer_finished)
        self.opt_thread.error_signal.connect(self.on_optimizer_error)
        self.opt_thread.start()

    def toggle_opt_indicator_inputs(self):
        toggle_pairs = [
            (self.opt_use_ma_cb, self.opt_ma_filters),
            (self.opt_use_rsi_cb, self.opt_rsi_filters),
            (self.opt_use_mfi_cb, self.opt_mfi_filters),
            (self.opt_use_vol_cb, self.opt_vol_filters),
            (self.opt_use_macd_cb, self.opt_macd_filters),
            (self.opt_use_bb_cb, self.opt_bb_filters),
            (self.opt_use_st_cb, self.opt_st_filters),
        ]
        for checkbox, widget in toggle_pairs:
            widget.setEnabled(checkbox.isChecked())

    def init_tab1_ui(self):
        main_layout = QHBoxLayout(self.tab1)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(20)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        input_group = QGroupBox("백테스트 기본 설정")
        input_layout = QGridLayout(input_group)
        self.entries = {}
        
        lbl_ticker = QLabel("종목 (Ticker):")
        lbl_ticker.setStyleSheet("color: #b2b5be;")
        input_layout.addWidget(lbl_ticker, 0, 0)
        entry_ticker = self._create_ticker_combo("KRW-XRP")
        entry_ticker.setMinimumWidth(80)
        input_layout.addWidget(entry_ticker, 0, 1)
        self.entries["Ticker:"] = entry_ticker

        lbl_base = QLabel("기준 데이터:")
        lbl_base.setStyleSheet("color: #b2b5be;")
        input_layout.addWidget(lbl_base, 1, 0)
        self.combo_base = QComboBox()
        self.combo_base.addItems(["1분봉 (1m)", "1초봉 (1s)"])
        input_layout.addWidget(self.combo_base, 1, 1)

        self.radio_days = QRadioButton("기간 (최근 N일):")
        self.radio_days.setChecked(True)
        input_layout.addWidget(self.radio_days, 2, 0)
        entry_days = QLineEdit("730")
        input_layout.addWidget(entry_days, 2, 1)
        self.entries["Days:"] = entry_days

        self.radio_period = QRadioButton("기간 (특정 일자):")
        input_layout.addWidget(self.radio_period, 3, 0)

        date_layout = QHBoxLayout()
        date_layout.setContentsMargins(0, 0, 0, 0)
        self.date_start = QDateEdit()
        self.date_start.setCalendarPopup(True)
        self.date_start.setDate(QDate.currentDate().addDays(-365))
        self.date_start.setDisplayFormat("yyyy-MM-dd")
        lbl_tilde = QLabel("~")
        lbl_tilde.setStyleSheet("color: #b2b5be;")
        lbl_tilde.setAlignment(Qt.AlignCenter)
        self.date_end = QDateEdit()
        self.date_end.setCalendarPopup(True)
        self.date_end.setDate(QDate.currentDate())
        self.date_end.setDisplayFormat("yyyy-MM-dd")
        date_layout.addWidget(self.date_start)
        date_layout.addWidget(lbl_tilde)
        date_layout.addWidget(self.date_end)
        input_layout.addLayout(date_layout, 3, 1, 1, 2)

        lbl_k = QLabel("K 값:")
        lbl_k.setStyleSheet("color: #b2b5be;")
        input_layout.addWidget(lbl_k, 4, 0)
        entry_k = QLineEdit("0.06")
        input_layout.addWidget(entry_k, 4, 1)
        self.entries["K-Value:"] = entry_k

        lbl_fee = QLabel("수수료 (%) :")
        lbl_fee.setStyleSheet("color: #b2b5be;")
        input_layout.addWidget(lbl_fee, 5, 0)
        entry_fee = QLineEdit("0.05")
        input_layout.addWidget(entry_fee, 5, 1)
        self.entries["Fee (%) :"] = entry_fee

        lbl_slip = QLabel("슬리피지 (%) :")
        lbl_slip.setStyleSheet("color: #b2b5be;")
        input_layout.addWidget(lbl_slip, 6, 0)
        entry_slip = QLineEdit("0.05")
        input_layout.addWidget(entry_slip, 6, 1)
        self.entries["Slippage (%) :"] = entry_slip
        
        lbl_tf = QLabel("타임프레임 :")
        lbl_tf.setStyleSheet("color: #b2b5be;")
        input_layout.addWidget(lbl_tf, 7, 0)
        self.combo_tf = QComboBox()
        self.combo_tf.addItems(["1일봉 (Daily)", "4시간봉 (4H)", "1시간봉 (1H)"])
        input_layout.addWidget(self.combo_tf, 7, 1)
        left_layout.addWidget(input_group)

        self.radio_days.toggled.connect(self.toggle_inputs)
        self.toggle_inputs() 

        strat_group = QGroupBox("보조지표 필터")
        strat_layout = QGridLayout(strat_group)
        strat_settings = [
            ("이동평균선 (MA):", ["0", "3", "5", "10"], "ma", "5"),
            ("RSI 제한:", ["100", "70", "80"], "rsi", "100"),
            ("MFI 제한:", ["100", "80"], "mfi", "100"),
            ("거래량 (> MA5 * X):", ["X"] + [f"{x:.1f}" for x in np.arange(1.0, 2.0 + 0.001, 0.1)], "vol", "X"),
            ("MACD:", ["O", "X"], "macd", "X"),
            ("볼린저 밴드:", ["O", "X"], "bb", "X"),
            ("슈퍼트렌드:", ["O", "X"], "st", "X")
        ]
        self.combos = {}
        row, col = 0, 0
        for label_text, values, key, default in strat_settings:
            lbl = QLabel(label_text)
            lbl.setStyleSheet("color: #b2b5be;")
            strat_layout.addWidget(lbl, row, col)
            cb = QComboBox()
            cb.addItems(values)
            cb.setCurrentText(default)
            strat_layout.addWidget(cb, row, col+1)
            self.combos[key] = cb
            col += 2
            if col > 2: col = 0; row += 1
        left_layout.addWidget(strat_group)

        control_group = QGroupBox("실행 패널")
        control_layout = QVBoxLayout(control_group)
        self.run_btn = QPushButton("백테스트 실행")
        self.run_btn.clicked.connect(self.start_backtest)
        self.export_btn = QPushButton("엑셀로 저장")
        self.export_btn.setStyleSheet("background-color: #2a2e39; color: #d1d4dc;")
        self.export_btn.setEnabled(False)
        self.export_btn.clicked.connect(self.export_to_excel)
        control_layout.addWidget(self.run_btn)
        control_layout.addWidget(self.export_btn)
        left_layout.addWidget(control_group)

        summary_group = QGroupBox("백테스트 결과 요약")
        summary_layout = QGridLayout(summary_group)

        # 복리 결과
        self.lbl_compound = QLabel("[복리]")
        self.lbl_compound.setFont(QFont("Arial", 11, QFont.Bold))
        self.lbl_compound.setStyleSheet("color: #fcca46;")
        self.lbl_return = QLabel("수익률: 0.00%")
        self.lbl_return.setFont(QFont("Arial", 12, QFont.Bold))
        self.lbl_mdd = QLabel("최대 낙폭 (MDD): 0.00%")
        self.lbl_mdd.setFont(QFont("Arial", 12, QFont.Bold))

        # 단리 결과
        self.lbl_simple = QLabel("[단리]")
        self.lbl_simple.setFont(QFont("Arial", 11, QFont.Bold))
        self.lbl_simple.setStyleSheet("color: #42a5f5;")
        self.lbl_simple_return = QLabel("수익률: 0.00%")
        self.lbl_simple_return.setFont(QFont("Arial", 12, QFont.Bold))
        self.lbl_simple_mdd = QLabel("최대 낙폭 (MDD): 0.00%")
        self.lbl_simple_mdd.setFont(QFont("Arial", 12, QFont.Bold))

        # 공통
        self.lbl_trades = QLabel("총 거래 횟수: 0")
        self.lbl_trades.setFont(QFont("Arial", 12, QFont.Bold))
        self.lbl_winrate = QLabel("승률: 0.00%")
        self.lbl_winrate.setFont(QFont("Arial", 12, QFont.Bold))
        self.lbl_avg_profit = QLabel("평균 수익: +0.00%")
        self.lbl_avg_profit.setFont(QFont("Arial", 12, QFont.Bold))
        self.lbl_avg_loss = QLabel("평균 손실: 0.00%")
        self.lbl_avg_loss.setFont(QFont("Arial", 12, QFont.Bold))

        summary_layout.addWidget(self.lbl_compound, 0, 0)
        summary_layout.addWidget(self.lbl_return, 0, 1)
        summary_layout.addWidget(self.lbl_mdd, 0, 2)
        summary_layout.addWidget(self.lbl_simple, 1, 0)
        summary_layout.addWidget(self.lbl_simple_return, 1, 1)
        summary_layout.addWidget(self.lbl_simple_mdd, 1, 2)
        summary_layout.addWidget(self.lbl_trades, 2, 0)
        summary_layout.addWidget(self.lbl_winrate, 2, 1)
        summary_layout.addWidget(self.lbl_avg_profit, 3, 0)
        summary_layout.addWidget(self.lbl_avg_loss, 3, 1)
        
        left_layout.addWidget(summary_group)

        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        left_layout.addWidget(self.log_area)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        history_group = QGroupBox("백테스트 실행 기록")
        history_layout = QVBoxLayout(history_group)
        history_layout.setContentsMargins(5, 15, 5, 5)
        history_scroll = QScrollArea()
        history_scroll.setWidgetResizable(True)
        history_scroll.setStyleSheet("background-color: #1e222d; border: 1px solid #2a2e39;")
        self.result_buttons_container = QWidget()
        self.result_buttons_layout = QVBoxLayout(self.result_buttons_container)
        self.result_buttons_layout.setContentsMargins(4, 4, 4, 4)
        self.result_buttons_layout.setSpacing(6)
        self.result_buttons_layout.addStretch()
        history_scroll.setWidget(self.result_buttons_container)
        history_layout.addWidget(history_scroll)
        right_layout.addWidget(history_group, 2)

        chart_group = QGroupBox("백테스트 수익률 차트")
        chart_layout = QVBoxLayout(chart_group)
        chart_layout.setContentsMargins(5, 15, 5, 5)
        
        self.fig = Figure(figsize=(8, 10), dpi=100)
        self.fig.patch.set_facecolor('#1e222d') 
        self.canvas = FigureCanvas(self.fig)
        self.toolbar = NavigationToolbar(self.canvas, self)
        self.toolbar.setStyleSheet("background-color: #d1d4dc;")
        chart_layout.addWidget(self.toolbar)
        self.canvas.mpl_connect('scroll_event', self.on_scroll_zoom)
        chart_layout.addWidget(self.canvas)
        right_layout.addWidget(chart_group, 8)
        
        main_layout.addWidget(left_panel, 28) 
        main_layout.addWidget(right_panel, 72)

    def init_tab2_ui(self):
        layout = QVBoxLayout(self.tab2)
        layout.setContentsMargins(15, 15, 15, 15) 
        
        # ---------------------------------------------------------
        # [상단 영역] 좌/우 2단 분할 (기본 설정 vs 보조지표 설정)
        # ---------------------------------------------------------
        top_layout = QHBoxLayout()
        top_layout.setSpacing(20)

        # --- 좌측: 1. 기본 환경 설정 ---
        settings_group = QGroupBox("1. 기본 환경 설정")
        settings_layout = QGridLayout(settings_group)
        
        lbl_opt_ticker = QLabel("대상 종목:")
        self.opt_entry_ticker = self._create_ticker_combo("KRW-XRP")
        settings_layout.addWidget(lbl_opt_ticker, 0, 0)
        settings_layout.addWidget(self.opt_entry_ticker, 0, 1)

        lbl_opt_base = QLabel("기준 데이터:")
        self.opt_combo_base = QComboBox()
        self.opt_combo_base.addItems(["1분봉 (1m)", "1초봉 (1s)"])
        settings_layout.addWidget(lbl_opt_base, 1, 0)
        settings_layout.addWidget(self.opt_combo_base, 1, 1)

        self.opt_radio_days = QRadioButton("기간 (최근 N일):")
        self.opt_radio_days.setChecked(True)
        settings_layout.addWidget(self.opt_radio_days, 2, 0)
        self.opt_entry_days = QLineEdit("300")
        settings_layout.addWidget(self.opt_entry_days, 2, 1)

        self.opt_radio_period = QRadioButton("기간 (특정 일자):")
        settings_layout.addWidget(self.opt_radio_period, 3, 0)

        opt_date_layout = QHBoxLayout()
        opt_date_layout.setContentsMargins(0, 0, 0, 0)
        self.opt_date_start = QDateEdit()
        self.opt_date_start.setCalendarPopup(True)
        self.opt_date_start.setDate(QDate.currentDate().addDays(-365))
        self.opt_date_start.setDisplayFormat("yyyy-MM-dd")
        lbl_opt_tilde = QLabel("~")
        lbl_opt_tilde.setAlignment(Qt.AlignCenter)
        self.opt_date_end = QDateEdit()
        self.opt_date_end.setCalendarPopup(True)
        self.opt_date_end.setDate(QDate.currentDate())
        self.opt_date_end.setDisplayFormat("yyyy-MM-dd")
        opt_date_layout.addWidget(self.opt_date_start)
        opt_date_layout.addWidget(lbl_opt_tilde)
        opt_date_layout.addWidget(self.opt_date_end)
        settings_layout.addLayout(opt_date_layout, 3, 1)

        lbl_opt_fee = QLabel("수수료 (%) :")
        self.opt_entry_fee = QLineEdit("0.05")
        settings_layout.addWidget(lbl_opt_fee, 4, 0)
        settings_layout.addWidget(self.opt_entry_fee, 4, 1)

        lbl_opt_slip = QLabel("슬리피지 (%) :")
        self.opt_entry_slip = QLineEdit("0.05")
        settings_layout.addWidget(lbl_opt_slip, 5, 0)
        settings_layout.addWidget(self.opt_entry_slip, 5, 1)
        
        lbl_opt_tf = QLabel("타임프레임 :")
        self.opt_combo_tf = QComboBox()
        self.opt_combo_tf.addItems(["1일봉 (Daily)", "4시간봉 (4H)", "1시간봉 (1H)"])
        settings_layout.addWidget(lbl_opt_tf, 6, 0)
        settings_layout.addWidget(self.opt_combo_tf, 6, 1)

        # 남는 공간을 밀어올리기 위한 빈 레이아웃
        settings_layout.setRowStretch(7, 1)
        top_layout.addWidget(settings_group, 1) # 좌측 비율 1


        # --- 우측: 2. 보조지표 필터 및 범위 설정 ---
        opt_filter_group = QGroupBox("2. 보조지표 필터 및 범위 설정")
        opt_filter_layout = QGridLayout(opt_filter_group)

        # K값 범위는 한 줄로 깔끔하게 배치
        k_layout = QHBoxLayout()
        k_layout.addWidget(QLabel("K 시작:"))
        self.opt_k_start = QLineEdit("0.01")
        k_layout.addWidget(self.opt_k_start)
        k_layout.addWidget(QLabel("K 종료:"))
        self.opt_k_end = QLineEdit("1.00")
        k_layout.addWidget(self.opt_k_end)
        k_layout.addWidget(QLabel("K 간격:"))
        self.opt_k_step = QLineEdit("0.01")
        k_layout.addWidget(self.opt_k_step)
        opt_filter_layout.addLayout(k_layout, 0, 0, 1, 2)

        # 가로선(구분선) 추가
        line = QWidget()
        line.setFixedHeight(1)
        line.setStyleSheet("background-color: #363c4e; margin: 10px 0;")
        opt_filter_layout.addWidget(line, 1, 0, 1, 2)

        self.opt_use_ma_cb = QCheckBox("MA 사용")
        self.opt_use_ma_cb.setChecked(True)
        self.opt_ma_filters = QLineEdit("0,3,5,10")
        
        self.opt_use_rsi_cb = QCheckBox("RSI 사용")
        self.opt_use_rsi_cb.setChecked(True)
        self.opt_rsi_filters = QLineEdit("100,70,80")
        
        self.opt_use_mfi_cb = QCheckBox("MFI 사용")
        self.opt_use_mfi_cb.setChecked(True)
        self.opt_mfi_filters = QLineEdit("100,80")
        
        self.opt_use_vol_cb = QCheckBox("거래량 사용")
        self.opt_use_vol_cb.setChecked(True)
        self.opt_vol_filters = QLineEdit("X,1.0,1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8,1.9,2.0,2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,3.0")
        
        # O/X 필터들은 한 줄로 묶어서 배치
        ox_layout = QHBoxLayout()
        self.opt_use_macd_cb = QCheckBox("MACD")
        self.opt_use_macd_cb.setChecked(True)
        self.opt_macd_filters = QLineEdit("O,X")
        self.opt_use_bb_cb = QCheckBox("볼린저")
        self.opt_use_bb_cb.setChecked(True)
        self.opt_bb_filters = QLineEdit("O,X")
        self.opt_use_st_cb = QCheckBox("슈퍼트렌드")
        self.opt_use_st_cb.setChecked(True)
        self.opt_st_filters = QLineEdit("O,X")
        
        ox_layout.addWidget(self.opt_use_macd_cb)
        ox_layout.addWidget(self.opt_macd_filters)
        ox_layout.addWidget(self.opt_use_bb_cb)
        ox_layout.addWidget(self.opt_bb_filters)
        ox_layout.addWidget(self.opt_use_st_cb)
        ox_layout.addWidget(self.opt_st_filters)

        opt_filter_layout.addWidget(self.opt_use_ma_cb, 2, 0)
        opt_filter_layout.addWidget(self.opt_ma_filters, 2, 1)
        opt_filter_layout.addWidget(self.opt_use_rsi_cb, 3, 0)
        opt_filter_layout.addWidget(self.opt_rsi_filters, 3, 1)
        opt_filter_layout.addWidget(self.opt_use_mfi_cb, 4, 0)
        opt_filter_layout.addWidget(self.opt_mfi_filters, 4, 1)
        opt_filter_layout.addWidget(self.opt_use_vol_cb, 5, 0)
        opt_filter_layout.addWidget(self.opt_vol_filters, 5, 1)
        opt_filter_layout.addLayout(ox_layout, 6, 0, 1, 2)

        # 남는 공간 밀어올리기
        opt_filter_layout.setRowStretch(7, 1)
        top_layout.addWidget(opt_filter_group, 1) # 우측 비율 1
        
        # 상단 레이아웃을 메인에 추가
        layout.addLayout(top_layout)

        # 체크박스 이벤트 연결
        for checkbox in [self.opt_use_ma_cb, self.opt_use_rsi_cb, self.opt_use_mfi_cb, self.opt_use_vol_cb,
                         self.opt_use_macd_cb, self.opt_use_bb_cb, self.opt_use_st_cb]:
            checkbox.toggled.connect(self.toggle_opt_indicator_inputs)

        self.opt_radio_days.toggled.connect(self.toggle_opt_inputs)
        self.toggle_opt_inputs()
        self.toggle_opt_indicator_inputs()

        # ---------------------------------------------------------
        # [중단 영역] 가로 꽉 차는 큰 실행 버튼
        # ---------------------------------------------------------
        self.opt_run_btn = QPushButton("🚀 전체 최적화 실행 (RUN OVERALL OPTIMIZER)")
        self.opt_run_btn.setFixedHeight(55)
        self.opt_run_btn.setStyleSheet("""
            QPushButton { font-size: 15px; font-weight: bold; margin-top: 10px; margin-bottom: 5px; }
        """)
        self.opt_run_btn.clicked.connect(self.start_optimizer)
        layout.addWidget(self.opt_run_btn)

        # 에러 등 안내 메시지용 라벨 (평소엔 안 보임)
        info_lbl = QLabel("")
        info_lbl.setStyleSheet("color: #ef5350; font-size: 13px; font-weight:bold;")
        layout.addWidget(info_lbl)

        # ---------------------------------------------------------
        # [하단 영역] 가로 100%를 쓰는 탭 기반의 결과 테이블 & 로그
        # ---------------------------------------------------------
        self.opt_result_tabs = QTabWidget()
        self.opt_result_tabs.setStyleSheet("""
            QTabBar::tab { font-size: 14px; font-weight: bold; padding: 12px 25px; }
            QTabWidget::pane { border: 1px solid #363c4e; border-top: 2px solid #2962ff; }
        """)

        # 탭 1: 복리순 결과
        self.tab_opt_compound = QWidget()
        compound_layout = QVBoxLayout(self.tab_opt_compound)
        compound_layout.setContentsMargins(0, 0, 0, 0)
        self.opt_table = QTableWidget()
        self.opt_table.setEditTriggers(QAbstractItemView.NoEditTriggers) 
        self.opt_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.opt_table.verticalHeader().setVisible(False) 
        self.opt_table.cellClicked.connect(lambda r, c: self.on_table_cell_clicked(r, c, 'opt_compound')) 
        compound_layout.addWidget(self.opt_table)
        self.opt_result_tabs.addTab(self.tab_opt_compound, "🥇 복리순 결과")

        # 탭 2: 단리순 결과
        self.tab_opt_simple = QWidget()
        simple_layout = QVBoxLayout(self.tab_opt_simple)
        simple_layout.setContentsMargins(0, 0, 0, 0)
        self.opt_simple_table = QTableWidget()
        self.opt_simple_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.opt_simple_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.opt_simple_table.verticalHeader().setVisible(False)
        self.opt_simple_table.cellClicked.connect(lambda r, c: self.on_table_cell_clicked(r, c, 'opt_simple'))
        simple_layout.addWidget(self.opt_simple_table)
        self.opt_result_tabs.addTab(self.tab_opt_simple, "🥈 단리순 결과")

        # 탭 3: 실행 로그
        self.tab_opt_log = QWidget()
        log_layout = QVBoxLayout(self.tab_opt_log)
        log_layout.setContentsMargins(0, 0, 0, 0)
        self.opt_log_area = QTextEdit()
        self.opt_log_area.setReadOnly(True)
        log_layout.addWidget(self.opt_log_area)
        self.opt_result_tabs.addTab(self.tab_opt_log, "📝 실행 로그")

        # 결과 탭 영역이 화면 하단을 꽉 채우도록 stretch 부여
        layout.addWidget(self.opt_result_tabs, stretch=1)

    def toggle_inputs(self):
        is_days = self.radio_days.isChecked()
        self.entries["Days:"].setEnabled(is_days)
        self.date_start.setEnabled(not is_days)
        self.date_end.setEnabled(not is_days)

    def toggle_opt_inputs(self):
        is_days = self.opt_radio_days.isChecked()
        self.opt_entry_days.setEnabled(is_days)
        self.opt_date_start.setEnabled(not is_days)
        self.opt_date_end.setEnabled(not is_days)

    def on_scroll_zoom(self, event):
        if event.inaxes is None: return
        base_scale = 1.2
        if event.button == 'up': scale_factor = 1 / base_scale 
        elif event.button == 'down': scale_factor = base_scale     
        else: return
        ax = event.inaxes
        cur_xlim = ax.get_xlim()
        cur_ylim = ax.get_ylim()
        xdata, ydata = event.xdata, event.ydata
        new_width = (cur_xlim[1] - cur_xlim[0]) * scale_factor
        relx = (cur_xlim[1] - xdata) / (cur_xlim[1] - cur_xlim[0])
        ax.set_xlim([xdata - new_width * (1 - relx), xdata + new_width * relx])
        new_height = (cur_ylim[1] - cur_ylim[0]) * scale_factor
        rely = (cur_ylim[1] - ydata) / (cur_ylim[1] - cur_ylim[0])
        ax.set_ylim([ydata - new_height * (1 - rely), ydata + new_height * rely])
        self.canvas.draw_idle()

    def log(self, msg):
        curr_time = datetime.datetime.now().strftime('[%H:%M:%S] ')
        self.log_area.append(curr_time + msg)
        
    def _parse_timeframe(self, tf_text):
        if "4H" in tf_text: return "minute240", 6
        elif "1H" in tf_text: return "minute60", 24
        return "day", 1

    def start_backtest(self):
        tf_interval, cpd = self._parse_timeframe(self.combo_tf.currentText())
        base_val = '1s' if '1s' in self.combo_base.currentText() else '1m'

        base_config = {
            "ticker": self.entries["Ticker:"].currentText().strip(),
            "mode": "days" if self.radio_days.isChecked() else "period",
            "days": self.entries["Days:"].text().strip(),
            "start_date": self.date_start.date().toString("yyyy-MM-dd"),
            "end_date": self.date_end.date().toString("yyyy-MM-dd"),
            "k_value": self.entries["K-Value:"].text().strip(),
            "fee": self.entries["Fee (%) :"].text().strip(),
            "slippage": self.entries["Slippage (%) :"].text().strip(),
            "interval": tf_interval,
            "candles_per_day": cpd,
            "base_type": base_val
        }
        for k, v in self.combos.items():
            base_config[k] = v.currentText()

        self.run_btn.setEnabled(False)
        self.export_btn.setEnabled(False)
        self.log_area.clear()
        self.backtest_queue = []

        if base_config["mode"] == "days":
            day_values = self._parse_days_input(base_config["days"])
            for day in day_values:
                cfg = dict(base_config)
                cfg["days"] = day
                self.backtest_queue.append(cfg)
        else:
            self.backtest_queue.append(base_config)

        self._start_next_backtest()

    def update_summary(self, compound_return, compound_mdd, simple_return, simple_mdd, trades, winrate, avg_profit, avg_loss):
        c_color = "#26a69a" if compound_return > 0 else "#ef5350"
        self.lbl_return.setText(f"수익률: {compound_return:,.2f}%")
        self.lbl_return.setStyleSheet(f"color: {c_color};")
        self.lbl_mdd.setText(f"최대 낙폭 (MDD): {compound_mdd:.2f}%")
        self.lbl_mdd.setStyleSheet("color: #ef5350;")
        
        s_color = "#26a69a" if simple_return > 0 else "#ef5350"
        self.lbl_simple_return.setText(f"수익률: {simple_return:,.2f}%")
        self.lbl_simple_return.setStyleSheet(f"color: {s_color};")
        self.lbl_simple_mdd.setText(f"최대 낙폭 (MDD): {simple_mdd:.2f}%")
        self.lbl_simple_mdd.setStyleSheet("color: #ef5350;")
        
        self.lbl_trades.setText(f"총 거래 횟수: {trades}회")
        self.lbl_trades.setStyleSheet("color: #fcca46;")
        self.lbl_winrate.setText(f"승률: {winrate:.2f}%")
        self.lbl_winrate.setStyleSheet("color: #2962ff;")
        self.lbl_avg_profit.setText(f"평균 수익: +{avg_profit:.2f}%")
        self.lbl_avg_profit.setStyleSheet("color: #26a69a;")
        self.lbl_avg_loss.setText(f"평균 손실: {avg_loss:.2f}%")
        self.lbl_avg_loss.setStyleSheet("color: #ef5350;")

    def update_chart(self, df, interval):
        self.fig.clf()
        gs = self.fig.add_gridspec(2, 1, height_ratios=[3, 1], hspace=0.1)
        ax_main = self.fig.add_subplot(gs[0, 0])
        ax_hpr = self.fig.add_subplot(gs[1, 0], sharex=ax_main)
        
        bg_color, grid_color, text_color = '#131722', '#2a2e39', '#787b86'
        for ax in [ax_main, ax_hpr]:
            ax.set_facecolor(bg_color)
            ax.tick_params(colors=text_color, labelsize=9)
            ax.yaxis.tick_right() 
            ax.grid(True, color=grid_color, linestyle='-', linewidth=0.5)
            for spine in ax.spines.values(): spine.set_color(grid_color)
                
        plt.setp(ax_main.get_xticklabels(), visible=False)

        ax_main.plot(df.index, df['close'], color='#b2b5be', linewidth=1, label='종가')
        if self.combos['bb'].currentText() == "O":
            ax_main.plot(df.index, df['bb_upper'], color='#2962ff', linewidth=0.8, alpha=0.3)
            ax_main.plot(df.index, df['bb_lower'], color='#2962ff', linewidth=0.8, alpha=0.3)
            ax_main.fill_between(df.index, df['bb_upper'], df['bb_lower'], color='#2962ff', alpha=0.05)

        buy_points = df[df['is_buy'] == True]
        if not buy_points.empty:
            ax_main.scatter(buy_points.index, buy_points['close'] * 0.95, color='#26a69a', s=40, marker='^', zorder=5, label='매수 신호')

        title_label = self.current_run_label if self.current_run_label else self.current_ticker
        ax_main.set_title(f"[{title_label}] 백테스트 가격 및 신호 ({interval})", color='#d1d4dc', fontsize=11, fontweight='bold', loc='left', pad=8)
        ax_main.legend(loc='upper left', frameon=False, labelcolor=text_color, fontsize=9)

        compound_pct = (df['hpr'] - 1) * 100
        simple_pct = (df['simple_hpr'] - 1) * 100
        ax_hpr.plot(df.index, compound_pct, color='#fcca46', linewidth=1.5, label='복리 수익률 (%)')
        ax_hpr.plot(df.index, simple_pct, color='#42a5f5', linewidth=1.5, label='단리 수익률 (%)')
        ax_hpr.axhline(0, color=text_color, linestyle='--', linewidth=0.8)
        ax_hpr.fill_between(df.index, compound_pct, 0, where=(compound_pct >= 0), color='#fcca46', alpha=0.07)
        ax_hpr.fill_between(df.index, simple_pct, 0, where=(simple_pct >= 0), color='#42a5f5', alpha=0.07)
        ax_hpr.legend(loc='upper left', frameon=False, labelcolor=text_color, fontsize=9)

        if interval == 'day': ax_hpr.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        else: ax_hpr.xaxis.set_major_formatter(mdates.DateFormatter('%y-%m-%d %H:%M'))
            
        for label in ax_hpr.get_xticklabels(): label.set_rotation(0)

        self.fig.subplots_adjust(left=0.06, right=0.92, top=0.95, bottom=0.08)
        self.canvas.draw()

    def on_backtest_finished(self, df, ticker):
        trades_mask = df['is_buy'] if 'is_buy' in df.columns else pd.Series(False, index=df.index)
        trades_ror = df.loc[trades_mask, 'ror'] if 'ror' in df.columns else pd.Series(dtype=float)
        profits = (trades_ror[trades_ror > 1.0] - 1.0) if not trades_ror.empty else pd.Series(dtype=float)
        losses = (trades_ror[trades_ror < 1.0] - 1.0) if not trades_ror.empty else pd.Series(dtype=float)
        summary = (
            (df['hpr'].iloc[-1] - 1) * 100 if not df.empty else 0.0,
            df['dd'].max() if not df.empty else 0.0,
            (df['simple_hpr'].iloc[-1] - 1) * 100 if not df.empty else 0.0,
            df['simple_dd'].max() if not df.empty else 0.0,
            int(trades_mask.sum()),
            float(((trades_ror > 1.0).sum() / trades_mask.sum()) * 100) if trades_mask.sum() > 0 else 0.0,
            float(profits.mean() * 100) if not profits.empty else 0.0,
            float(losses.mean() * 100) if not losses.empty else 0.0
        )
        self.last_result_df = df
        self.backtest_history[self.current_run_label] = {
            'df': df.copy(),
            'ticker': ticker,
            'interval': self.combo_tf.currentText().split(" ")[0],
            'summary': summary
        }
        self._refresh_backtest_buttons()
        self.show_backtest_result(self.current_run_label)
        self._start_next_backtest()

    def on_error(self, error_msg):
        self.log(str(error_msg))
        if self._single_run_active and self.backtest_queue:
            self.log("다음 백테스트를 계속 진행합니다.")
            self._start_next_backtest()
            return
        self.run_btn.setEnabled(True)
        QMessageBox.critical(self, "오류", str(error_msg))

    def export_to_excel(self):
        if self.last_result_df is None: return
        df = self.last_result_df
        folder_path = QFileDialog.getExistingDirectory(self, "엑셀 파일을 저장할 폴더를 선택하세요")
        if not folder_path: return
        
        try:
            import openpyxl
            from openpyxl.utils import get_column_letter

            def format_and_save_excel(file_path, df_export):
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    df_export.to_excel(writer, index=False, sheet_name='Sheet1')
                    worksheet = writer.sheets['Sheet1']
                    worksheet.freeze_panes = 'A2'
                    for idx, col_name in enumerate(df_export.columns, 1):
                        col_letter = get_column_letter(idx)
                        max_len = 0
                        for val in [col_name] + df_export[col_name].astype(str).tolist():
                            val_len = sum(2 if ord(c) > 127 else 1 for c in str(val))
                            if val_len > max_len:
                                max_len = val_len
                        worksheet.column_dimensions[col_letter].width = max_len + 2 

            tf_text = self.combo_tf.currentText().split(" ")[0]
            k_input = self.entries["K-Value:"].text().strip()
            k_label = float(k_input) if k_input != "동적K" else 'dynamic'
            proj_col = f'proj_vol_K_{k_label}'
            first_touch_time_col = f'first_touch_time_K_{k_label}'
            first_touch_price_col = f'first_touch_price_K_{k_label}'

            first_touch_time = pd.to_datetime(df[first_touch_time_col], errors='coerce') if first_touch_time_col in df.columns else pd.Series(pd.NaT, index=df.index)
            first_touch_price = pd.to_numeric(df[first_touch_price_col], errors='coerce') if first_touch_price_col in df.columns else pd.Series(np.nan, index=df.index)
            buy_time_series = pd.to_datetime(df['buy_time'], errors='coerce') if 'buy_time' in df.columns else pd.Series(pd.NaT, index=df.index)
            entry_price_series = pd.to_numeric(df['entry_price'], errors='coerce') if 'entry_price' in df.columns else pd.Series(np.nan, index=df.index)

            delayed_buy = (
                df['is_buy'] &
                df['is_price_touched'] &
                buy_time_series.notna() &
                first_touch_time.notna() &
                (buy_time_series > first_touch_time)
            )
            delayed_sec = ((buy_time_series - first_touch_time).dt.total_seconds()).where(delayed_buy)
            delayed_price_gap = (entry_price_series - df['buy_target']).where(delayed_buy)

            daily_df = pd.DataFrame({
                "일봉시작시간": df.index.strftime('%Y-%m-%d %H:%M:%S'), 
                "시가": df['open'].round(2), 
                "고가": df['high'].round(2), 
                "저가": df['low'].round(2),
                "종가": df['close'].round(2), 
                "10초전 매도가": df['sell_price'].round(2) if 'sell_price' in df.columns else df['close'].round(2),
                "종가 누적거래량": df['volume'].round(0), 
                "돌파시점 최댓값 예측거래량": df[proj_col].round(0) if proj_col in df.columns else 0,
                "목표 매수가": df['buy_target'].round(2),
                "최초 타겟 도달시각": first_touch_time.dt.strftime('%Y-%m-%d %H:%M:%S'),
                "최초 타겟 도달가격": first_touch_price.round(2),
                "실제 진입가": entry_price_series.round(2),
                "MA 만족": np.where(df['pass_ma'], "O", "X"),
                "RSI 만족": np.where(df['pass_rsi'], "O", "X"),
                "MFI 만족": np.where(df['pass_mfi'], "O", "X"),
                "거래량 만족": np.where(df['pass_vol'], "O", "X"),
                "MAC 준수": np.where(df['pass_macd'], "O", "X"),
                "볼린저 만족": np.where(df['pass_bb'], "O", "X"),
                "슈퍼트렌드 만족": np.where(df['pass_st'], "O", "X"),
                "타겟가 도달": np.where(df['is_price_touched'], "O", "X"),
                "거래량 미충족 후 매수": np.where(delayed_buy, "O", "X"),
                "지연초(초)": delayed_sec.fillna(0).round(0),
                "지연진입가차이": delayed_price_gap.fillna(0).round(2),
                "최종 매수여부": np.where(df['is_buy'], "O", "X"),
                "정확한 매수일시": buy_time_series.dt.strftime('%Y-%m-%d %H:%M:%S'),
                "정확한 매도일시": df['sell_time'].dt.strftime('%Y-%m-%d %H:%M:%S') if 'sell_time' in df.columns else "-"
            })
            
            label_suffix = ""
            if self.current_run_label:
                safe_label = self.current_run_label.replace("|", "_").replace(":", "-").replace(",", "_").replace("/", "-")
                label_suffix = f"_{safe_label}"
            daily_path = os.path.join(folder_path, f"{self.current_ticker}_{tf_text}{label_suffix}_State.xlsx")
            format_and_save_excel(daily_path, daily_df) 
            
            trades_df = df[df['is_buy'] == True].copy()
            if not trades_df.empty:
                buy_time_series = pd.to_datetime(trades_df['buy_time'], errors='coerce') if 'buy_time' in trades_df.columns else pd.to_datetime(trades_df.index)
                sell_time_series = pd.to_datetime(trades_df['sell_time'], errors='coerce') if 'sell_time' in trades_df.columns else (pd.to_datetime(trades_df.index) + pd.Timedelta(hours=1))
                first_touch_series = pd.to_datetime(trades_df[first_touch_time_col], errors='coerce') if first_touch_time_col in trades_df.columns else pd.Series(pd.NaT, index=trades_df.index)
                first_touch_price_series = pd.to_numeric(trades_df[first_touch_price_col], errors='coerce') if first_touch_price_col in trades_df.columns else pd.Series(np.nan, index=trades_df.index)
                entry_price_series = pd.to_numeric(trades_df['entry_price'], errors='coerce') if 'entry_price' in trades_df.columns else trades_df['buy_target']
                delayed_buy_trade = (first_touch_series.notna() & buy_time_series.notna() & (buy_time_series > first_touch_series))
                delayed_sec_trade = (buy_time_series - first_touch_series).dt.total_seconds()
                delayed_gap_trade = entry_price_series - trades_df['buy_target']

                export_df = pd.DataFrame({
                    "종목": self.current_ticker,
                    "최초 타겟 도달시각": first_touch_series.dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "최초 타겟 도달가격": first_touch_price_series.round(2),
                    "매수일시": buy_time_series.dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "매도일시": sell_time_series.dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "목표 매수가": trades_df['buy_target'].round(2),
                    "실제 진입가": entry_price_series.round(2),
                    "거래량 미충족 후 매수": np.where(delayed_buy_trade, "O", "X"),
                    "지연초(초)": delayed_sec_trade.fillna(0).round(0),
                    "지연진입가차이": delayed_gap_trade.fillna(0).round(2),
                    "백테스트 매도가": trades_df['sell_price'].round(2) if 'sell_price' in trades_df.columns else trades_df['close'].round(2),
                    "수익률(%)": ((trades_df['ror'] - 1) * 100).round(2),
                    "복리 누적수익률(%)": ((trades_df['hpr'] - 1) * 100).round(2),
                    "단리 누적수익률(%)": ((trades_df['simple_hpr'] - 1) * 100).round(2),
                    "복리 낙폭(DD %)": trades_df['dd'].round(2),
                    "단리 낙폭(DD %)": trades_df['simple_dd'].round(2)
                })
                history_path = os.path.join(folder_path, f"{self.current_ticker}_{tf_text}{label_suffix}_History.xlsx")
                format_and_save_excel(history_path, export_df) 
                
            QMessageBox.information(self, "성공", "엑셀 파일 저장이 완료되었습니다.")
            
        except ImportError:
            QMessageBox.critical(self, "오류", "openpyxl 라이브러리가 설치되어 있지 않습니다.")
        except Exception as e:
            QMessageBox.critical(self, "오류", str(e))

    def opt_log(self, msg):
        curr_time = datetime.datetime.now().strftime('[%H:%M:%S] ')
        self.opt_log_area.append(curr_time + msg)

    def start_optimizer(self):
        tf_interval, cpd = self._parse_timeframe(self.opt_combo_tf.currentText())
        base_val = '1s' if '1s' in self.opt_combo_base.currentText() else '1m'

        base_config = {
            "ticker": self.opt_entry_ticker.currentText().strip(),
            "mode": "days" if self.opt_radio_days.isChecked() else "period",
            "days": self.opt_entry_days.text().strip(),
            "start_date": self.opt_date_start.date().toString("yyyy-MM-dd"),
            "end_date": self.opt_date_end.date().toString("yyyy-MM-dd"),
            "fee": self.opt_entry_fee.text().strip(),
            "slippage": self.opt_entry_slip.text().strip(),
            "interval": tf_interval,
            "candles_per_day": cpd,
            "base_type": base_val,
            "opt_k_start": self.opt_k_start.text().strip(),
            "opt_k_end": self.opt_k_end.text().strip(),
            "opt_k_step": self.opt_k_step.text().strip(),
            "opt_ma_filters": self.opt_ma_filters.text().strip(),
            "opt_rsi_filters": self.opt_rsi_filters.text().strip(),
            "opt_mfi_filters": self.opt_mfi_filters.text().strip(),
            "opt_vol_filters": self.opt_vol_filters.text().strip(),
            "opt_macd_filters": self.opt_macd_filters.text().strip(),
            "opt_bb_filters": self.opt_bb_filters.text().strip(),
            "opt_st_filters": self.opt_st_filters.text().strip(),
            "opt_use_ma": self.opt_use_ma_cb.isChecked(),
            "opt_use_rsi": self.opt_use_rsi_cb.isChecked(),
            "opt_use_mfi": self.opt_use_mfi_cb.isChecked(),
            "opt_use_vol": self.opt_use_vol_cb.isChecked(),
            "opt_use_macd": self.opt_use_macd_cb.isChecked(),
            "opt_use_bb": self.opt_use_bb_cb.isChecked(),
            "opt_use_st": self.opt_use_st_cb.isChecked()
        }

        self.opt_run_btn.setEnabled(False)
        self.opt_log_area.clear()

        self.opt_table.setSortingEnabled(False)
        self.opt_table.clearContents()
        self.opt_table.setRowCount(0)
        self.opt_simple_table.setSortingEnabled(False)
        self.opt_simple_table.clearContents()
        self.opt_simple_table.setRowCount(0)

        self.optimizer_queue = []
        if base_config['mode'] == 'days':
            day_values = self._parse_days_input(base_config['days'])
            for day in day_values:
                cfg = dict(base_config)
                cfg['days'] = day
                self.optimizer_queue.append(cfg)
        else:
            self.optimizer_queue.append(base_config)

        self._start_next_optimizer()

    def on_optimizer_finished(self, msg, file_path, df):
        self.opt_log(msg)
        self.populate_optimizer_tables(df)
        self.opt_log(f"저장 완료: {file_path}")
        if self.optimizer_queue:
            self._start_next_optimizer()
        else:
            self.opt_run_btn.setEnabled(True)
            QMessageBox.information(self, "전체 최적화 완료", str(file_path))

    def on_optimizer_error(self, err_msg):
        self.opt_log(str(err_msg))
        if self.optimizer_queue:
            self.opt_log("다음 최적화를 계속 진행합니다.")
            self._start_next_optimizer()
        else:
            self.opt_run_btn.setEnabled(True)
            QMessageBox.critical(self, "오류", str(err_msg))

    def populate_optimizer_tables(self, df):
        if df is None or df.empty: return

        self.opt_last_df = df.copy()
        compound_df = df.sort_values(by=["복리 누적수익률(%)", "단리 누적수익률(%)"], ascending=False).reset_index(drop=True)
        compound_df["복리순위"] = np.arange(1, len(compound_df) + 1)
        compound_df["단리순위"] = compound_df["단리 누적수익률(%)"].rank(method='min', ascending=False).astype(int)

        simple_df = df.sort_values(by=["단리 누적수익률(%)", "복리 누적수익률(%)"], ascending=False).reset_index(drop=True)
        simple_df["단리순위"] = np.arange(1, len(simple_df) + 1)
        simple_df["복리순위"] = simple_df["복리 누적수익률(%)"].rank(method='min', ascending=False).astype(int)

        self.populate_table(self.opt_table, compound_df, store_target='opt_last_df')
        self.populate_table(self.opt_simple_table, simple_df, store_target=None)

    def populate_table(self, table_widget, df, store_target=None):
        if df is None or df.empty: return
        
        if store_target == 'opt_last_df':
            self.opt_last_df = df
            
        display_df = df.head(500)
        table_widget.setSortingEnabled(False)
        
        cols = list(display_df.columns)
        table_widget.setRowCount(len(display_df))
        table_widget.setColumnCount(len(cols) + 1) 
        
        headers = cols + ["적용"]
        table_widget.setHorizontalHeaderLabels(headers)
        
        for row_idx in range(len(display_df)):
            for col_idx in range(len(cols)):
                val = display_df.iloc[row_idx, col_idx]
                item = QTableWidgetItem()
                
                if isinstance(val, (int, np.integer)):
                    item.setData(Qt.DisplayRole, int(val))
                elif isinstance(val, (float, np.floating)):
                    item.setData(Qt.DisplayRole, float(val))
                else:
                    item.setData(Qt.DisplayRole, str(val))
                    
                item.setTextAlignment(Qt.AlignCenter)
                table_widget.setItem(row_idx, col_idx, item)
            
            btn_item = QTableWidgetItem("적용")
            btn_item.setTextAlignment(Qt.AlignCenter)
            btn_item.setBackground(QColor("#26a69a"))
            btn_item.setForeground(QColor("white"))
            btn_item.setData(Qt.UserRole, row_idx) 
            
            table_widget.setItem(row_idx, len(cols), btn_item)
                
        table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        table_widget.setSortingEnabled(True)

    def on_table_cell_clicked(self, row, col, source_table):
        if source_table == 'opt_compound':
            target_widget = self.opt_table
        elif source_table == 'opt_simple':
            target_widget = self.opt_simple_table
        else:
            return
            
        if target_widget and col == target_widget.columnCount() - 1:
            self.apply_strategy_to_tab1(row, source_table)

    def apply_strategy_to_tab1(self, row_idx, source_table):
        if source_table in ['opt_compound', 'opt_simple']:
            if not hasattr(self, 'opt_last_df') or self.opt_last_df is None: return
            source_widget = self.opt_table if source_table == 'opt_compound' else self.opt_simple_table
            btn_item = source_widget.item(row_idx, source_widget.columnCount() - 1)
            original_idx = btn_item.data(Qt.UserRole)
            row_data = self.opt_last_df.iloc[original_idx]
            
            ticker_input = self.opt_entry_ticker.currentText()
            fee_input = self.opt_entry_fee.text()
            slip_input = self.opt_entry_slip.text()
            combo_tf_text = self.opt_combo_tf.currentText()
            combo_base_text = self.opt_combo_base.currentText()
            is_days_radio = self.opt_radio_days.isChecked()
            days_input = self.opt_entry_days.text()
            start_date_val = self.opt_date_start.date()
            end_date_val = self.opt_date_end.date()
        
        self.entries["K-Value:"].setText(str(row_data["K_Value"]))
        self.combos["ma"].setCurrentText(str(row_data["이평선"]))
        self.combos["rsi"].setCurrentText(str(row_data["RSI"]))
        self.combos["mfi"].setCurrentText(str(row_data["MFI"]))
        self.combos["vol"].setCurrentText(str(row_data["거래량"]))
        self.combos["macd"].setCurrentText(str(row_data["MACD"]))
        self.combos["bb"].setCurrentText(str(row_data["볼린저"]))
        self.combos["st"].setCurrentText(str(row_data["슈퍼트렌드"]))
        
        self.entries["Ticker:"].setCurrentText(ticker_input)
        self.entries["Fee (%) :"].setText(fee_input)
        self.entries["Slippage (%) :"].setText(slip_input)
        self.combo_tf.setCurrentText(combo_tf_text)
        self.combo_base.setCurrentText(combo_base_text)
        
        if is_days_radio:
            self.radio_days.setChecked(True)
            self.entries["Days:"].setText(days_input)
        else:
            self.radio_period.setChecked(True)
            self.date_start.setDate(start_date_val)
            self.date_end.setDate(end_date_val)
            
        self.tabs.setCurrentIndex(0)