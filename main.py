import sys
import multiprocessing
from PyQt5.QtWidgets import QApplication

# 분리한 UI 불러오기
from gui.app import BacktestApp

if __name__ == "__main__":
    multiprocessing.freeze_support()
    app = QApplication(sys.argv)
    window = BacktestApp()
    window.show()
    sys.exit(app.exec_())