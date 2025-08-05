#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2025/2/13

import sys
import multiprocessing
from PySide6.QtWidgets import QApplication
from gui import qt_application

# sys.argv += ['-platform', 'windows:darkmode=2']
port = 65308

if __name__ == '__main__':
    if not qt_application.MainWindow.is_process_running(port=port):
        multiprocessing.freeze_support()
        app = QApplication(sys.argv)
        window = qt_application.MainWindow(port)
        window.show()
        sys.exit(app.exec())
    else:
        print('process already running')
