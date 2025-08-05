#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2024/9/20

import os
import re
import ast
import time
import shutil
import psutil
import platform
import pypinyin
import threading
import subprocess
from PySide6.QtCore import QTimer, Qt, QRect, QPoint, QStringListModel, QSortFilterProxyModel, QSize, QPropertyAnimation, QEasingCurve, QEvent, QModelIndex
from PySide6.QtGui import QIcon, QPainter, QPainterPath, QColor, QPixmap, QFont, QPen
from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QProgressBar, QDialog, QCheckBox, QDockWidget, QCompleter, QComboBox
from PySide6.QtWidgets import QCalendarWidget, QScrollArea, QStyledItemDelegate, QStyleOptionButton, QStyle, QApplication, QLineEdit, QGraphicsDropShadowEffect


class TaskListgWidget(QWidget):
    def __init__(self, task_parameter: dict, main_instance, parent=None, ObjectName=None):
        super().__init__(parent)
        self.setObjectName = ObjectName
        self.main_instance = main_instance
        self.hovering = False
        if not self.main_instance.settings['transparent_window']:
            self.default_color = (243, 243, 243, 255)
            self.hover_color = (234, 234, 234, 255)
        else:
            self.default_color = (243, 243, 243, 0)
            self.hover_color = (234, 234, 234, 100)
        self.setAutoFillBackground(True)
        self.widget_height = 15
        self.setMaximumHeight(135)
        self.task_main_layout = QVBoxLayout()
        self.setLayout(self.task_main_layout)
        self.task_main_layout.setSpacing(0)
        self.task_main_layout.setContentsMargins(30, 10, 30, 10)
        self.task_top_layout = QHBoxLayout()
        self.task_top_layout.setSpacing(25)
        self.task_top_layout.setContentsMargins(0, 10, 0, 0)
        self.task_download_label_layout = QHBoxLayout()
        self.task_download_label_layout.setSpacing(10)
        self.task_download_label_layout.setContentsMargins(0, 10, 0, 0)
        self.task_download_layout = QHBoxLayout()
        self.task_download_layout.setSpacing(10)
        self.task_download_layout.setContentsMargins(0, 0, 0, 0)
        self.task_stitch_label_layout = QHBoxLayout()
        self.task_stitch_label_layout.setSpacing(10)
        self.task_stitch_label_layout.setContentsMargins(0, 10, 0, 0)
        self.task_stitch_layout = QHBoxLayout()
        self.task_stitch_layout.setSpacing(10)
        self.task_stitch_layout.setContentsMargins(0, 0, 0, 5)
        self.task_main_layout.addLayout(self.task_top_layout)
        self.task_main_layout.addLayout(self.task_download_label_layout)
        self.task_main_layout.addLayout(self.task_download_layout)
        self.task_main_layout.addLayout(self.task_stitch_label_layout)
        self.task_main_layout.addLayout(self.task_stitch_layout)
        self.is_task_closed = QTimer()
        self.is_task_closed.timeout.connect(self.check_task_closed)  # Connect timeout signal to slot
        self.is_task_started = QTimer()
        self.is_task_started.timeout.connect(self.check_task_started)  # Connect timeout signal to slot
        self.task_started = None
        self.task_closed = None
        if ast.literal_eval(f"{task_parameter['speed_calculate_list']}") is None:
            self.speed_calculate_list = []
        else:
            self.speed_calculate_list = ast.literal_eval(f"{task_parameter['speed_calculate_list']}")
        self.is_download_failed = task_parameter['is_download_failed']
        self.is_task_executing = task_parameter['is_executing']
        self.is_task_complete = task_parameter['is_complete']
        self.remaining_time = task_parameter['remaining_time']
        self.this_start_time = task_parameter['this_start_time']
        self.task_name = task_parameter['taskname']
        self.finish_timestamp = task_parameter['finishtime']
        task_name_label_text = self.task_name
        if self.is_task_executing:
            if not task_parameter['is_CalculateTiles_done']:
                download_progress_label_text = '正在计算下载区域'
                stitch_progress_label_text = '等待下载'
            elif not task_parameter['is_TileDownload_done']:
                download_progress_label_text = '正在下载'
                stitch_progress_label_text = '等待下载'
            elif not task_parameter['is_TileStitch_done']:
                download_progress_label_text = '下载完成'
                stitch_progress_label_text = '正在处理'
        else:
            download_progress_label_text = '已暂停'
            stitch_progress_label_text = ''
        self.path = task_parameter['downloadpath']
        self.task_name_label = QLabel(text='任务名称： ' + task_name_label_text, parent=self)
        self.set_label_stylesheet(self.main_instance.settings['transparent_window'], self.task_name_label)
        self.pause_button = ColorButton(parent=self)
        if self.is_task_executing:
            self.pause_button.setIcon(QIcon("resources/icon/pause.png"))
        else:
            self.pause_button.setIcon(QIcon("resources/icon/start.png"))
        self.pause_button.setStyleSheet('border: none;')
        self.pause_button.setMinimumSize(20, 20)
        self.pause_button.clicked.connect(self.pause)
        self.openfiles_button = ColorButton(parent=self)
        self.openfiles_button.setMinimumSize(20, 20)
        self.openfiles_button.setIcon(QIcon("resources/icon/openfiles.png"))
        self.openfiles_button.setStyleSheet('border: none;')
        self.openfiles_button.clicked.connect(self.openfiles)
        self.delete_task_button = ColorButton(parent=self)
        self.delete_task_button.setMinimumSize(20, 20)
        self.delete_task_button.setIcon(QIcon("resources/icon/delete.png"))
        self.delete_task_button.setStyleSheet('border: none;')
        self.delete_task_button.clicked.connect(self.delete_task)
        self.blank = QLabel(parent=self)
        self.blank.setMinimumWidth(20)
        self.blank.setMaximumHeight(self.widget_height)
        self.blank.setMinimumHeight(self.widget_height)
        self.task_top_layout.addWidget(self.task_name_label)
        self.task_top_layout.addStretch(1)
        self.task_top_layout.addWidget(self.pause_button)
        self.task_top_layout.addWidget(self.openfiles_button)
        self.task_top_layout.addWidget(self.delete_task_button)
        self.task_top_layout.addWidget(self.blank)
        # self.default_color = QtGui.QColor(243, 243, 243)  # 默认背景色
        # self.hover_color = QtGui.QColor(255, 255, 255)  # 鼠标悬停时的背景色
        # self.setAutoFillBackground(True)  # 确保背景色可以填充
        # self.update_background_color(self.default_color)
        self.download_label = QLabel(text='下载进度：', parent=self)
        self.set_label_stylesheet(self.main_instance.settings['transparent_window'], self.download_label)
        self.cost_time_label = QLabel(text='', parent=self)
        self.cost_time_label.setMaximumHeight(self.widget_height)
        self.cost_time_label.setMinimumHeight(self.widget_height)
        self.set_label_stylesheet(self.main_instance.settings['transparent_window'], self.cost_time_label)
        self.cost_time_label.hide()
        self.remaining_time_label = QLabel(text=f'{self.remaining_time}', parent=self)
        self.remaining_time_label.hide()
        self.exception_label = QLabel(text='', parent=self)
        self.set_label_stylesheet(self.main_instance.settings['transparent_window'], self.exception_label)
        self.exception_label.setStyleSheet("color: red;")
        self.exception_label.setVisible(False)
        # self.is_download_failed = True
        if self.is_download_failed:
            self.exception_label.setVisible(True)
            self.exception_label.setText('下载失败，请检查网络或代理设置')
        self.download_progress_label = QLabel(text=download_progress_label_text, parent=self)
        self.download_progress_label.setMaximumHeight(self.widget_height)
        self.download_progress_label.setMinimumHeight(self.widget_height)
        self.set_label_stylesheet(self.main_instance.settings['transparent_window'], self.download_progress_label)
        self.task_download_label_layout.addWidget(self.download_label)
        self.task_download_label_layout.addStretch(1)
        self.task_download_label_layout.addWidget(self.cost_time_label)
        self.task_download_label_layout.addWidget(self.remaining_time_label)
        self.task_download_label_layout.addWidget(self.exception_label)
        self.task_download_label_layout.addStretch(1)
        self.task_download_label_layout.addWidget(self.download_progress_label)
        self.download_progress_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        # self.download_progress_label.setVisible(False)
        # self.download_progress_label.setText('正在计算下载区域')
        self.download_progress_label.setMinimumWidth(150)
        self.download_progress_bar = CustomProgressBar(parent=self)
        self.download_progress_bar.setMaximumHeight(2)
        self.download_progress_bar.setRange(0, 100)
        self.download_progress_bar.setStyleSheet("QProgressBar {border: 1px;background-color: lightgray}")
        self.download_progress_bar.setTextVisible(False)
        self.download_progress_bar_label = QLabel(text='', parent=self)
        self.download_progress_bar_label.setMinimumWidth(35)
        self.download_progress_bar_label.setMaximumHeight(self.widget_height)
        self.download_progress_bar_label.setMinimumHeight(self.widget_height)
        self.set_label_stylesheet(self.main_instance.settings['transparent_window'], self.download_progress_bar_label)
        try:
            download_progress_value = task_parameter['downloadprogress']
            self.download_progress_bar.setFloatValue(download_progress_value)
            if download_progress_value != 0:
                self.download_progress_bar_label.setText(f'{download_progress_value:.2f}%')
            else:
                self.download_progress_bar_label.setText('0%')
        except:
            self.download_progress_bar.setFloatValue(0)
            self.download_progress_bar_label.setText('0%')
        self.task_download_layout.addWidget(self.download_progress_bar)
        self.task_download_layout.addWidget(self.download_progress_bar_label)
        self.stitch_label = QLabel(text='处理进度：', parent=self)
        self.set_label_stylesheet(self.main_instance.settings['transparent_window'], self.stitch_label)
        # self.stitch_label.setVisible(False)
        self.stitch_progress_label = QLabel(text=stitch_progress_label_text, parent=self)
        self.stitch_progress_label.setMaximumHeight(self.widget_height)
        self.stitch_progress_label.setMinimumHeight(self.widget_height)
        self.set_label_stylesheet(self.main_instance.settings['transparent_window'], self.stitch_progress_label)
        self.stitch_progress_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        # self.stitch_progress_label.setVisible(False)
        self.stitch_progress_label.setMinimumWidth(150)
        # self.stitch_progress_label.setText('等待下载')
        # self.stitch_progress_label.setStyleSheet('border: 1px solid red')
        self.task_stitch_label_layout.addWidget(self.stitch_label)
        self.task_stitch_label_layout.addStretch(1)
        self.task_stitch_label_layout.addWidget(self.stitch_progress_label)
        self.stitch_progress_bar = CustomProgressBar(parent=self)
        self.stitch_progress_bar.setRange(0, 100)
        self.stitch_progress_bar_label = QLabel(text='', parent=self)
        self.stitch_progress_bar_label.setMinimumWidth(35)
        self.stitch_progress_bar_label.setMaximumHeight(self.widget_height)
        self.stitch_progress_bar_label.setMinimumHeight(self.widget_height)
        self.set_label_stylesheet(self.main_instance.settings['transparent_window'], self.stitch_progress_bar_label)
        self.stitch_progress_bar.setMaximumHeight(2)
        self.task_stitch_layout.addWidget(self.stitch_progress_bar)
        self.task_stitch_layout.addWidget(self.stitch_progress_bar_label)
        try:
            value = task_parameter['cropprogress']
            self.stitch_progress_bar.setFloatValue(value)
            if value != 0:
                self.stitch_progress_bar_label.setText(f'{value:.2f}%')
            else:
                self.stitch_progress_bar_label.setText('0%')
            # self.stitch_progress_bar.setFloatValue(0)
            # self.stitch_progress_bar_label.setText('0%')
        except Exception as e:
            self.stitch_progress_bar.setFloatValue(0)
            self.stitch_progress_bar_label.setText('0%')
        # self.stitch_progress_bar.setVisible(False)
        self.stitch_progress_bar.setStyleSheet("QProgressBar {border: 1px;background-color: lightgray}")
        self.stitch_progress_bar.setTextVisible(False)
        self.task_timer = QTimer()
        self.task_timer.timeout.connect(self.when_task_executing)
        if self.is_task_complete:
            self.download_progress_bar.setVisible(False)
            self.stitch_progress_bar.setVisible(False)
            self.download_label.setVisible(False)
            self.download_progress_label.setVisible(False)
            self.download_progress_bar_label.setVisible(False)
            self.stitch_progress_bar_label.setVisible(False)
            self.remaining_time_label.setVisible(False)
            self.stitch_label.setText(f'{self.get_finish_time()}      已完成      {self.get_size()}')
            self.pause_button.setEnabled(False)
        if self.is_task_executing:
            if self.this_start_time is not None:
                cost = time.time() - self.this_start_time
                self.cost_time_label.setText(f'用时：{self.format_time(cost)}')
            self.cost_time_label.show()
            self.remaining_time_label.hide()
            self.delete_task_button.setEnabled(False)
            # self.download_progress_label.setVisible(True)
            # self.stitch_progress_bar.setVisible(True)
            # self.stitch_progress_label.setVisible(True)
            self.stitch_label.setVisible(True)
            self.task_timer.start(500)
            try:
                if self.main_instance.subprocess.progress_info_dict[f'{self.task_name}']['target'] == 'TileDownload':
                    if len(self.speed_calculate_list) >= 24:
                        remaining_time = self.main_instance.exists_task.loc[self.main_instance.exists_task['taskname'] == self.task_name, 'remaining_time'][0]
                        if remaining_time is not None:
                            formatted_time = self.format_time(remaining_time)
                        else:
                            formatted_time = '--:--:--'
                        # self.remaining_time_label.hide()  # 放开注释以显示剩余时间
                        self.remaining_time_label.setText(f'下载剩余：{formatted_time}')
            except Exception as e:
                pass

    def when_task_executing(self):
        try:
            if self.this_start_time is None:
                self.this_start_time = self.main_instance.exists_task.loc[self.main_instance.exists_task['taskname'] == self.task_name, 'this_start_time'][0]
            info_dict = self.main_instance.subprocess.progress_info_dict[self.task_name]
            target = info_dict['target']
            e = self.main_instance.subprocess.process_done_dict[self.task_name][f'{target}_exception']
            if e is not None:
                if 'database or disk is full' in str(e) or 'No space left on device' in str(e):
                    self.exception_label.setText('磁盘空间不足')
                elif 'database is locked' in str(e):
                    self.exception_label.setText('文件被锁定，无法写入，请尝试重启程序')
                elif 'Max retries exceeded with url' in str(e):
                    self.exception_label.setText('请检查你的网络和VPN连接')
                else:
                    self.exception_label.setText(str(e))
                self.exception_label.setVisible(True)
                self.stitch_progress_label.setText('')
                self.delete_task_button.setEnabled(True)
                self.pause_button.setIcon(QIcon("resources/icon/start.png"))
                self.task_timer.stop()  # 关闭进度显示定时器
                self.main_instance.close_task_timer(self.task_name)  # 关闭任务定时器
                # 切换dataframe中任务执行状态
                self.main_instance.exists_task.loc[self.main_instance.exists_task['taskname'] == f'{self.task_name}', 'is_executing'] = False
                self.is_task_executing = False  # 切换自身任务执行状态
                self.speed_calculate_list.clear()  # 清空下载速度数据
            else:
                cost = time.time() - self.this_start_time
                self.cost_time_label.setText(f'用时：{self.format_time(cost)}')
                if target == 'CalculateTiles':
                    self.download_progress_label.setText('正在计算下载区域')
                    self.stitch_progress_label.setText('等待下载')
                elif target == 'TileDownload':
                    self.download_progress_label.setText('正在下载')
                    self.stitch_progress_label.setText('等待下载')
                    download_total = info_dict['download_total']
                    download_success = info_dict['download_success']
                    download_fail = info_dict['download_fail']
                    self.speed_calculate_list.append((time.time(), download_success))
                    if len(self.speed_calculate_list) <= 24:
                        self.remaining_time_label.hide()
                        speed = (self.speed_calculate_list[-1][1] - self.speed_calculate_list[0][1]) / (
                                self.speed_calculate_list[-1][0] - self.speed_calculate_list[0][0])
                        if speed == 0:
                            formatted_time = '--:--:--'
                            remaining_time = None
                        else:
                            remaining_time = download_total / speed
                            formatted_time = self.format_time(remaining_time)
                    else:
                        self.speed_calculate_list.pop(0)
                        # self.remaining_time_label.show()  # 放开注释以显示剩余时间
                        speed = (self.speed_calculate_list[-1][1] - self.speed_calculate_list[0][1]) / (
                                self.speed_calculate_list[-1][0] - self.speed_calculate_list[0][0])
                        if speed == 0:
                            formatted_time = '--:--:--'
                            remaining_time = None
                        else:
                            remaining_time = download_total / speed
                            formatted_time = self.format_time(remaining_time)
                    download_progress_value = download_success / download_total * 100
                    self.main_instance.exists_task.loc[
                        self.main_instance.exists_task['taskname'] == f'{self.task_name}', 'downloadprogress'] = download_progress_value
                    if self.download_progress_bar.value() < download_progress_value:
                        self.download_progress_bar.setFloatValue(download_progress_value)
                        self.download_progress_bar_label.setText(f'{download_progress_value:.2f}%')
                    self.main_instance.exists_task.loc[self.main_instance.exists_task['taskname'] == f'{self.task_name}', 'remaining_time'] = remaining_time
                    self.main_instance.exists_task.loc[self.main_instance.exists_task['taskname'] == f'{self.task_name}', 'speed_calculate_list'] \
                        = str(self.speed_calculate_list)
                    self.remaining_time_label.setText(f'下载剩余：{formatted_time}')
                elif target == 'TileStitch':
                    self.download_progress_bar.setFloatValue(100)
                    self.download_progress_bar_label.setText('100%')

                    if self.remaining_time_label.isVisible():
                        self.remaining_time_label.hide()
                    is_restart = info_dict['is_restart']
                    stitch_total = info_dict['stitch_total']
                    stitched = info_dict['stitched_tiles']
                    crop_total = info_dict['crop_total']
                    croped = info_dict['croped_blocks']
                    stitch_status = info_dict['is_stitch_complete']
                    stitch_progress_value = stitched / stitch_total * 100
                    crop_progress_value = croped / crop_total * 100
                    self.main_instance.exists_task.loc[
                        self.main_instance.exists_task['taskname'] == f'{self.task_name}', 'stitchprogress'] = crop_progress_value
                    if not is_restart:
                        if self.stitch_progress_bar.value() < crop_progress_value:
                            self.stitch_progress_bar.setFloatValue(crop_progress_value)
                            self.stitch_progress_bar_label.setText(f'{crop_progress_value:.2f}%')
                    else:
                        self.stitch_progress_bar.setFloatValue(crop_progress_value)
                        self.stitch_progress_bar_label.setText(f'{crop_progress_value:.2f}%')
                    if stitched / stitch_total * 100 < 100:
                        self.download_progress_label.setText('下载完成')
                        self.stitch_progress_label.setText('正在处理')
                        self.remaining_time_label.setVisible(False)
                    elif stitched / stitch_total * 100 >= 100 and not stitch_status:
                        self.download_progress_label.setText('下载完成')
                        if info_dict['is_cropping_complete']:
                            self.stitch_progress_label.setText('正在写入磁盘')
                        else:
                            self.stitch_progress_label.setText('正在裁剪')
                    else:
                        self.download_progress_label.setText('下载完成')
                        self.stitch_progress_label.setText('处理完成')
                        self.remaining_time_label.setVisible(False)
                        self.task_timer.stop()
                        self.pause_button.setEnabled(False)
                        self.pause_button.setIcon(QIcon("resources/icon/start.png"))
        except Exception as e:
            pass

    @staticmethod
    def format_time(seconds):
        # 获取整数部分的秒数
        total_seconds = int(seconds)
        # 计算小时、分钟和秒
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        remaining_seconds = total_seconds % 60
        # 格式化为 "hh:mm:ss"
        if hours < 24:
            return f"{hours:02}:{minutes:02}:{remaining_seconds:02}"
        else:
            return "下载速度缓慢"

    def delete_task(self):
        confirm_delte = ConfirmDeleteDialog(self)
        confirm_delte.exec()

    def get_finish_time(self):
        try:
            local_time = time.localtime(self.finish_timestamp)
            # 格式化为年月日时分秒字符串
            formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
            return formatted_time
        except Exception as e:
            return ''

    def get_size(self):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.path):
            for file in filenames:
                file_path = os.path.join(dirpath, file)
                if os.path.isfile(file_path):
                    total_size += os.path.getsize(file_path)
        total_size_gb = f'{total_size / 1024 / 1024 / 1024:.2f}GB'
        return total_size_gb

    def openfiles(self):
        try:
            if platform.system() == 'Windows':
                os.startfile(self.path)
            elif platform.system() == 'Darwin':  # macOS
                subprocess.call(['open', self.path])
            else:  # Linux
                subprocess.call(['xdg-open', self.path])
        except:
            pass

    def is_process_low_priority(self):
        try:
            p = psutil.Process(self.main_instance.subprocess.process_dict[f'{self.task_name}'][0])
            if p.nice() > 32:  # 优先级高于 32 表示是低优先级进程，可能处于效率模式下
                return True
            else:
                return False
        except Exception as e:
            return False

    def close_task(self):
        try:
            self.main_instance.subprocess.closeone(f'{self.task_name}')
        except:
            pass
        self.task_closed = True

    def check_task_closed(self):
        if self.task_closed:
            self.download_progress_label.setText('已暂停')
            self.pause_button.setEnabled(True)
            self.delete_task_button.setEnabled(True)
            self.is_task_closed.stop()
            self.task_closed = None

    def start_task(self):
        while True:
            time.sleep(0.1)
            try:
                pid = self.main_instance.subprocess.process_dict[f'{self.task_name}'][0]
                if pid != -1:
                    break
            except Exception as e:
                pass
        self.task_started = True

    def check_task_started(self):
        if self.task_started:
            self.pause_button.setEnabled(True)
            self.is_task_started.stop()
            self.task_started = None

    def pause(self):
        if self.is_task_executing:
            self.task_timer.stop()  # 关闭进度显示定时器
            self.download_progress_label.setText('正在结束后台进程')
            self.this_start_time = None
            self.cost_time_label.setText('')
            self.remaining_time_label.setText('')
            self.pause_button.setEnabled(False)
            self.cost_time_label.hide()
            close_task_thread = threading.Thread(target=self.close_task)  # 并发线程，关闭后台进程
            close_task_thread.start()
            self.is_task_closed.start(100)  # 启用定时器
            self.stitch_progress_label.setText('')
            self.pause_button.setIcon(QIcon("resources/icon/start.png"))
            self.main_instance.close_task_timer(self.task_name)  # 关闭任务定时器
            self.is_task_executing = False  # 切换自身任务执行状态
            self.main_instance.exists_task.loc[self.main_instance.exists_task['taskname'] == f'{self.task_name}', 'remaining_time'] = None
            self.main_instance.exists_task.loc[self.main_instance.exists_task['taskname'] == f'{self.task_name}', 'speed_calculate_list'] = None
            self.main_instance.exists_task.loc[self.main_instance.exists_task['taskname'] == f'{self.task_name}', 'this_start_time'] = None
            self.remaining_time = None
            self.speed_calculate_list.clear()  # 清空下载速度数据
            # self.stitch_progress_bar_label.setText('0%')
            # self.stitch_progress_bar.setFloatValue(0)
            self.remaining_time_label.setVisible(False)
        elif not self.is_task_executing:
            rs = self.main_instance.task_execute(self.main_instance.task_path / f'{self.task_name}.xml')
            if rs:
                self.download_progress_label.setText('正在启动后台进程')
                self.cost_time_label.show()
                self.exception_label.setVisible(False)
                self.pause_button.setEnabled(False)
                self.delete_task_button.setEnabled(False)
                start_task_thread = threading.Thread(target=self.start_task)
                start_task_thread.start()
                self.is_task_started.start(100)
                self.pause_button.setIcon(QIcon("resources/icon/pause.png"))
                self.is_task_executing = True
                self.download_progress_label.setVisible(True)
                self.stitch_progress_bar.setVisible(True)
                self.stitch_progress_label.setVisible(True)
                # self.stitch_progress_bar_label.setVisible(True)
                self.stitch_label.setVisible(True)
                self.task_timer.start(500)
                self.main_instance.exists_task.loc[self.main_instance.exists_task['taskname'] == f'{self.task_name}', 'this_start_time'] = time.time()

    def enterEvent(self, event):
        self.hovering = True
        self.update()  # 触发重绘
        super().enterEvent(event)

    def leaveEvent(self, event):
        self.hovering = False
        self.update()  # 触发重绘
        super().leaveEvent(event)

    def update_style(self):
        """修改控件的背景色，保留其他样式属性"""
        original_style = self.styleSheet()
        color = self.hover_color if self.hovering else self.default_color
        # 使用正则表达式替换 background-color
        # 匹配所有 background-color 属性（不区分大小写）
        pattern = re.compile(r'background-color\s*:\s*[^;]+;?', re.IGNORECASE)
        new_style = pattern.sub(f'background-color: rgba{str(color)};', original_style)
        # 如果原样式表中没有 background-color，则添加
        if 'background-color:' not in new_style.lower():
            new_style += f'background-color: rgba{str(color)};'
        self.setStyleSheet(new_style.strip())
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        # 获取窗口的矩形区域
        rect = self.rect()
        path = QPainterPath()
        # 根据状态选择颜色
        color = QColor(*self.hover_color) if self.hovering else QColor(*self.default_color)
        # 添加圆角矩形
        path.addRoundedRect(rect.x(), rect.y(), rect.width(), rect.height(), 20, 20)
        painter.fillPath(path, color)  # 使用自定义背景色
        if not self.main_instance.settings['transparent_window']:
            self.update_style()
        # painter.drawPath(path)
        # 调用父类绘制其他内容（如子控件）
        super().paintEvent(event)
        painter.end()

    @staticmethod
    def set_label_stylesheet(transparent_window, label):
        if not transparent_window:
            color = 'black'
        else:
            color = 'white'
        label.setStyleSheet(f"""
                    color: {color}; 
                    QLabel {{
                       font-size: 18px;
                       padding: 20px;
                       color: black;
                    }}
                """)

    @staticmethod
    def set_pushbutton_stylesheet(pushbutton):
        pushbutton.setStyleSheet("""
                    QPushButton {
                        background-color: rgb(243,243,243); /* 正常状态背景色 */
                        color: black; /* 正常状态文本颜色 */
                        border: 0.5px solid rgb(219,219,219); /* 边框颜色 */
                        border-radius: 3px; /* 圆角 */
                        padding: 0px; /* 内边距 */
                    }
                    QPushButton:hover {
                        background-color: lightblue; /* 悬停时背景色 */
                    }
                    QPushButton:pressed {
                        background-color: lightblue; /* 按下时背景色 */
                    }
                    QPushButton:disabled {
                        background-color: rgb(199,199,199); /* 禁用状态背景色 */
                        color: rgb(120,120,120); /* 禁用状态文本颜色 */
                    }
            """)

    @staticmethod
    def set_checkbox_stylesheet(checkbox):
        checkbox.setStyleSheet("""
                QCheckBox {
                    border: none;
                    border-radius: 3px; /* 圆角 */
                    color: black;
                }
                QCheckBox::indicator:unchecked {
                    border: 0.5px solid rgb(219,219,219);
                    background-color: rgb(243,243,243); /* 未选中时背景颜色 */
                    border-radius: 3px;
                }
        """)


class ColorButton(QPushButton):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def enterEvent(self, event):
        if self.isEnabled():
            self.setStyleSheet("background-color: lightblue; border: none;")
        super().enterEvent(event)

    def leaveEvent(self, event):
        self.setStyleSheet("background-color: none; border: none;")
        super().leaveEvent(event)


class CustomProgressBar(QProgressBar):
    def __init__(self, parent=None):
        super().__init__(parent)

    def setFloatValue(self, value):
        # 限制在进度条范围内
        if 0 <= value < 100:
            self.setValue(int(value))
            self.setFormat(f"{value:.2f}%")
        elif int(value) >= 100:
            self.setValue(int(100))
            self.setFormat("100%")


class ConfirmDeleteDialog(QDialog):
    def __init__(self, main_instance):
        super().__init__()
        self.setWindowTitle("确认删除")
        self.main_instance = main_instance
        self.icon = QIcon()
        self.icon.addPixmap(QPixmap("resources/icon/Nevasa.ico").scaled(64, 64), QIcon.Mode.Normal, QIcon.State.Off)
        self.setWindowIcon(self.icon)
        self.setModal(True)
        self.setFixedSize(300, 150)
        # self.setWindowFlags(self.windowFlags() | QtCore.Qt.WindowType.FramelessWindowHint)
        self.setStyleSheet("background-color: rgb(243, 243, 243)")
        self.label = QLabel(text="确认要删除吗？", parent=self)
        TaskListgWidget.set_label_stylesheet(self.main_instance.main_instance.settings['transparent_window'], self.label)
        self.label.setGeometry(QRect(20, 20, 200, 20))
        self.checkbox = QCheckBox(text="同时从磁盘删除", parent=self)
        TaskListgWidget.set_checkbox_stylesheet(self.checkbox)
        self.checkbox.setGeometry(QRect(20, 60, 200, 20))
        self.button_yes = QPushButton(text="是", parent=self)
        TaskListgWidget.set_pushbutton_stylesheet(self.button_yes)
        self.button_yes.setGeometry(QRect(40, 100, 100, 25))
        self.button_no = QPushButton(text="否", parent=self)
        TaskListgWidget.set_pushbutton_stylesheet(self.button_no)
        self.button_no.setGeometry(QRect(160, 100, 100, 25))
        self.button_yes.clicked.connect(self.confirm_delete)
        self.button_no.clicked.connect(self.reject)

    def confirm_delete(self):
        try:
            self.main_instance.main_instance.close_task_timer(self.main_instance.task_name)
        except:
            pass
        try:
            self.main_instance.main_instance.subprocess.closeone(self.main_instance.task_name)
        except:
            pass
        if self.checkbox.isChecked():
            try:
                shutil.rmtree(self.main_instance.path)
            except PermissionError:
                self.main_instance.main_instance.show_msg_box('部分文件被占用，请关闭程序后手动删除')
            except Exception as e:
                print(e)
        try:
            os.remove(self.main_instance.main_instance.task_path / f'{self.main_instance.task_name}.xml')
        except:
            pass
        try:
            self.main_instance.main_instance.exists_task = (self.main_instance.main_instance.exists_task
                                                            )[self.main_instance.main_instance.exists_task['taskname'] != f'{self.main_instance.task_name}']
        except Exception as e:
            pass
        try:
            self.main_instance.main_instance.update_taskscrollarea()
            self.main_instance.main_instance.stackwidget.setCurrentIndex(1)
        except Exception as e:
            pass
        self.main_instance.main_instance.dataset_name_combobox.update_placeholdertext()
        self.accept()


class OutlinedLabel(QLabel):
    def __init__(self, text, outline_color=QColor(255, 255, 255), text_color=QColor(0, 0, 0), font_size=30, parent=None):
        super().__init__(parent)
        self.outline_color = outline_color
        self.text_color = text_color
        self.font_size = font_size
        self.setText(text)  # 设置文本

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        # 设置字体
        font = QFont("Arial", self.font_size)
        painter.setFont(font)

        # 获取文本行
        lines = self.text().split("<br>")

        # 计算文本的绘制位置
        line_height = self.font_size + 5  # 行高
        total_height = len(lines) * line_height
        start_y = (self.height() - total_height) / 2  # 垂直居中

        # 绘制轮廓文本
        painter.setPen(self.outline_color)
        for dx in [-1, 0, 1]:  # 水平偏移
            for dy in [-1, 0, 1]:  # 垂直偏移
                if dx != 0 or dy != 0:  # 排除正常文本
                    for i, line in enumerate(lines):
                        # 计算水平居中
                        start_x = (self.width() - painter.fontMetrics().boundingRect(line).width()) / 2
                        painter.drawText(int(start_x + dx), int(start_y + dy + i * line_height), line)

        # 绘制正常文本
        painter.setPen(self.text_color)  # 正常文本颜色
        for i, line in enumerate(lines):
            # 计算水平居中
            start_x = (self.width() - painter.fontMetrics().boundingRect(line).width()) / 2
            painter.drawText(int(start_x), int(start_y + i * line_height), line)
        painter.end()

    def setText(self, text):
        super().setText(text)  # 调用父类的 setText 方法
        self.update()


class DraggableDockWidget(QDockWidget):
    def __init__(self, title, parent=None):
        super().__init__(title, parent)
        # 自定义的可拖动标题栏
        title_bar = QWidget()
        self.setTitleBarWidget(title_bar)
        self.setFloating(True)  # 设置为浮动状态
        # 拖动相关变量
        self._drag_pos = QPoint()
        self._is_dragging = False
        # 连接事件
        title_bar.mousePressEvent = self.mousePressEvent
        title_bar.mouseMoveEvent = self.mouseMoveEvent
        title_bar.mouseReleaseEvent = self.mouseReleaseEvent

    def mousePressEvent(self, event):
        if self.isVisible() and event.button() == Qt.MouseButton.LeftButton:
            self._is_dragging = True
            self._drag_pos = event.position().toPoint()

    def mouseMoveEvent(self, event):
        if self.isVisible() and self._is_dragging:
            self.move(self.mapToGlobal(event.position().toPoint() - self._drag_pos))

    def mouseReleaseEvent(self, event):
        self._is_dragging = False


class DraggableWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__()
        self.setGeometry(100, 100, 200, 100)
        self.setWindowFlags(Qt.WindowType.Window | Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint)
        self.setParent(parent)
        self.dragging = False
        self.offset = QPoint()

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.dragging = True
            self.offset = event.pos()

    def mouseMoveEvent(self, event):
        if self.dragging:
            self.move(self.pos() + event.pos() - self.offset)

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.dragging = False


class CustomCompleter(QCompleter):
    def __init__(self, items, parent=None):
        super().__init__(parent)
        # 创建并保存源模型
        self.source_model = QStringListModel(items)
        # 将源模型绑定到 QCompleter
        self.setModel(self.source_model)  # 调用父类的 setModel

    def splitPath(self, path: str) -> list[str]:
        # 创建代理模型并绑定源模型
        proxy_model = CustomSortFilterProxyModel()
        proxy_model.SetSplitPath(path)
        proxy_model.setSourceModel(self.source_model)  # 使用同一源模型
        self.setModel(proxy_model)  # 动态切换补全器的模型为代理模型
        return []

    def get_source_list(self) -> list:
        """直接获取源模型的原始数据列表"""
        if isinstance(self.source_model, QStringListModel):
            return self.source_model.stringList()
        return []

    def get_filtered_list(self) -> list:
        """获取代理模型过滤后的数据列表"""
        current_model = self.model()  # 获取当前模型（可能是代理模型或源模型）
        filtered_data = []
        # 如果是代理模型（过滤后的数据）
        if isinstance(current_model, QSortFilterProxyModel):
            # 遍历代理模型的所有行
            for row in range(current_model.rowCount()):
                index = current_model.index(row, 0)  # 第0列
                data = current_model.data(index)  # 获取显示文本（Qt.DisplayRole）
                filtered_data.append(data)
        # 如果是源模型（未过滤的完整数据）
        elif isinstance(current_model, QStringListModel):
            filtered_data = current_model.stringList()
        return filtered_data


class CustomSortFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.split_path = ''

    def SetSplitPath(self, split_path):
        self.split_path = split_path.upper()

    def filterAcceptsRow(self, source_row: int, source_parent) -> bool:
        index = self.sourceModel().index(source_row, 0, source_parent)
        word = self.sourceModel().data(index)
        # 匹配逻辑（保持你的原有实现）
        if (self.split_path in word.upper() or
                self.split_path in ''.join(pypinyin.lazy_pinyin(word)).upper() or
                self.split_path in ''.join([item[0].upper() for item in pypinyin.pinyin(word, style=pypinyin.Style.FIRST_LETTER, heteronym=False)])):
            return True
        return False


class CustomLineEdit(QComboBox):
    def __init__(self, main_intance, PlaceholderText='', parent=None, ObjectName=None):
        super().__init__(parent)
        self.FoucsInPlaceholderText = PlaceholderText
        self.main_intance = main_intance
        self.setObjectName(ObjectName)
        self.setEditable(True)
        self.lineEdit().setPlaceholderText(PlaceholderText)

    def focusInEvent(self, event):
        # 当获取焦点时修改PlaceholderText
        self.lineEdit().setPlaceholderText(self.FoucsInPlaceholderText)
        super().focusInEvent(event)  # 调用父类方法以保持默认行为

    def focusOutEvent(self, event):
        # 当失去焦点时修改PlaceholderText
        foucs_out_placeholder_text = self.get_foucsoutplaceholdertext()
        if foucs_out_placeholder_text != '':
            self.lineEdit().setPlaceholderText(foucs_out_placeholder_text)
        else:
            self.lineEdit().setPlaceholderText(self.FoucsInPlaceholderText)
        super().focusOutEvent(event)  # 调用父类方法以保持默认行为

    def get_foucsoutplaceholdertext(self):
        return self.main_intance.get_default_datasetname()

    def update_placeholdertext(self):
        self.lineEdit().setPlaceholderText(self.get_foucsoutplaceholdertext())
        self.update()


class CustomCalendar(QCalendarWidget):
    def __init__(self, parent=None, ObjectName=None):
        super().__init__()
        self.setParent(parent)
        self.setObjectName(ObjectName)

    def paintCell(self, painter, rect, date):
        # 获取当前日期和本月日期
        current_date = self.selectedDate()
        current_month = current_date.month()
        current_year = current_date.year()

        # 获取当前显示的月份和年份（这将根据视图中的月份动态变化）
        displayed_month = self.monthShown()
        displayed_year = self.yearShown()

        # 判断该日期是否属于当前显示的月份
        if date.month() != displayed_month or date.year() != displayed_year or date < self.minimumDate() or date > self.maximumDate():
            # 设置非当前显示月份日期为灰色字体
            painter.setPen(QColor(169, 169, 169))  # 灰色字体
        else:
            # 设置当前显示月份的日期为黑色字体
            painter.setPen(QColor(0, 0, 0))  # 黑色字体

        # 绘制日期
        painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, str(date.day()))


class CustomTooltip(QWidget):
    def __init__(self, show_time=3000, min_size=QSize(200, 100), max_size=QSize(800, 800), line_wrap=True):
        super().__init__()
        self.show_time = show_time
        self.setWindowFlags(Qt.ToolTip | Qt.FramelessWindowHint | Qt.WindowDoesNotAcceptFocus)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setStyleSheet("""
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, 
                                stop:0 #FFF8DC, stop:1 #FFE4B5);
                    color: #333;
                    border: 1px solid #CDB38B;
                    border-radius: 5px;
                    padding: 8px;
                """)
        # 设置最小和最大尺寸
        self.min_size = min_size
        self.max_size = max_size
        self.setMaximumSize(self.max_size)
        # 使用垂直布局
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        # 初始化 QScrollArea
        self.scroll_area = QScrollArea(self)
        self.scroll_area.setWidgetResizable(True)  # 设置滚动区域内的 widget 可根据区域尺寸自动调整
        self.scroll_area.setStyleSheet("border: none")
        layout.addWidget(self.scroll_area)
        vertical_bar_style = """
                    QScrollBar::sub-line, QScrollBar::add-line {
                        height: 0px;
                        subcontrol-origin: margin;
                    }
                """
        horizontal_bar_style = """
                    QScrollBar::sub-line, QScrollBar::add-line {
                        width: 0px;
                        subcontrol-origin: margin;
                    }
                """
        self.scroll_area.verticalScrollBar().setStyleSheet(vertical_bar_style)
        self.scroll_area.horizontalScrollBar().setStyleSheet(horizontal_bar_style)
        self.label_widget = QWidget()
        self.label_widget_layout = QVBoxLayout()
        self.label_widget_layout.setContentsMargins(0, 0, 0, 0)
        self.label_widget.setLayout(self.label_widget_layout)
        # 初始化标签，并设置文本、自动换行
        self.label = QLabel(self)
        self.label_widget_layout.addWidget(self.label)
        self.label_widget_layout.addStretch(1)
        self.scroll_area.setWidget(self.label_widget)
        self.set_line_wrap(line_wrap)
        # 动画效果
        self.opacity_anim = QPropertyAnimation(self, b"windowOpacity")
        self.opacity_anim.setDuration(500)  # 动画时长500ms
        self.opacity_anim.setEasingCurve(QEasingCurve.OutQuad)

    def set_line_wrap(self, enable: bool):
        """启用/禁用自动换行"""
        self.line_wrap = enable
        self.label.setWordWrap(enable)

    def calculate_best_size(self, text: str) -> QSize:
        """计算最佳显示尺寸"""
        # 创建临时标签用于尺寸计算
        self.label.adjustSize()
        temp_label = QLabel(text=text)
        # 计算新尺寸，限制在 min_size 与 max_size 之间
        new_width = min(max(temp_label.sizeHint().width(), self.min_size.width()), self.max_size.width())
        temp_label.setWordWrap(True)
        temp_label.setFixedWidth(new_width)
        new_height = min(max(temp_label.sizeHint().height() + 50, self.min_size.height()), self.max_size.height())
        # 调整窗口尺寸
        return QSize(new_width, new_height)

    def show_tooltip(self, pos: QPoint, text: str):
        """显示提示信息"""
        best_size = self.calculate_best_size(text)
        self.setFixedSize(best_size)
        self.label.setText(text)
        self.move(pos + QPoint(25, 10))
        self.setWindowOpacity(1.0)
        self.show()
        # 设置自动关闭定时器
        self.hide_timer = QTimer(singleShot=True)
        self.hide_timer.timeout.connect(self.fade_out)
        self.hide_timer.start(self.show_time)

    def fade_out(self):
        """淡出隐藏"""
        self.opacity_anim.setStartValue(1.0)
        self.opacity_anim.setEndValue(0.0)
        self.opacity_anim.start()
        self.opacity_anim.finished.connect(self.hide)
        try:
            self.hide_timer.deleteLater()
        except Exception:
            pass


class MultiSelectDelegate(QStyledItemDelegate):
    def __init__(self, tooltip_dict=None, show_time=3000, parent=None):
        super().__init__(parent)
        # tooltip_dict 保存各项对应的提示文本，若未传入则为空字典
        self.tooltip_dict = tooltip_dict or {}
        self.show_time = show_time
        self._last_index = -1
        self._view = None

    def set_tooltip_dict(self, tooltip_dict):
        self.tooltip_dict = tooltip_dict

    def paint(self, painter, option, index):
        item = index.model().itemFromIndex(index)
        check_rect = QRect(option.rect)
        check_rect.setWidth(20)
        text_rect = option.rect.adjusted(25, 0, 0, 0)
        # 绘制复选框：使用 QStyleOptionButton，根据 item 的 checkState 绘制
        check_box_option = QStyleOptionButton()
        check_box_option.rect = check_rect
        check_box_option.state = QStyle.State_Enabled | (
            QStyle.State_On if item.checkState() == Qt.Checked else QStyle.State_Off
        )
        QApplication.style().drawControl(QStyle.CE_CheckBox, check_box_option, painter)
        # 绘制文本
        painter.drawText(text_rect, Qt.AlignVCenter, item.text())

    def sizeHint(self, option, index):
        size = super().sizeHint(option, index)
        return QSize(size.width() + 20, size.height())

    def install_on_viewport(self, view):
        self._view = view
        view.viewport().installEventFilter(self)
        view.setMouseTracking(True)

    def eventFilter(self, obj, event):
        if event.type() == QEvent.MouseMove:
            pos = event.position().toPoint() if hasattr(event, "position") else event.pos()
            index = self._view.indexAt(pos)
            if index.isValid():
                self.showToolTip(index, pos)
            else:
                self._last_index = -1
                self.tooltip.fade_out()
        elif event.type() == QEvent.Leave:
            self._last_index = -1
            self.tooltip.fade_out()
        return super().eventFilter(obj, event)

    def showToolTip(self, index, pos):
        if index.row() == self._last_index:
            return
        item_text = index.data()
        if item_text != '全选':
            tip_text = self.tooltip_dict.get(item_text, "暂无说明")
            global_pos = self._view.viewport().mapToGlobal(pos)
            try:
                self.tooltip.deleteLater()
            except:
                pass
            self.tooltip = CustomTooltip(show_time=self.show_time)
            self.tooltip.show_tooltip(global_pos, tip_text)
            self._last_index = index.row()


class CheckableComboBox(QComboBox):
    def __init__(self, parent=None):
        super(CheckableComboBox, self).__init__(parent)
        self.setLineEdit(QLineEdit())
        self.lineEdit().setReadOnly(True)
        # 通过 clicked 信号处理点击事件（这里使用 clicked 信号而非 pressed）
        self.view().clicked.connect(self.selectItemAction)
        self.SelectAllStatus = 1
        # 设置自定义委托，此处传入自身作为父控件，可后续通过外部设置 tooltip 数据
        self._delegate = MultiSelectDelegate(parent=self)
        self.setItemDelegate(self._delegate)
        # 安装事件过滤器到 view 的 viewport（用于 tooltip 显示）
        self._delegate.install_on_viewport(self.view())
        # 设置复选框的样式（可根据需要修改）
        self.setStyleSheet("""
                    /* 自定义复选框的样式 */
                    QCheckBox::indicator {
                        width: 18px;
                        height: 18px;
                        background-color: rgb(243, 243, 243);
                        border-radius: 4px;
                    }
                    QCheckBox::indicator:checked {
                        background-color: #4caf50;
                        border: 2px solid #388e3c;
                    }
                    QCheckBox::indicator:unchecked {
                        background-color: rgb(243, 243, 243);
                        border: 2px solid #d32f2f;
                    }
                    QCheckBox::indicator:hover {
                        background-color: rgb(243, 243, 243);
                    }
                    QCheckBox::indicator:checked:hover {
                        background-color: #66bb6a;
                    }
                    QCheckBox::indicator:disabled {
                        background-color: #c1c1c1;
                        border: 2px solid #9e9e9e;
                    }
                """)

    def set_tooltip_dict(self, tooltip_dict):
        self._delegate.set_tooltip_dict(tooltip_dict)

    def addItem(self, text):
        super().addItem(text)
        # 若下拉框中没有“全选”，则先添加
        if self.findText('全选') == -1:
            super().addItem('全选')
        item = self.model().item(self.count() - 1, 0)
        item.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)
        item.setCheckState(Qt.CheckState.Unchecked)
        item.setToolTip(text)

    def addItems(self, texts):
        if texts:
            self.addItem('全选')
        for text in texts:
            self.addItem(text)

    def ifChecked(self, index):
        item = self.model().item(index, 0)
        return item.checkState() == Qt.CheckState.Checked

    def checkedItems(self):
        return [self.itemText(i) for i in range(self.count()) if self.ifChecked(i)]

    def checkedItemsStr(self):
        # 去掉“全选”字符串后再合并
        return ','.join(self.checkedItems()).replace('全选', '').strip(', ')

    def selectItemAction(self, index):
        if index.row() == 0:
            # 点击“全选”行时，切换所有项的状态
            for i in range(self.model().rowCount()):
                if self.SelectAllStatus:
                    self.model().item(i).setCheckState(Qt.CheckState.Checked)
                else:
                    self.model().item(i).setCheckState(Qt.CheckState.Unchecked)
            self.SelectAllStatus = (self.SelectAllStatus + 1) % 2
        self.lineEdit().clear()
        self.lineEdit().setText(self.checkedItemsStr())

    def clear(self) -> None:
        super().clear()

    def select_all(self):
        for i in range(self.model().rowCount()):
            self.model().item(i).setCheckState(Qt.CheckState.Checked)
        self.lineEdit().setText(self.checkedItemsStr())
        self.lineEdit().setCursorPosition(0)

    def select_items(self, items):
        for i in range(self.model().rowCount()):
            item = self.model().item(i, 0)
            if item.text() in items:
                item.setCheckState(Qt.CheckState.Checked)
            else:
                item.setCheckState(Qt.CheckState.Unchecked)
        self.lineEdit().clear()
        self.lineEdit().setText(self.checkedItemsStr())
        self.lineEdit().setCursorPosition(0)


class HoverComboBox(QComboBox):
    def __init__(self, parent=None, ObjectName=None, tooltip_dict=None, show_time=1500):
        super().__init__(parent)
        if tooltip_dict is None:
            tooltip_dict = {}
        self.setObjectName(ObjectName)
        self._last_index = -1
        self.tooltip_dict = tooltip_dict
        self.show_time = show_time

    def showPopup(self):
        super().showPopup()
        self.view().viewport().installEventFilter(self)  # 直接获取视图的viewport

    def eventFilter(self, obj, event):
        if event.type() == QEvent.MouseMove:
            # 获取鼠标在viewport中的相对坐标
            pos = event.position().toPoint()
            index = self.view().indexAt(pos)
            if index.isValid():
                self.showToolTip(index, pos)  # 传递鼠标位置
            else:
                self.hideToolTip()
        elif event.type() == QEvent.Leave:
            self._last_index = -1
            self.hideToolTip()
        return super().eventFilter(obj, event)

    def showToolTip(self, index: QModelIndex, pos: QPoint):
        if index.row() == self._last_index:
            return
        item_text = self.itemText(index.row())
        tooltip = "暂无说明" if self.tooltip_dict.get(item_text) is None else self.tooltip_dict.get(item_text)
        if tooltip:
            # 将viewport的相对坐标转换为全局坐标
            global_pos = self.view().viewport().mapToGlobal(pos)
            try:
                self.tooltip.deleteLater()
            except:
                pass
            self.tooltip = CustomTooltip(show_time=self.show_time)
            self.tooltip.show_tooltip(global_pos, tooltip)
            self._last_index = index.row()

    def hideToolTip(self):
        self.tooltip.fade_out()

    def setTootipDict(self, tooltip_dict):
        self.tooltip_dict = tooltip_dict


class RoundButton(QPushButton):
    def __init__(self, hover_text, default_text, countdown, parent=None, ObjectName=None):
        super().__init__(parent)
        self.setFixedSize(100, 100)  # 设置按钮的固定大小
        self.max_time = countdown  # 倒计时最大时间（秒）
        self.current_time = self.max_time  # 当前倒计时
        self.timer = QTimer(self)
        self.setObjectName(ObjectName)
        self.timer.timeout.connect(self.update_time)
        self.timer.start(1000)  # 每秒更新时间
        self.is_mouse_enter = False
        # 设置按钮的阴影效果
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(10)  # 设置阴影模糊半径
        shadow.setOffset(5, 5)  # 设置阴影的偏移
        shadow.setColor(QColor(0, 0, 0, 100))  # 设置阴影的颜色和透明度
        self.setGraphicsEffect(shadow)
        self.default_text = default_text  # 默认文本
        self.text = f'{self.current_time}s{self.default_text}'
        self.hover_text = hover_text  # 鼠标进入时显示的文本

    def update_time(self):
        if self.current_time > 0:
            self.current_time -= 1  # 每秒倒计时减少1
            self.text = f'{self.current_time}s{self.default_text}'
        else:
            self.timer.stop()  # 停止计时器
            self.clicked.emit()  # 触发点击事件
        self.update()  # 更新按钮绘制

    def enterEvent(self, event):
        if not self.isEnabled():
            return
        # 鼠标进入按钮时，切换显示文本
        self.is_mouse_enter = True
        self.setText(self.hover_text)
        super().enterEvent(event)  # 调用父类的 enterEvent

    def leaveEvent(self, event):
        if not self.isEnabled():
            return
        # 鼠标离开按钮时，恢复原始文本
        self.is_mouse_enter = False
        self.setText(self.default_text)
        super().leaveEvent(event)  # 调用父类的 leaveEvent

    def paintEvent(self, event, text=None):
        # 创建QPainter对象
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)  # 启用抗锯齿，使按钮更平滑
        # 获取按钮的矩形区域
        rect = self.rect()
        # 绘制外圆（背景）
        painter.setPen(Qt.NoPen)  # 设置无边框
        if not self.is_mouse_enter:
            painter.setBrush(QColor(243, 243, 243))  # 设置圆形按钮的背景颜色
            painter.drawEllipse(rect)
            painter.setPen(Qt.black)
            painter.drawText(rect, Qt.AlignCenter, self.text)
        else:
            painter.setBrush(QColor('lightblue'))
            painter.drawEllipse(rect)
            painter.setPen(Qt.black)
            painter.drawText(rect, Qt.AlignCenter, self.hover_text)
        # 绘制倒计时外圈（圆环）
        if self.text != '':
            pen = QPen(QColor(0, 0, 255), 5)  # 设置蓝色的边框
            painter.setPen(pen)
            # 计算倒计时百分比
            percentage = self.current_time / self.max_time
            start_angle = 90 * 16  # 起始角度，90度
            span_angle = -360 * 16 * percentage  # 根据倒计时百分比调整圆环的弧度
            # 绘制外圈圆环
            painter.drawArc(rect.adjusted(5, 5, -5, -5), start_angle, span_angle)
        # 绘制按钮文本
        painter.end()
        self.lower()
