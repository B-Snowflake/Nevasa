#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2025/2/13

import os.path
import socket
import threading
from pynput.mouse import Listener, Controller
from gui import main_gui
from PySide6.QtGui import QIcon, QFont, QAction, QPixmap
from PySide6.QtWidgets import QMainWindow, QApplication, QSystemTrayIcon, QMessageBox, QMenu
from PySide6.QtCore import QSize, Qt, QEvent, QPoint, Signal


class MainWindow(QMainWindow, main_gui.Nevasa):

    cursor_signal = Signal(str)

    def __init__(self, port):
        super().__init__()
        self.setupui(self, port)
        self.is_app_start = True
        self.setMinimumSize(QSize(800, 600))
        self.resize(1300, 700)
        if not os.path.exists(self.task_path):
            os.makedirs(self.task_path)
        if not os.path.exists(self.setting_path):
            os.makedirs(self.setting_path)
        if not os.path.exists(self.dataset_path):
            os.makedirs(self.dataset_path)
        self.icon = QIcon()
        self.icon.addPixmap(QPixmap("resources/icon/Nevasa.ico").scaled(64, 64), QIcon.Mode.Normal, QIcon.State.Off)
        self.setWindowIcon(self.icon)
        self.setWindowTitle(self.app_name)
        self.center()
        font = QFont()
        font.setPointSize(10)
        menu_action_show = QAction("显示主界面", self)
        menu_action_taskmanager = QAction("任务管理", self)
        menu_action_setting = QAction("软件设置", self)
        menu_action_quit = QAction("退出", self)
        menu_action_show.setFont(font)
        menu_action_taskmanager.setFont(font)
        menu_action_setting.setFont(font)
        menu_action_quit.setFont(font)
        menu_action_show.triggered.connect(self.show_main_gui)
        menu_action_taskmanager.triggered.connect(self.show_tasks)
        menu_action_setting.triggered.connect(self.show_settings)
        menu_action_quit.triggered.connect(self.quit)
        self.tray_icon_menu = QMenu()
        self.tray_icon_menu.addAction(menu_action_show)
        self.tray_icon_menu.addAction(menu_action_taskmanager)
        self.tray_icon_menu.addAction(menu_action_setting)
        self.tray_icon_menu.addAction(menu_action_quit)
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(QIcon(self.icon))
        self.tray_icon.setContextMenu(self.tray_icon_menu)
        self.tray_icon.activated.connect(self.tray_icon_clicked)
        self.tray_icon.show()
        self.view_toolbutton.click()
        self.data_source_lv1_combobox.setCurrentIndex(0)
        self.data_source_lv2_combobox.setCurrentIndex(0)
        if self.settings['transparent_window']:
            self.edge_size = 8
            self.drag_edge = None
            self.dragging = None
            self.resize_dragging = None
            self.mouse = Controller()
            self.location = None
            self.cursor_signal.connect(self.set_resize_cursor)
            listener_mouse_move_thread = threading.Thread(target=self.mouse_listening, daemon=True)
            listener_mouse_move_thread.start()

    def center(self):
        screen = QApplication.primaryScreen().geometry()
        x = (screen.width() - self.width()) // 2
        y = (screen.height() - self.height()) // 2
        self.move(x, y)

    def show_main_gui(self, click=None):
        if click:
            click.click()
        else:
            self.view_toolbutton.click()
        self.show()
        self.activateWindow()
        self.raise_()

    def show_settings(self):
        self.show_main_gui(self.setting_toolbutton)

    def show_tasks(self):
        self.show_main_gui(self.task_toolbutton)

    @staticmethod
    def is_process_running(port):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(2)
        # 连接到服务器
        try:
            client_socket.connect(('127.0.0.1', port))
            # 发送消息给服务器
            client_socket.send('activate window'.encode())
            response = client_socket.recv(1024)  # 接收最多 1024 字节的数据
            if response == b'copy that':
                return True
            else:
                return False
        except Exception as e:
            return False
        finally:
            client_socket.close()

    def set_resize_cursor(self, location):
        self.location = location
        self.setCursor(self.get_cursor_shape(location))
        if (self.resize_dragging or self.dragging) and self.settings['transparent_window']:
            self.resize_transparent_window()

    def resize_transparent_window(self):
        topleft_x = self.mapToGlobal(QPoint(0, 0)).x()
        topleft_y = self.mapToGlobal(QPoint(0, 0)).y()
        botright_x = self.mapToGlobal(QPoint(self.geometry().width(), self.geometry().height())).x()
        botright_y = self.mapToGlobal(QPoint(self.geometry().width(), self.geometry().height())).y()
        event_x, event_y = self.mouse.position
        if self.location == 'topleft' and self.resize_dragging:
            self.setGeometry(min(event_x, botright_x - self.minimumSize().width()), min(event_y, botright_y - self.minimumSize().height()),
                             max(botright_x - event_x, self.minimumSize().width()), max(botright_y - event_y, self.minimumSize().height()))
        elif self.location == 'topright' and self.resize_dragging:
            self.setGeometry(topleft_x, min(event_y, botright_y - self.minimumSize().height()),
                             max(event_x - topleft_x, self.minimumSize().width()), max(botright_y - event_y, self.minimumSize().height()))
        elif self.location == 'botright' and self.resize_dragging:
            self.setGeometry(topleft_x, topleft_y,
                             max(abs(event_x - topleft_x), self.minimumSize().width()), max(abs(event_y - topleft_y), self.minimumSize().height()))
        elif self.location == 'botleft' and self.resize_dragging:
            self.setGeometry(min(event_x, botright_x - self.minimumSize().width()), topleft_y,
                             max(abs(botright_x - event_x), self.minimumSize().width()), max(abs(event_y - topleft_y), self.minimumSize().height()))
        elif self.location == 'left' and self.resize_dragging:
            self.setGeometry(min(event_x, botright_x - self.minimumSize().width()), topleft_y,
                             max(abs(botright_x - event_x), self.minimumSize().width()), self.size().height())
        elif self.location == 'top' and self.resize_dragging:
            self.setGeometry(topleft_x, min(event_y, (botright_y - self.minimumSize().height())),
                             self.size().width(), max(botright_y - event_y, self.minimumSize().height()))
        elif self.location == 'right' and self.resize_dragging:
            self.setGeometry(topleft_x, topleft_y,
                             max(abs(event_x - topleft_x), self.minimumSize().width()), self.size().height())
        elif self.location == 'bot' and self.resize_dragging:
            self.setGeometry(topleft_x, topleft_y,
                             self.size().width(), max(abs(event_y - topleft_y), self.minimumSize().height()))
        elif self.location == 'normal' and self.dragging:
            self.move(event_x - self.mouse_start_pos.x(), event_y - self.mouse_start_pos.y())

    def mousePressEvent(self, event: QEvent):
        if self.settings['transparent_window'] and event.button() == Qt.MouseButton.LeftButton and self.location == 'normal':
            self.mouse_start_pos = event.globalPosition().toPoint() - self.pos()
            self.dragging = True
        elif self.settings['transparent_window'] and event.button() == Qt.MouseButton.LeftButton and self.location != 'normal':
            self.resize_dragging = True

    # def mouseMoveEvent(self, event: QEvent):
    #     if self.settings['transparent_window'] and self.dragging:
    #         self.move(event.globalPosition().toPoint() - self.mouse_start_pos)

    # def mouseReleaseEvent(self, event: QEvent):
    #     if self.settings['transparent_window'] and event.button() == Qt.MouseButton.LeftButton:
    #         self.dragging = False
    #         self.resize_dragging = False
    #         self.setCursor(Qt.CursorShape.ArrowCursor)

    def mouse_listening(self):
        with Listener(on_move=self.on_mouse_move, on_click=self.on_mouse_click) as listener:
            listener.join()

    def on_mouse_click(self, x, y, button, pressed):
        if button == button.left and not pressed:
            self.dragging = False
            self.resize_dragging = False

    def on_mouse_move(self, x, y):
        try:
            if self.isActiveWindow():
                topleft_x = self.mapToGlobal(QPoint(0, 0)).x()
                topleft_y = self.mapToGlobal(QPoint(0, 0)).y()
                botright_x = self.mapToGlobal(QPoint(self.geometry().width(), self.geometry().height())).x()
                botright_y = self.mapToGlobal(QPoint(self.geometry().width(), self.geometry().height())).y()
                if topleft_x - self.edge_size < x < topleft_x + self.edge_size and topleft_y - self.edge_size < y < topleft_y + self.edge_size:
                    self.cursor_signal.emit('topleft')
                elif botright_x - self.edge_size < x < botright_x + self.edge_size and topleft_y - self.edge_size < y < topleft_y + self.edge_size:
                    self.cursor_signal.emit('topright')
                elif botright_x - self.edge_size < x < botright_x + self.edge_size and botright_y - self.edge_size < y < botright_y + self.edge_size:
                    self.cursor_signal.emit('botright')
                elif topleft_x - self.edge_size < x < topleft_x + self.edge_size and botright_y - self.edge_size < y < botright_y + self.edge_size:
                    self.cursor_signal.emit('botleft')
                elif topleft_x - self.edge_size < x < topleft_x + self.edge_size and topleft_y + self.edge_size < y < botright_y - self.edge_size:
                    self.cursor_signal.emit('left')
                elif topleft_x + self.edge_size < x < botright_x - self.edge_size and topleft_y - self.edge_size < y < topleft_y + self.edge_size:
                    self.cursor_signal.emit('top')
                elif botright_x - self.edge_size < x < botright_x + self.edge_size and topleft_y + self.edge_size < y < botright_y - self.edge_size:
                    self.cursor_signal.emit('right')
                elif topleft_x + self.edge_size < x < botright_x - self.edge_size and botright_y - self.edge_size < y < botright_y + self.edge_size:
                    self.cursor_signal.emit('bot')
                elif topleft_x + self.edge_size < x < botright_x - self.edge_size and topleft_y + self.edge_size < y < botright_y - self.edge_size:
                    self.cursor_signal.emit('normal')
                else:
                    self.cursor_signal.emit('out of window')
        except:
            pass

    @staticmethod
    def get_cursor_shape(edge):
        """根据边缘方向返回对应光标形状"""
        if not edge:
            return None
        return {
            "left": Qt.CursorShape.SizeHorCursor,
            "right": Qt.CursorShape.SizeHorCursor,
            "top": Qt.CursorShape.SizeVerCursor,
            "bot": Qt.CursorShape.SizeVerCursor,
            "topleft": Qt.CursorShape.SizeFDiagCursor,
            "topright": Qt.CursorShape.SizeBDiagCursor,
            "botleft": Qt.CursorShape.SizeBDiagCursor,
            "botright": Qt.CursorShape.SizeFDiagCursor
        }.get(edge, Qt.CursorShape.ArrowCursor)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self.is_app_start:
            if self.settings['transparent_window']:
                self.webtoolbuttonwidget.move(1130, 50)
                self.creategeemapwidget.setGeometry(0, 20, 270, 660)
            else:
                self.webtoolbuttonwidget.move(1150, 50)
                self.creategeemapwidget.setGeometry(0, 0, 270, 680)
            self.draw_polygon_tips_label.move(500, 0)
            self.customregion_downloadwidget.move(420, 80)
            self.newtaskwidget.setGeometry(400, 300, 650, 60)
            self.is_app_start = False
        else:
            webwidget_width = self.webwidget.size().width()
            webwidget_height = self.webwidget.size().height()
            centralwidget_width = self.central_widget.size().width()
            centralwidget_height = self.central_widget.size().height()
            if self.settings['transparent_window']:
                self.webtoolbuttonwidget.move(webwidget_width - 85, 50)
                self.creategeemapwidget.setGeometry(0, 20, 270, webwidget_height - 40)
            else:
                self.webtoolbuttonwidget.move(webwidget_width - 65, 50)
                self.creategeemapwidget.setGeometry(0, 0, 270, webwidget_height - 20)
            self.draw_polygon_tips_label.move((centralwidget_width - 300)/2, 0)
            self.customregion_downloadwidget.move((centralwidget_width - 460)/2, (centralwidget_height - 540)/2)
            self.newtaskwidget.setGeometry((centralwidget_width - 500)/2, (centralwidget_height - 100)/2, centralwidget_width - 650, 60)

    def quit(self, event=None):
        if self.settings['when_close_application'] == 'quit_immediately':
            self.close_all_when_exit()
        elif self.settings['when_close_application'] == 'remind_me':
            try:
                display_tasks = self.exists_task[self.exists_task['is_executing']]
                if not display_tasks.empty:
                    msg_box = QMessageBox(self)
                    self.set_messagebox_stylesheet(msg_box)
                    msg_box.setWindowTitle("提示")
                    msg_box.setText("有任务正在执行，确认退出？")
                    msg_box.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                    ok_button = msg_box.button(QMessageBox.StandardButton.Yes)
                    cancel_button = msg_box.button(QMessageBox.StandardButton.No)
                    ok_button.setText("确认")
                    cancel_button.setText("取消")
                    result = msg_box.exec()
                    if result == QMessageBox.StandardButton.Yes:
                        self.close_all_when_exit()
                    else:
                        event.ignore()
                else:
                    self.close_all_when_exit()
            except Exception as e:
                self.close_all_when_exit()

    def close_all_when_exit(self):
        try:
            os.remove(self.temp_html_file)
            os.remove(self.temp_pdf_file)
        except:
            pass
        self.hide()
        self.tray_icon.hide()
        try:
            self.subprocess.closeall()
        except:
            pass
        QApplication.instance().quit()

    def tray_icon_clicked(self, reason):
        try:
            if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
                self.show()
        except:
            pass

    def closeEvent(self, event):
        if self.settings['when_click_closebutton'] == 'quit':
            self.quit(event=event)
        elif self.settings['when_click_closebutton'] == 'minimize':
            self.hide()
            # notification.notify(message=f'已最小化到任务栏托盘', timeout=5)
            event.ignore()
