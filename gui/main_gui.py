#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2025/2/13

import os
import signal
import socket
import threading
import traceback
import ee
import platform
import random
import shutil
import sqlite3
import psycopg2
import subprocess
import tempfile
import uuid
import webbrowser
import geojson
import geopandas
import numpy
import requests
import xmltodict
import keyring
import time
import pandas as pd
import coordinate
import pycuda.driver as cudadrv
from pathlib import Path
from bs4 import BeautifulSoup
from plyer import notification
from pypinyin import lazy_pinyin
from shapely import make_valid, GeometryCollection, unary_union, MultiPolygon, Polygon
from datetime import datetime, timedelta
from PySide6.QtWebChannel import QWebChannel
from geodata import geodata_view
from gui import customwidget
from license import license
from shapely.wkt import loads, dumps
from BlurWindow.blurWindow import GlobalBlur
from map_engine import map_engine
from multiprocess_manager import auth_proxy_testing as ts, multiprocess_manager
from PySide6.QtNetwork import QNetworkProxy
from PySide6.QtWebEngineCore import QWebEngineSettings
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtCore import QSize, Qt, Slot, QUrl, QTimer, QMetaObject, QRegularExpression, Signal
from PySide6.QtGui import QIcon, QPixmap, QPainter, QTransform, QMovie, QRegularExpressionValidator, QColor, QIntValidator
from PySide6.QtWidgets import QPushButton, QComboBox, QCalendarWidget
from PySide6.QtWidgets import QTextEdit, QApplication, QRadioButton, QButtonGroup, QScrollArea, QAbstractButton, QFileDialog, QDialogButtonBox
from PySide6.QtWidgets import QGridLayout, QMessageBox, QToolButton, QWidget, QHBoxLayout, QStackedWidget, QVBoxLayout, QLabel, QFormLayout, QLineEdit


class Nevasa:
    ee_initialization_signal = Signal(str)
    export_html_signal = Signal(str)
    datasource_connect_signal = Signal(str)
    active_window_signal = Signal(str)

    def __init__(self):
        self.os_system = platform.system()
        self.app_name = 'Nevasa_gee'
        if self.os_system == 'Windows':
            self.path = Path(os.environ.get('LOCALAPPDATA')) / self.app_name
        elif self.os_system == 'Darwin':  # macOS
            self.path = Path(os.path.expanduser('~/Library/Application Support')) / self.app_name
        elif self.os_system == 'Linux':  # Linux
            self.path = Path(os.path.expanduser('~/.local/share')) / self.app_name
        self.settings = {
            'max_display_task': 10,  # 任务列表最大显示数量（以创建时间为序，取最近N个）
            'max_display_dataset': 10,  # 数据集最大显示数量（以创建时间为序，取最近N个）
            'max_download_try': 3,  # 最大下载尝试次数
            'max_proxy_server': 1,  # 最大代理服务器数量
            'max_download_fail': 0,  # 允许的最大切片下载失败数量
            'enable_gpu': False,  # 使用GPU进行图像处理（可通过GUI设置）
            'project_id': None,  # 谷歌Projrct_id（可通过GUI设置）
            'json_file': None,  # 谷歌JSON-KEY路径（可通过GUI设置）
            'service_account': None,  # 谷歌服务账户（可通过GUI设置）
            'select_task': 'all_tasks',  # 任务列表筛选设置（可通过GUI设置）
            'when_close_application': 'remind_me',  # 退出软件时，执行动作（可通过GUI设置）
            'when_click_closebutton': 'quit',  # 点击关闭按钮时，执行动作（可通过GUI设置）
            'last_save_path': '',  # 最后一次任务保存路径（可通过GUI设置）
            'proxies': {},  # 已保存的代理（可通过GUI设置）
            'license_path': '',  # 许可文件名
            'map_opacity': 0.7,  # 3d地图渲染图层透明度
            'min_zoom_3d': 14,  # 3d地图建筑物最小显示层级
            'default_map_visualize': '2d',  # 默认地图渲染形式（可通过GUI设置）
            'display_proxies': {},  # 显示到GUI界面的代理（可通过GUI设置）
            'proxies_username': {},  # 代理用户名（可通过GUI设置）
            'transparent_window': False,  # 主界面窗口风格（可通过GUI设置）
            'loading_html_timeout_ms': 25000,  # 数据集加载超时时间
            'license_server_url': 'http://1.12.228.148:42689'  # 许可服务器地址
        }
        self.map, self.test = map_engine.GeeMapHtml(), ts.AuthAndKeyTest()
        self.task_download_times, self.dataset_info = {}, {}
        self.exists_task, self.exists_dataset = pd.DataFrame(), pd.DataFrame()
        self.task_path, self.setting_path, = self.path / "downloadtask", self.path / "setting"
        self.dataset_path, self.license_path = self.path / "dataset", self.path / "license"
        self.log_path, self.numbacache_path = self.path / "log.txt", self.path / "numba_cache"
        self.get_all_settings()
        self.get_all_tasks()
        self.get_all_datasets()
        self.color = 'white' if self.settings['transparent_window'] else 'black'
        self.connection = sqlite3.connect(database='resources/data/data.nev', check_same_thread=False)
        self.execute_task_timer, self.polygon_from_js, self.test_result_dict = {}, {}, {}
        self.subprocess, self.transform, self.export_html_exception, self.webengine_loaded_geometry = None, None, None, None
        self.is_eeinitialization_done, self.is_reinitialization_ee, self.no_proxy_notifaction = False, False, False
        self.end_listening_js, self.dataset_html_loaded, self.base_map_change_attention = False, False, False
        self.current_loading_dataset, self.current_loading_visualize, self.current_loading_datasource = None, None, None
        self.temp_pdf_file, self.temp_html_file = None, None
        self.license_check_result, self.license_enddate = None, None
        self.load_html_timeout_timer, self.picked_region_draw_timer = QTimer(), QTimer()
        self.load_html_timeout_timer.timeout.connect(self.loading_html_timeout)
        self.picked_region_draw_timer.timeout.connect(self.listener_for_js)
        self.request_headers = {'Accept-Language': 'zh-CN',
                                'sec-ch-ua': '"Chromium";v="136", "Not.A/Brand";v="99", "Google Chrome";v="136"',
                                'sec-ch-ua-mobile': '?0',
                                'upgrade-insecure-requests': '1',
                                'sec-ch-ua-platform': '"Windows"',
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'}
        self.set_proxy(True)

    def setupui(self, mainwindow, port):
        self.mainwindow = mainwindow
        self.central_widget = QWidget(parent=self.mainwindow)
        self.mainwindow.setCentralWidget(self.central_widget)
        self.main_layout = QHBoxLayout()
        self.main_layout.setSpacing(0)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.central_widget.setLayout(self.main_layout)
        self.stackwidget = QStackedWidget(parent=self.central_widget)
        self.tab_widget()
        self.web_widget()
        self.newtask_widget()
        self.custom_region_download_widget()
        self.task_widget()
        self.setting_widget()
        self.stackwidget.addWidget(self.webwidget)
        self.stackwidget.addWidget(self.taskwidget)
        self.stackwidget.addWidget(self.settingwidget)
        self.main_layout.addWidget(self.tabwidget)
        self.main_layout.addWidget(self.stackwidget)
        self.ee_initialization_signal.connect(self.ee_initialization_result)
        self.export_html_signal.connect(self.load_dataset_html)
        self.datasource_connect_signal.connect(self.customregion_connect_test_result)
        self.active_window_signal.connect(self.mainwindow.show_main_gui)
        initialization_ee_thread = threading.Thread(target=self.initialization_ee, daemon=True)
        initialization_ee_thread.start()
        initialization_license_check_thread = threading.Thread(target=self.license_check, daemon=True)
        initialization_license_check_thread.start()
        initialization_spatialdata_thread = threading.Thread(target=self.initialization_spatialdata, daemon=True)
        initialization_spatialdata_thread.start()
        initialization_multiprocessmanager_thread = threading.Thread(target=self.initialization_multiprocessmanager, daemon=True)
        initialization_multiprocessmanager_thread.start()
        initialization_socketserver_thread = threading.Thread(target=self.initialization_socketserver, args=(port,), daemon=True)
        initialization_socketserver_thread.start()
        initialization_cuda_thread = threading.Thread(target=self.check_cuda, daemon=True)
        initialization_cuda_thread.start()
        if self.settings['transparent_window']:
            self.mainwindow.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
            self.mainwindow.setWindowFlag(Qt.WindowType.FramelessWindowHint)
            GlobalBlur(self.mainwindow.winId(), Dark=True, Acrylic=True, QWidget=mainwindow)
        else:
            self.central_widget.setStyleSheet("background-color: rgb(243, 243, 243);")
        QMetaObject.connectSlotsByName(self.mainwindow)

    def check_cuda(self):
        is_cuda_ready = False
        try:
            cudadrv.init()
            num_devices = cudadrv.Device.count()
            if num_devices > 0:
                self.enable_gpu_data_process_radio1.setEnabled(True)
                self.enable_gpu_data_process_radio2.setEnabled(True)
                is_cuda_ready = True
            else:
                is_cuda_ready = False
                self.settings['enable_gpu'] = False
        except:
            is_cuda_ready = False
            self.settings['enable_gpu'] = False
        finally:
            print('CUDA device ready' if is_cuda_ready else 'no available CUDA device')

    def initialization_multiprocessmanager(self):
        try:
            self.subprocess = multiprocess_manager.MultiprocessManager(self.settings['max_download_fail'], self.task_path)
            print('multiprocessmanager done')
        except Exception as e:
            print('multiprocessmanage exception', e)

    def initialization_socketserver(self, port):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # 设置服务器地址和端口
            self.server_socket.bind(('127.0.0.1', port))
            # 开始监听客户端连接，最多允许 5 个客户端连接
            self.server_socket.listen(5)
            self.server_socket.settimeout(1)
            print('socketserver done')
            while True:
                # 等待客户端连接
                try:
                    client_socket, client_address = self.server_socket.accept()
                    while True:
                        data = client_socket.recv(1024)
                        if not data:
                            break
                        if data == b'activate window':
                            self.active_window_signal.emit('activate window')
                            client_socket.send(b'copy that')
                    client_socket.close()
                except Exception as e:
                    continue
                finally:
                    time.sleep(0.1)
        except Exception as e:
            print('socketserver exception', e)

    def initialization_spatialdata(self):
        try:
            # 初始化坐标系转换器
            self.transform = coordinate.CoordTransform()
            # 初始化空间查询数据表
            self.geo_sn = pd.read_sql('select * from st_r_sn order by adcode', self.connection)
            self.geo_sh = pd.read_sql('select * from st_r_sh order by adcode', self.connection)
            self.geo_xn = pd.read_sql('select * from st_r_xn order by adcode', self.connection)
            self.geo_special_region = pd.read_sql("select * from st_r_sh where adcode not like '%00'", self.connection)
            self.geo_sn.loc[:, 'geom'] = self.geo_sn['geometry'].apply(loads)
            self.geo_sn.loc[:, 'geom'] = self.geo_sn['geom'].apply(lambda geom: make_valid(geom))
            self.geo_sh.loc[:, 'geom'] = self.geo_sh['geometry'].apply(loads)
            self.geo_sh.loc[:, 'geom'] = self.geo_sh['geom'].apply(lambda geom: make_valid(geom))
            self.geo_xn.loc[:, 'geom'] = self.geo_xn['geometry'].apply(loads)
            self.geo_xn.loc[:, 'geom'] = self.geo_xn['geom'].apply(lambda geom: make_valid(geom))
            self.geo_special_region.loc[:, 'geom'] = self.geo_special_region['geometry'].apply(loads)
            self.geo_special_region.loc[:, 'geom'] = self.geo_special_region['geom'].apply(lambda geom: make_valid(geom))
            print('spatialdata done')
        except Exception as e:
            print('spatialdata exception', e)

    def initialization_ee(self):
        try:
            if self.settings['project_id'] is not None and self.settings['json_file'] is not None and self.settings['service_account'] is not None:
                credentials = ee.ServiceAccountCredentials(self.settings['service_account'], self.settings['json_file'])
                ee.Initialize(credentials=credentials, project=self.settings['project_id'])
                self.is_eeinitialization_done = True
                print('eeinitialization done')
                self.ee_initialization_signal.emit("completed")
            else:
                self.ee_initialization_signal.emit("no available google auth")
                print('no available google auth')
        except Exception as e:
            print('eeinitialization exception', e)
            self.ee_initialization_signal.emit("fail")

    def license_check(self):
        try:
            self.license = license.License(self.settings['license_server_url'])
            with open(self.license_path / f'{self.settings['license_path']}', 'r') as file:
                xml_str = file.read()
            license_info = xmltodict.parse(xml_str)
            license_chain_a = license_info['root']['license']
            license_chain_b = keyring.get_password(service_name='Nevasa', username='Nevasa_Master_password')
            if self.license.check_is_dna(chain_a=license_chain_a, chain_b=license_chain_b):
                try:
                    self.license_check_result, date = self.license.check_license(nee_license=license_chain_a)
                    self.license_enddate = date[:4] + '-' + date[4:6] + '-' + date[6:]
                except:
                    print('license check fail')
                    print(traceback.print_exc())
                    self.license_check_result, self.license_enddate = False, None
                if self.license_check_result:
                    self.license_file_lineedit.setText(f"{self.settings['license_path']}  (有效期至{self.license_enddate})")
                else:
                    self.license_file_lineedit.setText(f"{self.settings['license_path']}  (验证失败)")
                self.license_file_lineedit.setCursorPosition(0)
            else:
                print('license DNA check fail')
                self.license_check_result, self.license_enddate = False, None
                self.license_file_lineedit.setText(f"{self.settings['license_path']}  (验证失败)")
                self.license_file_lineedit.setCursorPosition(0)
            print('license check', "success" if self.license_check_result else "fail", self.license_enddate)
        except Exception as e:
            print('license exception', e)
            return e

    def ee_initialization_result(self, message):
        if message == 'fail':
            self.view_toolbutton.setEnabled(False)
            self.download_toolbutton.setEnabled(False)
            self.task_toolbutton.setEnabled(False)
            self.setting_toolbutton.click()
            self.show_msg_box('地图组件初始化失败，请检查你的网络连接、VPN和谷歌认证设置无误后<br>再次初始化组件')
            self.replace_widget(self.applicationsettingwidget, self.reinitialization_ee_widget)
            self.application_label.hide()
        elif message == 'no available google auth':
            self.download_toolbutton.setEnabled(False)
            self.task_toolbutton.setEnabled(False)
            self.google_authenticate_guidance_label.show()
            self.setting_toolbutton.click()
            self.show_msg_box('请在设置中添加谷歌认证信息')
        else:
            self.view_toolbutton.setEnabled(True)
            self.download_toolbutton.setEnabled(True)
            self.task_toolbutton.setEnabled(True)
            self.google_authenticate_guidance_label.hide()
            if self.is_reinitialization_ee:
                self.view_toolbutton.click()
                self.webEngineView.reload()
                self.replace_widget(self.reinitializationeewidget, self.application_setting_widget)
                self.application_label.show()
                self.show_msg_box('地图组件初始化成功')

    def load_dataset_html(self, datasetname):
        print('export_finished', self.dataset_name_combobox.lineEdit().text())
        self.dataset_name_combobox.blockSignals(True)
        self.dataset_name_combobox_update_items()
        if datasetname == '1fail' and self.export_html_exception is not None:
            e = self.export_html_exception
            print('Generate dataset fail', e)
            self.export_html_exception = None
            self.show_msg_box('选定日期或区域内无可用数据，请尝试加大起止时间间隔或调整选定区域')
            self.restore_widgets_when_load_fail()
            try:
                self.load_html_timeout_timer.stop()
            except:
                pass
        else:
            self.dataset_name_combobox.lineEdit().setText(datasetname)
            if self.settings['default_map_visualize'] == '3d' and self.dataset_info[self.data_source_lv3_combobox.currentText()]['has_3d']:
                self.webEngineView.load(QUrl.fromLocalFile(self.dataset_path / f'{datasetname}_3d.html'))
            else:
                self.webEngineView.load(QUrl.fromLocalFile(self.dataset_path / f'{datasetname}_2d.html'))
            print('load_start', self.dataset_name_combobox.lineEdit().text())
        self.dataset_name_combobox.blockSignals(False)

    def newtask_updateprogress(self, taskname):
        self.progress_value += 1  # 每次增加 1
        self.newtask_progressbar.setValue(self.progress_value)
        self.newtask_progressbar_label.setText(f'{self.progress_value}%')
        if self.progress_value >= 100:
            self.newtask_timer.deleteLater()
            self.newtaskwidget.setVisible(False)  # 关闭窗口
            if self.license_check_result:
                self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'this_start_time'] = time.time()
                self.download_dataset_button.setEnabled(True)
                self.task_toolbutton.click()
                self.all_tasks_radio.click()
                self.dataset_name_combobox.update_placeholdertext()
            else:
                self.download_toolbutton.click()
                self.task_toolbutton.setEnabled(False)

    def listener_for_js(self):
        if self.end_listening_js:
            self.picked_region_draw_timer.stop()
        else:
            try:
                polygon = self.polygon_from_js['polygon']
                input_polygon = loads(polygon)
                intersecting_geom_sn = self.geo_sn[self.geo_sn['geom'].apply(lambda geom: geom.intersects(input_polygon))].loc[:, ['adcode', 'geom']]
                sh_result = intersecting_geom_sn.merge(self.geo_sh, left_on='adcode', right_on='parent', how='inner')
                intersecting_geom_sh = sh_result[sh_result['geom_y'].apply(lambda geom: geom.intersects(input_polygon))].loc[:, ['adcode_y', 'geom_y']]
                xn_result = intersecting_geom_sh.merge(self.geo_xn, left_on='adcode_y', right_on='parent', how='inner')
                intersecting_geom_xn = xn_result[xn_result['geom'].apply(lambda geom: geom.intersects(input_polygon))].loc[:, ['adcode', 'geom', 'geometry']]
                intersecting_special_region = self.geo_special_region[self.geo_special_region['geom'].apply(lambda geom: geom.intersects(input_polygon))].loc[:,
                                              ['adcode', 'geom', 'geometry']]
                final_intersecting_region = pd.concat([intersecting_geom_xn, intersecting_special_region]).drop_duplicates().reset_index(drop=True)
                merged_geometry = unary_union(final_intersecting_region['geom'])
                # 使用聚合后的边界来下载数据
                self.picked_region_draw = dumps(merged_geometry)
                self.webEngineView.page().runJavaScript('removeOverlay()')
                self.display_polygon_picked_region_draw = MultiPolygon()
                try:
                    # 使用融合后的边界，显示到地图
                    # 处理显示边界，遍历每个区县，对于区县之间采用聚合，对于区县内部，采用融合
                    self.adcode_picked_region_draw = []
                    for idx, row in final_intersecting_region.iterrows():
                        geometry, adcode = row['geometry'], row['adcode']
                        geometry = loads(geometry)
                        self.adcode_picked_region_draw.append(adcode)
                        if isinstance(geometry, Polygon):
                            # 融合Polygon类型的区县到结果集
                            self.display_polygon_picked_region_draw = MultiPolygon(list(self.display_polygon_picked_region_draw.geoms) + [geometry])
                        elif isinstance(geometry, MultiPolygon):
                            # 融合MultiPolygon类型的区县到结果集
                            if isinstance(unary_union(geometry.geoms), Polygon):
                                # 先使用unary_union聚合单个区县内部的Polygon类型，然后融合到结果集
                                self.display_polygon_picked_region_draw = MultiPolygon(
                                    list(self.display_polygon_picked_region_draw.geoms) + [unary_union(geometry.geoms)])
                            elif isinstance(unary_union(geometry.geoms), MultiPolygon):
                                # 先使用unary_union聚合单个区县内部的MultiPolygon类型，然后融合到结果集
                                self.display_polygon_picked_region_draw = (
                                    MultiPolygon(list(self.display_polygon_picked_region_draw.geoms) + list(unary_union(geometry.geoms).geoms)))
                except Exception as e:
                    pass
                # 一般情况下，显示融合后的边界
                if not self.display_polygon_picked_region_draw.is_empty:
                    polygon = dumps(self.display_polygon_picked_region_draw)
                # 冗余，如果显示边界计算错误，显示实际下载边界（聚合后的边界）
                elif self.display_polygon_picked_region_draw.is_empty and not merged_geometry.is_empty:
                    polygon = dumps(self.display_polygon_picked_region_draw)
                self.webEngineView_load_geometry(polygon)
                self.end_listening_js = True
                self.picked_region_draw_timer.stop()
            except Exception as e:
                pass

    def restore_widgets_when_load_fail(self):
        self.disable_creategeemapwidget_widgets(False)
        datainfo = self.dataset_info[self.data_source_lv3_combobox.lineEdit().text()]
        self.view_data_button_movie_label.hide()
        self.view_data_movie.stop()
        self.view_data_button.setText('预览数据集')
        self.view_data_button.setIcon(QIcon('resources/icon/view_dataset.png'))
        self.view_data_button.setEnabled(True)
        self.download_dataset_button.setEnabled(False)
        self.delete_dataset_button.setEnabled(False)
        self.new_dataset_button.setEnabled(True)
        self.data_scale_combobox.setEnabled(True)
        self.base_map_combobox.setEnabled(True)
        self.range_type_combobox.setEnabled(True)
        self.savepath_lineEdit.setEnabled(True)
        self.savepath_pushbutton.setEnabled(True)
        self.dataset_name_combobox.setEnabled(True)
        self.data_info_toolbutton.setEnabled(True)
        if len(datainfo['bands']) > 0:
            self.bands_combobox.setEnabled(True)
        else:
            self.bands_combobox.setEnabled(False)
        if datainfo['need_date'] != 'no':
            self.start_date_calendar.setEnabled(True)
            self.start_date_lineEdit.setEnabled(True)
            self.start_date_calendar_button.setEnabled(True)
            self.end_date_calendar.setEnabled(True)
            self.end_date_lineEdit.setEnabled(True)
            self.end_date_calendar_button.setEnabled(True)
        else:
            self.start_date_calendar.setEnabled(False)
            self.start_date_lineEdit.setEnabled(False)
            self.start_date_calendar_button.setEnabled(False)
            self.end_date_calendar.setEnabled(False)
            self.end_date_lineEdit.setEnabled(False)
            self.end_date_calendar_button.setEnabled(False)
        self.data_source_lv1_combobox.setDisabled(False)
        self.data_source_lv2_combobox.setEnabled(True)
        self.data_source_lv3_combobox.setEnabled(True)
        if self.range_type_combobox.currentIndex() == 2:
            self.pick_region_sn_combobox.setEnabled(True)
            self.pick_region_sh_combobox.setEnabled(True)
            if self.pick_region_sh_combobox.currentIndex() != -1:
                self.pick_region_xn_combobox.setEnabled(True)

    def replace_widget(self, old_widget, factory):
        """
        查找父布局并自动替换 Widget
        :param old_widget: 需要替换的旧 Widget
        :param factory: 工厂函数，用于生成新 Widget
        """
        new_widget = factory()
        parent = old_widget.parent()
        if parent:
            # 通过父布局查找
            layout = old_widget.parent().layout()
            if layout:
                index = layout.indexOf(old_widget)
                if index != -1:
                    # 直接替换，避免布局残留
                    layout.removeWidget(old_widget)
                    old_widget.deleteLater()
                    layout.insertWidget(index, new_widget)

    def get_pdf_html_from_db(self, pdf_id=None, html_id=None):
        cur = self.connection.cursor()
        if pdf_id is not None:
            cur.execute("SELECT pdf FROM guidance_files WHERE id=?", (pdf_id,))
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        elif html_id is not None:
            cur.execute("SELECT html FROM javascript_api WHERE id=?", (html_id,))
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.html')
        data = cur.fetchone()[0]
        with open(temp_file.name, 'wb') as f:
            f.write(data)
        print(temp_file.name)
        cur.close()
        return temp_file.name

    def get_cuda_kernel_func(self, name='process_image_kernel'):
        cur = self.connection.cursor()
        kernel_func = cur.execute('select binary from ptx_modules where name=?', (name,)).fetchone()[0]
        cur.close()
        return kernel_func

    def tab_widget(self):
        self.tabwidget = QWidget(parent=self.central_widget)
        self.tabwidget.setMaximumWidth(100)
        self.tabwidget_layout = QVBoxLayout()
        self.tabwidget_layout.setSpacing(30)
        self.tabwidget_layout.setContentsMargins(10, 30, 10, 30)
        self.tabwidget.setLayout(self.tabwidget_layout)
        self.view_toolbutton = QToolButton(parent=self.tabwidget, ObjectName="view_toolbutton")
        self.set_toolbutton_stylesheet(self.view_toolbutton)
        self.view_toolbutton.setText('浏览')
        self.view_toolbutton.setIcon(QIcon("resources/icon/browse.png"))
        self.view_toolbutton.setIconSize(QSize(45, 45))
        self.view_toolbutton.setMinimumSize(65, 65)
        self.tabwidget_layout.addWidget(self.view_toolbutton)
        self.download_toolbutton = QToolButton(parent=self.tabwidget, ObjectName='download_toolbutton')
        self.set_toolbutton_stylesheet(self.download_toolbutton)
        self.download_toolbutton.setText('下载')
        self.download_toolbutton.setIcon(QIcon("resources/icon/download.png"))
        self.download_toolbutton.setIconSize(QSize(45, 45))
        self.download_toolbutton.setMinimumSize(65, 65)
        self.tabwidget_layout.addWidget(self.download_toolbutton)
        self.task_toolbutton = QToolButton(parent=self.tabwidget, ObjectName='task_toolbutton')
        self.set_toolbutton_stylesheet(self.task_toolbutton)
        self.task_toolbutton.setText('任务')
        self.task_toolbutton.setIcon(QIcon("resources/icon/task.png"))
        self.task_toolbutton.setIconSize(QSize(45, 45))
        self.task_toolbutton.setMinimumSize(65, 65)
        self.tabwidget_layout.addWidget(self.task_toolbutton)
        self.setting_toolbutton = QToolButton(parent=self.tabwidget, ObjectName='setting_toolbutton')
        self.set_toolbutton_stylesheet(self.setting_toolbutton)
        self.setting_toolbutton.setText('设置')
        self.setting_toolbutton.setIcon(QIcon("resources/icon/setting.png"))
        self.setting_toolbutton.setIconSize(QSize(45, 45))
        self.setting_toolbutton.setMinimumSize(65, 65)
        self.tabwidget_layout.addWidget(self.setting_toolbutton)
        self.tabwidget_layout.addStretch(1)
        if self.settings['transparent_window']:
            self.exit_toolbutton = QToolButton(parent=self.tabwidget, ObjectName='setting_toolbutton')
            self.exit_toolbutton.clicked.connect(self.mainwindow.close)
            self.set_toolbutton_stylesheet(self.exit_toolbutton)
            self.exit_toolbutton.setText('退出')
            self.exit_toolbutton.setIcon(QIcon("resources/icon/exit.png"))
            self.exit_toolbutton.setIconSize(QSize(45, 45))
            self.exit_toolbutton.setMinimumSize(65, 65)
            self.tabwidget_layout.addWidget(self.exit_toolbutton)
        return self.tabwidget

    def tabwidget_toolbutton_clicked(self, clicked_button):
        for toolbutton in self.tabwidget.findChildren(QToolButton):
            if toolbutton != clicked_button:
                self.set_toolbutton_stylesheet(button=toolbutton)
            else:
                self.set_toolbutton_stylesheet(button=toolbutton, hover=False)

    @Slot()
    def on_view_toolbutton_clicked(self):
        self.tabwidget_toolbutton_clicked(self.sender())
        self.stackwidget.setCurrentIndex(0)
        self.creategeemapwidget.hide()
        self.draw_polygon_tips_label.hide()
        self.customregion_downloadwidget.hide()

    @Slot()
    def on_download_toolbutton_clicked(self):
        self.tabwidget_toolbutton_clicked(self.sender())
        self.stackwidget.setCurrentIndex(0)
        self.creategeemapwidget.show()
        self.customregion_downloadwidget.hide()

    @Slot()
    def on_task_toolbutton_clicked(self):
        self.tabwidget_toolbutton_clicked(self.sender())
        self.stackwidget.setCurrentIndex(1)
        self.draw_polygon_tips_label.hide()
        self.customregion_downloadwidget.hide()

    @Slot()
    def on_setting_toolbutton_clicked(self):
        self.tabwidget_toolbutton_clicked(self.sender())
        self.stackwidget.setCurrentIndex(2)
        self.draw_polygon_tips_label.hide()
        self.customregion_downloadwidget.hide()

    def newtask_widget(self):
        self.newtaskwidget = QWidget(parent=self.central_widget)
        self.newtaskwidget_layout = QVBoxLayout()
        self.newtaskwidget_layout.setContentsMargins(15, 10, 10, 10)
        self.newtaskwidget_layout.setSpacing(0)
        self.newtaskwidget.setLayout(self.newtaskwidget_layout)
        self.newtaskwidget.raise_()
        self.newtaskwidget.setStyleSheet("background-color: rgb(243, 243, 243);")
        self.newtask_label = QLabel(text='正在创建下载任务:', parent=self.newtaskwidget)
        self.set_label_stylesheet(self.newtask_label)
        self.newtaskwidget_progress_widget_layout = QHBoxLayout()
        self.newtaskwidget_progress_widget_layout.setSpacing(5)
        self.newtaskwidget_progress_widget_layout.setContentsMargins(0, 0, 0, 0)
        self.newtask_progressbar = customwidget.CustomProgressBar(parent=self.newtaskwidget)
        self.newtask_progressbar.setStyleSheet("QProgressBar {border: 1px;background-color: lightgray}")
        self.newtask_progressbar.setMaximumHeight(2)
        self.newtask_progressbar.setTextVisible(False)
        self.newtask_progressbar_label = QLabel(self.newtaskwidget)
        self.set_label_stylesheet(self.newtask_progressbar_label)
        self.newtask_progressbar.setRange(0, 100)
        self.newtaskwidget_progress_widget_layout.addWidget(self.newtask_progressbar)
        self.newtaskwidget_progress_widget_layout.addWidget(self.newtask_progressbar_label)
        self.newtaskwidget_layout.addWidget(self.newtask_label)
        self.newtaskwidget_layout.addLayout(self.newtaskwidget_progress_widget_layout)
        self.newtaskwidget.hide()

    def web_widget(self):
        self.webwidget = QWidget(parent=self.stackwidget)
        self.webwidget_layout = QVBoxLayout()
        self.webwidget_layout.setSpacing(0)
        if self.settings['transparent_window']:
            self.webwidget_layout.setContentsMargins(0, 20, 20, 0)
        else:
            self.webwidget_layout.setContentsMargins(0, 0, 0, 0)
        self.webwidget.setLayout(self.webwidget_layout)
        # self.webwidget.setStyleSheet("border: 1px solid red;")
        self.browser_widget = QWidget(parent=self.webwidget)
        self.browser_widget_layout = QVBoxLayout()
        self.browser_widget_layout.setContentsMargins(0, 0, 0, 0)
        self.browser_widget.setLayout(self.browser_widget_layout)
        self.webEngineView = QWebEngineView(parent=self.browser_widget, ObjectName='webEngineView')
        self.browser_widget_layout.addWidget(self.webEngineView)
        self.maplabel_widget = QWidget(parent=self.webwidget)
        self.maplabel_widget.setMaximumHeight(20)
        # self.webEngineView.setStyleSheet("border: 1px solid red;")
        self.maplabel_widget_layout = QHBoxLayout()
        self.maplabel_widget_layout.setSpacing(20)
        self.maplabel_widget_layout.setContentsMargins(0, 0, 0, 0)
        self.maplabel_widget.setLayout(self.maplabel_widget_layout)
        self.lat = QLabel(parent=self.maplabel_widget)
        self.set_label_stylesheet(self.lat)
        self.lat.setMaximumWidth(120)
        self.lon = QLabel(parent=self.maplabel_widget)
        self.set_label_stylesheet(self.lon)
        self.lon.setMaximumWidth(120)
        self.zoom = QLabel(parent=self.maplabel_widget)
        self.set_label_stylesheet(self.zoom)
        self.zoom.setMaximumWidth(120)
        # self.maplabel_widget_layout.addSpacing(50)
        self.maplabel_widget_layout.addWidget(self.lon)
        self.maplabel_widget_layout.addWidget(self.lat)
        self.maplabel_widget_layout.addWidget(self.zoom)
        self.maplabel_widget_layout.addStretch(1)
        self.webengine_loadtimer = QTimer()
        self.webengine_loadtimer.timeout.connect(self.rotate_image)
        self.webEngineView.settings().setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, True)
        self.webEngineView.settings().setAttribute(QWebEngineSettings.WebAttribute.PluginsEnabled, True)
        self.webwidget_layout.addWidget(self.browser_widget)
        self.webwidget_layout.addWidget(self.maplabel_widget)
        self.webengine_toolbutton_widget()
        self.create_geemap_widget()
        if self.temp_html_file is None:
            self.temp_html_file = self.get_pdf_html_from_db(html_id=1)
        if len(self.settings['proxies']) != 0:
            self.set_proxy(True)
        if self.settings['project_id'] is None and self.settings['json_file'] is None and self.settings['service_account'] is None:
            if self.temp_pdf_file is None:
                self.temp_pdf_file = self.get_pdf_html_from_db(pdf_id=1)
            self.webEngineView.load(QUrl.fromLocalFile(self.temp_pdf_file))
            self.maplabel_widget.hide()
            self.webtoolbuttonwidget.hide()
        else:
            self.webEngineView.load(QUrl.fromLocalFile(self.temp_html_file))
            self.lon.setText("经度：96.951170")
            self.lat.setText("纬度：36.452220")
            self.zoom.setText("层级：4")
        self.conn_to_js()
        return self.webwidget

    def webEngineView_load_geometry(self, geometry):
        if geometry != self.webengine_loaded_geometry:
            self.webEngineView.page().runJavaScript('removeOverlay()')
            self.webEngineView.page().runJavaScript(f'loadGeoJson({self.to_geojson(geometry)})')
            self.webengine_loaded_geometry = geometry

    def conn_to_js(self):
        self.backend = ts.Backend(self)
        self.channel = QWebChannel()
        self.channel.registerObject('connection_to_JS', self.backend)
        self.webEngineView.page().setWebChannel(self.channel)

    @Slot()
    def on_webEngineView_loadStarted(self):
        self.webengine_loadtimer.start(20)

    @Slot()
    def on_webEngineView_loadFinished(self):
        print('html loadfinished', self.dataset_name_combobox.currentText())
        self.dataset_name_combobox.blockSignals(True)
        try:
            self.load_html_timeout_timer.stop()
        except:
            pass
        try:
            if self.current_loading_dataset is not None:
                self.dataset_html_loaded = True
                self.disable_creategeemapwidget_widgets(False)
                self.data_source_lv1_combobox.setEnabled(False)
                self.data_source_lv2_combobox.setEnabled(False)
                self.data_source_lv3_combobox.setEnabled(False)
                self.range_type_combobox.setEnabled(False)
                self.pick_region_sn_combobox.setEnabled(False)
                self.pick_region_sh_combobox.setEnabled(False)
                self.pick_region_xn_combobox.setEnabled(False)
                if self.dataset_info[self.data_source_lv3_combobox.currentText()]['need_scale']:
                    self.data_scale_combobox.setEnabled(True)
                else:
                    self.data_scale_combobox.setEnabled(False)
                if self.dataset_info[self.data_source_lv3_combobox.currentText()]['need_date'] == 'no':
                    self.start_date_lineEdit.setEnabled(False)
                    self.start_date_calendar_button.setEnabled(False)
                    self.end_date_lineEdit.setEnabled(False)
                    self.end_date_calendar_button.setEnabled(False)
                else:
                    self.start_date_lineEdit.setEnabled(True)
                    self.start_date_calendar_button.setEnabled(True)
                    self.end_date_lineEdit.setEnabled(True)
                    self.end_date_calendar_button.setEnabled(True)
                if len(self.dataset_info[self.data_source_lv3_combobox.currentText()]['bands']) > 0:
                    self.bands_combobox.setEnabled(True)
                else:
                    self.bands_combobox.setEnabled(False)
            else:
                self.disable_creategeemapwidget_widgets(False)
                self.delete_dataset_button.setEnabled(False)
                self.download_dataset_button.setEnabled(False)
                self.bands_combobox.setEnabled(False)
                self.data_source_lv2_combobox.setEnabled(True)
                self.data_source_lv3_combobox.setEnabled(True)
                self.pick_region_sn_combobox.setEnabled(False)
                self.pick_region_sh_combobox.setEnabled(False)
                self.pick_region_xn_combobox.setEnabled(False)
        except Exception as e:
            print(e)
        self.webengine_loadtimer.stop()
        self.view_data_button_movie_label.hide()
        self.view_data_movie.stop()
        self.view_data_button.setText('预览数据集')
        self.view_data_button.setIcon(QIcon('resources/icon/view_dataset.png'))
        self.dataset_name_combobox.blockSignals(False)

    @Slot()
    def on_reload_webengine_toolbutton_clicked(self):
        self.webEngineView.reload()

    @Slot()
    def on_switch_visiualize_toolbutton_clicked(self):
        dataset = self.current_loading_dataset
        if dataset is not None and self.dataset_info[self.current_loading_datasource]['has_3d']:
            if self.current_loading_visualize == '2d':
                self.webEngineView.load(QUrl.fromLocalFile(self.dataset_path / f'{dataset}_3d.html'))
                self.current_loading_visualize = '3d'
            else:
                self.webEngineView.load(QUrl.fromLocalFile(self.dataset_path / f'{dataset}_2d.html'))
                self.current_loading_visualize = '2d'
        elif dataset is None:
            pass
        else:
            self.show_msg_box('该数据集不支持3d预览')

    def rotate_image(self):
        # 每次定时器触发时增加旋转角度
        self.reload_webengine_toolbutton_angle += 5  # 每次增加 5 度
        if self.reload_webengine_toolbutton_angle >= 360:
            self.angle = 0  # 如果旋转超过 360 度，则重新开始
        # 创建一个空的 QPixmap 来绘制旋转后的图像
        rotated_pixmap = QPixmap(self.reload_webengine_toolbutton.size())
        rotated_pixmap.fill(Qt.GlobalColor.transparent)  # 填充透明背景
        # 使用 QPainter 绘制旋转后的图像
        painter = QPainter(rotated_pixmap)
        transform = QTransform()
        # 获取按钮的中心位置
        center_x = self.reload_webengine_toolbutton.width() / 2
        center_y = self.reload_webengine_toolbutton.height() / 2
        # 将旋转原点设置为按钮的中心
        transform.translate(center_x, center_y)  # 将原点移动到按钮的中心
        transform.rotate(self.reload_webengine_toolbutton_angle)  # 旋转图像
        transform.translate(-self.reload_webengine_toolbutton.size().width() / 2, -self.reload_webengine_toolbutton.size().height() / 2)  # 绘制图像回原位置
        # 设置旋转变换
        painter.setTransform(transform)
        # 绘制旋转后的图像到新的 QPixmap
        painter.drawPixmap(0, 0, self.reload_webengine_toolbutton_pixmap)
        painter.end()
        # 强制将图像调整为适应按钮的大小
        resized_pixmap = rotated_pixmap.scaled(self.reload_webengine_toolbutton.size(), Qt.AspectRatioMode.KeepAspectRatio,
                                               Qt.TransformationMode.SmoothTransformation)
        # 设置按钮图标为调整大小后的图像
        self.reload_webengine_toolbutton.setIcon(QIcon(resized_pixmap))

    def create_geemap_widget(self):
        self.creategeemapwidget = QWidget(parent=self.webwidget)
        self.creategeemapwidget.setStyleSheet("background-color: rgb(243, 243, 243);")
        self.draw_polygon_tips_label = customwidget.OutlinedLabel(text='请在图上绘制数据范围', text_color=QColor('#87CEEB'), font_size=20,
                                                                  parent=self.central_widget)
        self.draw_polygon_tips_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.draw_polygon_tips_label.setStyleSheet("background-color: rgba(0, 0, 0, 0);")
        self.draw_polygon_tips_label.setFixedSize(500, 150)
        self.draw_polygon_tips_label.hide()
        self.creategeemapwidget_layout = QVBoxLayout()
        self.creategeemapwidget_layout.setContentsMargins(10, 20, 10, 20)
        self.creategeemapwidget.setLayout(self.creategeemapwidget_layout)
        # 上半部分各类控件表单布局
        self.creategeemapwidget_top_layout = QFormLayout()
        self.creategeemapwidget_top_layout.setContentsMargins(0, 0, 0, 0)
        self.creategeemapwidget_top_layout.setHorizontalSpacing(5)
        self.creategeemapwidget_top_layout.setVerticalSpacing(15)
        self.dataset_name_lable = QLabel(text='数据集名:', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.dataset_name_lable)
        self.dataset_name_combobox = customwidget.CustomLineEdit(main_intance=self, PlaceholderText='仅字母、数字和下划线',
                                                                 parent=self.creategeemapwidget, ObjectName='dataset_name_combobox')
        self.dataset_name_combobox_update_items()
        self.dataset_name_combobox.setCurrentIndex(-1)
        self.set_combobox_stylesheet(self.dataset_name_combobox)
        regex = QRegularExpression(r'^[a-zA-Z0-9_]*$')  # 只允许字母、数字和下划线
        validator = QRegularExpressionValidator(regex, self.dataset_name_combobox)
        self.dataset_name_combobox.setValidator(validator)
        self.data_source_lable = QLabel(text='原始数据:', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.data_source_lable)
        data_source_lv1_result, tooltip_dict = self.get_original_datasource(parent=0, level=1)
        self.data_source_lv1_combobox = customwidget.HoverComboBox(parent=self.creategeemapwidget, ObjectName='data_source_lv1_combobox',
                                                                   tooltip_dict=tooltip_dict)
        self.set_combobox_stylesheet(self.data_source_lv1_combobox)
        self.data_source_lv1_combobox.setEditable(True)
        data_source_lv1_combobox_items = []
        for abbreviation, data_id, notes in data_source_lv1_result:
            self.data_source_lv1_combobox.addItem(abbreviation)
            self.data_source_lv1_combobox.setItemData(self.data_source_lv1_combobox.count() - 1, data_id)
            data_source_lv1_combobox_items.append(abbreviation)
        self.data_source_lv1_combobox_completer = customwidget.CustomCompleter(data_source_lv1_combobox_items)
        self.data_source_lv1_combobox.setCompleter(self.data_source_lv1_combobox_completer)
        self.data_source_lv1_combobox.setCurrentIndex(-1)
        self.data_source_lv2_combobox = customwidget.HoverComboBox(parent=self.creategeemapwidget, ObjectName='data_source_lv2_combobox')
        self.set_combobox_stylesheet(self.data_source_lv2_combobox)
        self.data_source_lv2_combobox.setEditable(True)
        self.data_source_lv3_combobox = customwidget.HoverComboBox(parent=self.creategeemapwidget, ObjectName='data_source_lv3_combobox', show_time=30000)
        self.set_combobox_stylesheet(self.data_source_lv3_combobox)
        self.data_source_lv3_combobox.setEditable(True)
        self.data_info_toolbutton = QToolButton(parent=self.creategeemapwidget, ObjectName='data_info_toolbutton')
        self.set_toolbutton_stylesheet(self.data_info_toolbutton)
        self.data_info_toolbutton.setIcon(QIcon("resources/icon/link.png"))
        self.start_date_label = QLabel(text='开始时间:', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.start_date_label)
        self.start_date_layout = QHBoxLayout()
        self.start_date_layout.setSpacing(0)
        self.start_date_layout.setContentsMargins(0, 0, 0, 0)
        self.start_date_lineEdit = QLineEdit(parent=self.creategeemapwidget)
        self.set_lineedit_stylesheet(self.start_date_lineEdit)
        date_regex = QRegularExpression(r"^\d{4}-\d{2}-\d{2}$")  # 只允许'2024-11-01'形式日期
        start_date_validator = QRegularExpressionValidator(date_regex, self.start_date_lineEdit)
        self.start_date_lineEdit.setValidator(start_date_validator)
        self.start_date_lineEdit.editingFinished.connect(self.is_valid_date)
        self.start_date_calendar_button = QPushButton(text='...', parent=self.creategeemapwidget, ObjectName='start_date_calendar_button')
        self.set_pushbutton_stylesheet(self.start_date_calendar_button)
        self.start_date_layout.addWidget(self.start_date_lineEdit)
        self.start_date_layout.addWidget(self.start_date_calendar_button)
        self.start_date_calendar = customwidget.CustomCalendar(parent=self.creategeemapwidget, ObjectName='start_date_calendar')
        self.start_date_calendar.setGeometry(20, 190, 230, 240)
        self.set_calendar_stylesheet(self.start_date_calendar)
        self.start_date_calendar.hide()
        self.end_date_label = QLabel(text='结束时间：', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.end_date_label)
        self.end_date_layout = QHBoxLayout()
        self.end_date_layout.setSpacing(0)
        self.end_date_layout.setContentsMargins(0, 0, 0, 0)
        self.end_date_lineEdit = QLineEdit(parent=self.creategeemapwidget)
        end_date_validator = QRegularExpressionValidator(date_regex, self.end_date_lineEdit)
        self.end_date_lineEdit.setValidator(end_date_validator)
        self.end_date_lineEdit.editingFinished.connect(self.is_valid_date)
        self.set_lineedit_stylesheet(self.end_date_lineEdit)
        self.end_date_calendar_button = QPushButton(text='...', parent=self.creategeemapwidget, ObjectName='end_date_calendar_button')
        self.set_pushbutton_stylesheet(self.end_date_calendar_button)
        self.end_date_layout.addWidget(self.end_date_lineEdit)
        self.end_date_layout.addWidget(self.end_date_calendar_button)
        self.end_date_calendar = customwidget.CustomCalendar(parent=self.creategeemapwidget, ObjectName='end_date_calendar')
        self.end_date_calendar.setGeometry(20, 230, 230, 240)
        self.set_calendar_stylesheet(self.end_date_calendar)
        self.end_date_calendar.hide()
        self.base_map_lable = QLabel(text='地图底图：', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.base_map_lable)
        self.base_map_combobox = QComboBox(parent=self.creategeemapwidget)
        self.set_combobox_stylesheet(self.base_map_combobox)
        self.base_map_combobox.addItems(['矢量底图', '影像底图', '地形底图'])
        self.data_scale_lable = QLabel(text='分辨率：', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.data_scale_lable)
        self.data_scale_combobox = QComboBox(parent=self.creategeemapwidget)
        self.set_combobox_stylesheet(self.data_scale_combobox)
        self.data_scale_combobox.addItems(['10', '30', '50', '70', '90'])
        self.range_type_lable = QLabel(text='数据范围：', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.range_type_lable)
        self.range_type_combobox = QComboBox(parent=self.creategeemapwidget, ObjectName='range_type_combobox')
        self.range_type_combobox.setEditable(True)
        self.range_type_combobox.lineEdit().setReadOnly(True)
        self.set_combobox_stylesheet(self.range_type_combobox)
        self.range_type_combobox.setPlaceholderText('请选择数据范围')
        self.range_type_combobox.addItems(['绘制矩形', '绘制多边形', '选择区域', '选择区域（绘制）', '自定义区域'])
        self.pick_region_sn_lable = QLabel(text='省/直辖市：', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.pick_region_sn_lable)
        self.pick_region_sn_combobox = QComboBox(parent=self.creategeemapwidget, ObjectName='pick_region_sn_combobox')
        self.pick_region_sn_combobox.setEditable(True)
        self.pick_region_sn_combobox.lineEdit().setObjectName('pick_region_sn_combobox_linedit')
        self.set_combobox_stylesheet(self.pick_region_sn_combobox)
        self.pick_region_sh_lable = QLabel(text='地级市：', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.pick_region_sh_lable)
        self.pick_region_sh_combobox = QComboBox(parent=self.creategeemapwidget, ObjectName='pick_region_sh_combobox')
        self.pick_region_sh_combobox.setEditable(True)
        self.pick_region_sh_combobox.lineEdit().setObjectName('pick_region_sh_combobox_linedit')
        self.set_combobox_stylesheet(self.pick_region_sh_combobox)
        self.pick_region_xn_lable = QLabel(text='区/县：', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.pick_region_xn_lable)
        self.pick_region_xn_combobox = QComboBox(parent=self.creategeemapwidget, ObjectName='pick_region_xn_combobox')
        self.pick_region_xn_combobox.setEditable(True)
        self.pick_region_xn_combobox.lineEdit().setObjectName('pick_region_xn_combobox_linedit')
        self.set_combobox_stylesheet(self.pick_region_xn_combobox)
        self.savepath_lable = QLabel(text='存储路径：', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.savepath_lable)
        self.savepath_layout = QHBoxLayout()
        self.savepath_lineEdit = QLineEdit(parent=self.creategeemapwidget)
        self.set_lineedit_stylesheet(self.savepath_lineEdit)
        self.savepath_lineEdit.setText(self.settings['last_save_path'])
        self.savepath_pushbutton = QPushButton(parent=self.creategeemapwidget)
        self.set_pushbutton_stylesheet(self.savepath_pushbutton)
        self.savepath_pushbutton.clicked.connect(self.choosepath)
        self.savepath_pushbutton.setText("...")
        self.savepath_layout.addWidget(self.savepath_lineEdit)
        self.savepath_layout.addWidget(self.savepath_pushbutton)
        self.bands_lable = QLabel(text='频段：', parent=self.creategeemapwidget)
        self.set_label_stylesheet(self.bands_lable)
        self.bands_combobox = customwidget.CheckableComboBox(parent=self.creategeemapwidget)
        self.set_combobox_stylesheet(self.bands_combobox)
        self.creategeemapwidget_top_layout.addRow(self.dataset_name_lable, self.dataset_name_combobox)
        self.creategeemapwidget_top_layout.addRow(self.data_source_lable, self.data_source_lv1_combobox)
        self.creategeemapwidget_top_layout.addRow(QLabel('', parent=self.creategeemapwidget), self.data_source_lv2_combobox)
        self.creategeemapwidget_top_layout.addRow(self.data_info_toolbutton, self.data_source_lv3_combobox)
        self.creategeemapwidget_top_layout.addRow(self.start_date_label, self.start_date_layout)
        self.creategeemapwidget_top_layout.addRow(self.end_date_label, self.end_date_layout)
        self.creategeemapwidget_top_layout.addRow(self.base_map_lable, self.base_map_combobox)
        self.creategeemapwidget_top_layout.addRow(self.data_scale_lable, self.data_scale_combobox)
        self.creategeemapwidget_top_layout.addRow(self.range_type_lable, self.range_type_combobox)
        self.creategeemapwidget_top_layout.addRow(self.pick_region_sn_lable, self.pick_region_sn_combobox)
        self.creategeemapwidget_top_layout.addRow(self.pick_region_sh_lable, self.pick_region_sh_combobox)
        self.creategeemapwidget_top_layout.addRow(self.pick_region_xn_lable, self.pick_region_xn_combobox)
        self.creategeemapwidget_top_layout.addRow(self.savepath_lable, self.savepath_layout)
        self.creategeemapwidget_top_layout.addRow(self.bands_lable, self.bands_combobox)
        # 下半部分网格布局
        self.creategeemapwidget_bot_layout = QGridLayout()
        self.creategeemapwidget_bot_layout.setContentsMargins(0, 0, 0, 0)
        self.creategeemapwidget_bot_layout.setSpacing(10)
        self.view_data_button = QToolButton(parent=self.creategeemapwidget, ObjectName='view_data_button')
        self.view_data_button.setIcon(QIcon('resources/icon/view_dataset.png'))
        self.view_data_button.setIconSize(QSize(40, 40))
        self.view_data_button.setText('预览数据集')
        self.set_toolbutton_stylesheet(self.view_data_button)
        self.view_data_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextUnderIcon)
        self.view_data_button_layout = QHBoxLayout()
        self.view_data_button_layout.setContentsMargins(0, 0, 0, 0)
        self.view_data_button.setLayout(self.view_data_button_layout)
        self.view_data_movie = QMovie("resources/icon/spinner.gif")
        self.view_data_button_movie_label = QLabel(parent=self.view_data_button)
        self.view_data_button_movie_label.setStyleSheet("background-color: rgba(0,0,0,0);")
        self.view_data_button_movie_label.setMovie(self.view_data_movie)
        self.view_data_button_movie_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.view_data_button_movie_label.hide()
        self.view_data_button_layout.addWidget(self.view_data_button_movie_label)
        self.download_dataset_button = QToolButton(parent=self.creategeemapwidget, ObjectName='download_dataset_button')
        self.download_dataset_button.setIcon(QIcon('resources/icon/download_dataset.png'))
        self.download_dataset_button.setIconSize(QSize(40, 40))
        self.download_dataset_button.setText('下载数据集')
        self.set_toolbutton_stylesheet(self.download_dataset_button)
        self.download_dataset_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextUnderIcon)
        self.delete_dataset_button = QToolButton(parent=self.creategeemapwidget, ObjectName='delete_dataset_button')
        self.delete_dataset_button.setIcon(QIcon('resources/icon/delete_dataset.png'))
        self.delete_dataset_button.setIconSize(QSize(40, 40))
        self.delete_dataset_button.setText('删除数据集')
        self.set_toolbutton_stylesheet(self.delete_dataset_button)
        self.delete_dataset_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextUnderIcon)
        self.new_dataset_button = QToolButton(parent=self.creategeemapwidget, ObjectName='new_dataset_button')
        self.new_dataset_button.setIcon(QIcon('resources/icon/new_dataset.png'))
        self.new_dataset_button.setIconSize(QSize(40, 40))
        self.new_dataset_button.setText('新建数据集')
        self.set_toolbutton_stylesheet(self.new_dataset_button)
        self.set_toolbutton_stylesheet(self.new_dataset_button)
        self.new_dataset_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextUnderIcon)
        self.creategeemapwidget_bot_layout.addWidget(self.view_data_button, 0, 0)
        self.creategeemapwidget_bot_layout.addWidget(self.download_dataset_button, 0, 1)
        self.creategeemapwidget_bot_layout.addWidget(self.delete_dataset_button, 1, 0)
        self.creategeemapwidget_bot_layout.addWidget(self.new_dataset_button, 1, 1)
        self.creategeemapwidget_layout.addLayout(self.creategeemapwidget_top_layout)
        self.creategeemapwidget_layout.addLayout(self.creategeemapwidget_bot_layout)
        self.creategeemapwidget_layout.addStretch(1)
        for label in self.creategeemapwidget.findChildren(QLabel):
            if label != self.view_data_button_movie_label:
                label.setMinimumHeight(20)
                label.setMaximumHeight(20)
                label.setMinimumWidth(40)
                label.setMaximumWidth(60)
        for lineedit in self.creategeemapwidget.findChildren(QLineEdit):
            lineedit.setMinimumHeight(20)
            lineedit.setMaximumHeight(20)
            lineedit.setMinimumWidth(170)
        for pushbutton in self.creategeemapwidget.findChildren(QPushButton):
            pushbutton.setMinimumHeight(20)
            pushbutton.setMaximumHeight(20)
            pushbutton.setMinimumWidth(20)
            pushbutton.setMaximumWidth(20)
        for toolbutton in self.creategeemapwidget.findChildren(QToolButton):
            if toolbutton == self.data_info_toolbutton:
                toolbutton.setMinimumHeight(20)
                toolbutton.setMaximumHeight(20)
                toolbutton.setMinimumWidth(20)
                toolbutton.setMaximumWidth(20)
            else:
                toolbutton.setMinimumHeight(60)
                toolbutton.setMaximumHeight(60)
                toolbutton.setMinimumWidth(100)
                toolbutton.setMaximumWidth(100)
        # for widget in self.creategeemapwidget.findChildren(QWidget):
        #     widget.setStyleSheet("border: 1px solid red;")
        return self.creategeemapwidget

    @Slot()
    def on_data_info_toolbutton_clicked(self):
        datainfo_url = self.get_datainfo_url(self.data_source_lv3_combobox.currentText())
        webbrowser.open(url=datainfo_url)

    @Slot()
    def on_download_dataset_button_clicked(self):
        taskname = self.current_loading_dataset
        downloadpath = self.savepath_lineEdit.text()
        try:
            task_df = pd.read_xml(self.dataset_path / f'{taskname}.xml')
            dataset = task_df.loc[0, 'data_source_lv3']
            task_df['downloadpath'] = str(Path(downloadpath).resolve() / f'{taskname}')
            task_df['taskname'] = taskname
            task_df['createtime'] = time.time()
            task_df['finishtime'] = None
            task_df['downloadprogress'] = None
            task_df['stitchprogress'] = None
            task_df['cropprogress'] = None
            task_df['is_export_shp'] = True
            task_df['is_CalculateTiles_done'] = False
            task_df['is_TileDownload_done'] = False
            task_df['is_TileStitch_done'] = False
            task_df['is_executing'] = True
        except:
            pass
        if self.current_loading_dataset is None:
            self.show_msg_box('必须建立或指定数据集')
        elif downloadpath == '' or not self.is_valid_path(Path(downloadpath).resolve() / f'{taskname}'):
            self.show_msg_box('必须提供可用的下载路径（文件夹）')
        elif self.dataset_info[dataset]['need_date'] != 'no' and (self.start_date_lineEdit.text() != task_df.loc[0, 'start_date']
                                                                  or self.end_date_lineEdit.text() != task_df.loc[0, 'end_date']):
            self.show_msg_box('您已经修改了该数据集的起止时间，预览确认后，才可以下载')
        elif self.dataset_info[dataset]['need_scale'] and int(self.data_scale_combobox.currentText()) != int(task_df.loc[0, 'data_scale']):
            self.show_msg_box('您已经修改了该数据集的分辨率，预览确认后，才可以下载')
        elif self.bands_combobox.currentText() != task_df.loc[0, 'bands'] and len(self.dataset_info[dataset]['bands']) > 0:
            self.show_msg_box('您已经修改了该数据集的频段，预览确认后，才可以下载')
        elif len(self.dataset_info[dataset]['bands']) > 0 and self.bands_combobox.currentText() == '':
            self.show_msg_box('您至少需要选择一个下载频段')
        else:
            self.newtask_timer = QTimer()
            self.newtask_timer.timeout.connect(lambda: self.newtask_updateprogress(taskname))
            if os.path.isfile(self.task_path / f'{taskname}.xml'):
                msg_box = QMessageBox()
                self.set_messagebox_stylesheet(msg_box)
                msg_box.setIcon(QMessageBox.Icon.Question)
                msg_box.setText("该任务已存在，是否覆盖？")
                msg_box.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                if msg_box.exec() == QMessageBox.StandardButton.Yes:
                    self.subprocess.closeone(taskname)
                    try:
                        last_task_downloadpath = self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'downloadpath'].values
                        shutil.rmtree(last_task_downloadpath[0])
                        os.makedirs(Path(downloadpath).resolve() / f'{taskname}')
                    except Exception as e:
                        pass
                    self.exists_task = self.exists_task[self.exists_task['taskname'] != f'{taskname}']
                    self.write_settings('last_save_path', downloadpath)
                    with open(self.task_path / f'{taskname}.xml', 'w', encoding='utf-8') as f:
                        f.write(task_df.to_xml())
                    self.exists_task = (
                        self.exists_task.copy() if task_df.empty else task_df.copy() if self.exists_task.empty else pd.concat([self.exists_task, task_df]))
                    self.download_dataset_button.setEnabled(False)
                    self.newtaskwidget.show()
                    self.progress_value = 0
                    self.newtask_timer.start(10)
                    self.task_execute(self.task_path / f'{taskname}.xml')
                else:
                    self.newtask_timer.deleteLater()
            else:
                self.write_settings('last_save_path', downloadpath)
                with open(self.task_path / f'{taskname}.xml', 'w', encoding='utf-8') as f:
                    f.write(task_df.to_xml())
                self.exists_task = (
                    self.exists_task.copy() if task_df.empty else task_df.copy() if self.exists_task.empty else pd.concat([self.exists_task, task_df]))
                self.sort_downloadtasks()
                self.download_dataset_button.setEnabled(False)
                self.newtaskwidget.show()
                self.progress_value = 0
                self.newtask_timer.start(10)
                self.task_execute(self.task_path / f'{taskname}.xml')

    @Slot()
    def on_view_data_button_clicked(self):
        try:
            dataset_start_date = self.dataset_info[self.data_source_lv3_combobox.lineEdit().text()]['start_date']
            dataset_end_date = self.dataset_info[self.data_source_lv3_combobox.lineEdit().text()]['end_date']
        except:
            dataset_start_date = datetime(1000, 1, 1)
            dataset_end_date = datetime(9999, 12, 31)
        if not self.start_date_lineEdit.text():
            self.start_date_lineEdit.setText(self.start_date_lineEdit.placeholderText())
        if not self.end_date_lineEdit.text():
            self.end_date_lineEdit.setText(self.end_date_lineEdit.placeholderText())
        if self.dataset_name_combobox.lineEdit().text():
            datasetname = self.dataset_name_combobox.lineEdit().text()
        else:
            datasetname = self.get_default_datasetname()
        try:
            if self.dataset_info[self.data_source_lv3_combobox.currentText()]['need_scale']:
                scale = int(self.data_scale_combobox.currentText())
            else:
                scale = None
        except:
            pass
        if self.data_source_lv3_combobox.currentIndex() == -1:
            self.show_msg_box('必须选择原始数据')
        elif self.range_type_combobox.currentIndex() == -1:
            self.show_msg_box('必须选择数据范围')
        elif (self.dataset_info[self.data_source_lv3_combobox.currentText()]['need_date'] != 'no' and
              (not self.start_date_lineEdit.text() or not self.end_date_lineEdit.text()
               or datetime.strptime(self.start_date_lineEdit.text(), "%Y-%m-%d").date() < dataset_start_date
               or datetime.strptime(self.end_date_lineEdit.text(), "%Y-%m-%d").date() > dataset_end_date)):
            self.show_msg_box(f'必须输入起止时间，且不能超过谷歌发布的该数据集时间范围<br>开始时间： {dataset_start_date}<br>结束时间： {dataset_end_date}')
        elif self.polygon_from_js['polygon'] is None and (self.range_type_combobox.currentIndex() == 0 or self.range_type_combobox.currentIndex() == 1):
            self.show_msg_box('必须在地图绘制数据范围')
        elif self.range_type_combobox.currentIndex() == 2 and self.picked_region[3] is None:
            self.show_msg_box('必须选择数据区域')
        elif self.range_type_combobox.currentIndex() == 3 and self.picked_region_draw is None:
            self.show_msg_box('必须提供数据区域(绘制)')
        elif self.range_type_combobox.currentIndex() == 4 and self.custom_region is None:
            self.show_msg_box('必须上传数据区域')
        elif datasetname == '' or datasetname.isdigit() or datasetname.startswith('_'):
            self.show_msg_box('任务名称仅支持字母、数字和"_"且不能以纯数字或"_"开头')
        elif not self.bands_combobox.checkedItemsStr() and len(self.dataset_info[self.data_source_lv3_combobox.currentText()]['bands']) > 0:
            self.show_msg_box('至少需要选择一个频段')
        else:
            if self.bands_combobox.currentText():
                try:
                    bands = tuple(map(str, self.bands_combobox.checkedItemsStr().split(',')))
                except:
                    bands = (self.bands_combobox.checkedItemsStr(),)
            else:
                bands = None
            self.dataset_name_combobox.blockSignals(True)
            self.draw_polygon_tips_label.hide()
            self.view_data_button.setText('')
            self.view_data_button.setIcon(QIcon())
            self.view_data_button_movie_label.show()
            self.view_data_movie.setScaledSize(QSize(self.view_data_button_movie_label.size().height(), self.view_data_button_movie_label.size().height()))
            self.view_data_movie.start()
            if self.range_type_combobox.currentIndex() == 0 or self.range_type_combobox.currentIndex() == 1:
                region = self.polygon_from_js['polygon']
                adcode_picked_region_draw = None
            elif self.range_type_combobox.currentIndex() == 2:
                region = self.get_region(region=self.picked_region[0], adcode=self.picked_region[2])[0][0]
                self.picked_region = self.picked_region + (region,)
                adcode_picked_region_draw = None
            elif self.range_type_combobox.currentIndex() == 3:
                region = self.picked_region_draw
                adcode_picked_region_draw = ','.join(adcode for adcode in self.adcode_picked_region_draw)
            elif self.range_type_combobox.currentIndex() == 4:
                region = self.custom_region
                adcode_picked_region_draw = None
            while True:
                if self.is_eeinitialization_done:
                    break
                else:
                    time.sleep(0.01)
            self.disable_creategeemapwidget_widgets()
            load_new_dataset_thread = threading.Thread(target=self.load_new_dataset, args=(datasetname, region, scale,
                                                                                           self.data_source_lv3_combobox.currentText(),
                                                                                           adcode_picked_region_draw, bands))
            load_new_dataset_thread.start()
            self.lon.hide()
            self.lat.hide()
            self.zoom.hide()
            self.load_html_timeout_timer.start(self.settings['loading_html_timeout_ms'])
            self.dataset_name_combobox.blockSignals(False)

    @staticmethod
    def format_region(wkt_string):
        region = make_valid(loads(wkt_string))
        if isinstance(region, Polygon):
            return wkt_string
        elif isinstance(region, MultiPolygon):
            if len(region.geoms) == 1:
                return dumps(region.geoms[0])
            else:
                return wkt_string
        elif isinstance(region, GeometryCollection):
            polygon_list = []
            for geom in region.geoms:
                if isinstance(geom, Polygon):
                    polygon_list.append(geom)
                elif isinstance(geom, MultiPolygon):
                    for geo in geom.geoms:
                        polygon_list.append(geo)
            return dumps(MultiPolygon(polygon_list)) if len(polygon_list) > 1 else dumps(Polygon(polygon_list[0]))

    def load_new_dataset(self, datasetname, region, scale, datasource_lv3, adcode_picked_region_draw, bands=None):
        try:
            self.dataset_name_combobox.blockSignals(True)
            self.map.export_html(ee_object=self.data_source_lv3_combobox.currentText(), savepath=self.dataset_path, datasetname=datasetname, region=region,
                                 basemap=self.base_map_combobox.currentText(), map_style='2d', scale=scale, min_zoom=self.settings['min_zoom_3d'], bands=bands,
                                 start_date=self.start_date_lineEdit.text(), end_date=self.end_date_lineEdit.text(), opacity=self.settings['map_opacity'])
            self.map.export_html(ee_object=self.data_source_lv3_combobox.currentText(), savepath=self.dataset_path, datasetname=datasetname, region=region,
                                 basemap=self.base_map_combobox.currentText(), map_style='3d', scale=scale, min_zoom=self.settings['min_zoom_3d'], bands=bands,
                                 start_date=self.start_date_lineEdit.text(), end_date=self.end_date_lineEdit.text(), opacity=self.settings['map_opacity'])
            self.current_loading_datasource = datasource_lv3
            self.current_loading_dataset = datasetname
            if self.settings['default_map_visualize'] == '2d':
                self.current_loading_visualize = '2d'
            else:
                self.current_loading_visualize = '3d'
            dataparameter = pd.DataFrame(data={'datasetname': datasetname,
                                               'createtime': time.time(),
                                               'data_source_lv1': self.data_source_lv1_combobox.currentText(),
                                               'data_source_lv2': self.data_source_lv2_combobox.currentText(),
                                               'data_source_lv3': self.data_source_lv3_combobox.currentText(),
                                               'datasetregion': self.format_region(region),
                                               'base_map': self.base_map_combobox.currentText(),
                                               'datarangetype': self.range_type_combobox.currentIndex(),
                                               'adcode_picked_region_draw': adcode_picked_region_draw,
                                               'picked_sn': self.pick_region_sn_combobox.itemData(self.pick_region_sn_combobox.currentIndex()),
                                               'picked_sh': self.pick_region_sh_combobox.itemData(self.pick_region_sh_combobox.currentIndex()),
                                               'picked_xn': self.pick_region_xn_combobox.itemData(self.pick_region_xn_combobox.currentIndex()),
                                               'data_scale': scale,
                                               'bands': ','.join(bands) if bands is not None else '',
                                               'start_date': self.start_date_lineEdit.text(),
                                               'end_date': self.end_date_lineEdit.text()},
                                         index=['data'])
            with open(self.dataset_path / f'{datasetname}.xml', 'w', encoding='utf-8') as f:
                f.write(dataparameter.to_xml())
            self.exists_dataset = self.exists_dataset[self.exists_dataset['datasetname'] != datasetname]
            self.exists_dataset = (self.exists_dataset.copy() if dataparameter.empty else dataparameter.copy()
            if self.exists_dataset.empty else pd.concat([self.exists_dataset, dataparameter]))
            self.export_html_signal.emit(datasetname)
            self.dataset_name_combobox.blockSignals(False)
        except Exception as e:
            self.export_html_signal.emit('1fail')
            self.export_html_exception = e

    def task_execute(self, taskfile):
        error = None
        task_parameter = pd.read_xml(taskfile)
        taskname = task_parameter.loc[0, 'taskname']
        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_executing'] = True
        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_download_failed'] = False
        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_complete'] = False
        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'remaining_time'] = None
        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'this_start_time'] = None
        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'speed_calculate_list'] = None
        if self.license_check_result is None or not self.license_check_result:
            error = self.license_check()
        if self.license_check_result is None or not self.license_check_result:
            self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_executing'] = False
            self.view_toolbutton.click()
            self.task_toolbutton.setEnabled(False)
            if error is not None and 'network error' in str(error):
                self.show_msg_box('请检查你的网络连接')
            else:
                self.show_msg_box('没有可用许可，请联系你的管理员')
            return False
        else:
            while True:
                if self.subprocess is not None:
                    break
                time.sleep(0.01)
            downloadpath = task_parameter.loc[0, 'downloadpath']
            ee_object = task_parameter.loc[0, 'data_source_lv3']
            downloadpolygon = task_parameter.loc[0, 'datasetregion']
            datarangetype = task_parameter.loc[0, 'datarangetype']
            ee_initialize = (self.settings['service_account'], self.settings['json_file'], self.settings['project_id'])
            start_date = task_parameter.loc[0, 'start_date']
            end_date = task_parameter.loc[0, 'end_date']
            scale = task_parameter.loc[0, 'data_scale']
            bands = task_parameter.loc[0, 'bands']
            if isinstance(bands, float) and numpy.isnan(bands):
                bands = None
            else:
                try:
                    bands = tuple(map(str, bands.split(',')))
                except:
                    bands = (bands,)
            picked_sn = task_parameter.loc[0, 'picked_sn']
            picked_sh = task_parameter.loc[0, 'picked_sh']
            picked_xn = task_parameter.loc[0, 'picked_xn']
            is_export_shp = task_parameter.loc[0, 'is_export_shp']
            try:
                adcode_picked_region_draw = tuple(map(int, task_parameter.loc[0, 'adcode_picked_region_draw'].split(',')))
            except:
                adcode_picked_region_draw = (task_parameter.loc[0, 'adcode_picked_region_draw'],)
            regionname = self.get_regionname(datarangetype, picked_sn, picked_sh, picked_xn, adcode_picked_region_draw)
            self.subprocess.process_done_dict[taskname] = {}
            self.subprocess.process_done_dict[taskname]['CalculateTiles'] = task_parameter.loc[0, 'is_CalculateTiles_done']
            self.subprocess.process_done_dict[taskname]['TileDownload'] = task_parameter.loc[0, 'is_TileDownload_done']
            self.subprocess.process_done_dict[taskname]['TileStitch'] = task_parameter.loc[0, 'is_TileStitch_done']
            self.task_download_times[taskname] = 0
            self.execute_task_timer[taskname] = QTimer()
            self.execute_task_timer[taskname].timeout.connect(
                lambda: self.task_timer(taskfile, taskname, ee_object, downloadpath, ee_initialize, start_date,
                                        end_date, bands, scale, downloadpolygon, regionname, is_export_shp))
            self.execute_task_timer[taskname].start(500)
            return True

    def task_timer(self, taskfile, taskname, ee_object, downloadpath, ee_initialize, start_date, end_date, bands, scale, downloadpolygon, regionname,
                   is_export_shp):
        try:
            status = self.subprocess.is_task_inprogress(taskname)
            try:
                while True:
                    try:
                        data = self.subprocess.queue_dict[taskname].get_nowait()
                        self.subprocess.progress_info_dict[taskname].update(data[0])
                        self.subprocess.process_done_dict[taskname].update(data[1])
                    except:
                        break
            except Exception as e:
                pass
            try:
                if self.subprocess.process_done_dict[taskname]['process_ended']:
                    self.subprocess.close_queue(taskname)
                    os.kill(self.subprocess.process_dict[taskname][0], signal.SIGTERM)
            except Exception as e:
                pass
            if not status:
                self.subprocess.create_queue(taskname)
                is_calculatetiles_done = self.subprocess.process_done_dict[taskname]['CalculateTiles']
                is_tiledownload_done = self.subprocess.process_done_dict[taskname]['TileDownload']
                is_tilestitch_done = self.subprocess.process_done_dict[taskname]['TileStitch']
                self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_CalculateTiles_done'] = is_calculatetiles_done
                self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_TileDownload_done'] = is_tiledownload_done
                self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_TileStitch_done'] = is_tilestitch_done
                if not is_calculatetiles_done and not is_tiledownload_done and not is_tilestitch_done:
                    target = 'CalculateTiles'
                    savepath = downloadpath
                    self.subprocess.new_process((taskname, target, self.settings['proxies'], ee_object, savepath, downloadpolygon,
                                                 ee_initialize, start_date, end_date, scale, bands))
                elif is_calculatetiles_done and not is_tiledownload_done and not is_tilestitch_done:
                    self.task_download_times[f'{taskname}'] = self.task_download_times[f'{taskname}'] + 1
                    if self.task_download_times[f'{taskname}'] > 3:
                        notification.notify(message=f'任务： {taskname}，下载失败，请检查网络或代理设置', app_icon='resources/icon/Nevasa.ico', timeout=5)
                        self.close_task_timer(taskname)
                        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_executing'] = False
                        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_complete'] = False
                        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_download_failed'] = True
                        self.update_taskscrollarea()
                        self.stackwidget.setCurrentIndex(1)
                        # self.show_msg_box(f'任务： {taskname}，下载失败，请检查网络或代理设置')
                    else:
                        target = 'TileDownload'
                        savepath = downloadpath + f'/{taskname}.nev'
                        with open(taskfile, 'r', encoding='utf-8') as file:
                            xml_str = file.read()
                        task_info = xmltodict.parse(xml_str)
                        task_info['data']['row']['is_CalculateTiles_done'] = True
                        xml_str = xmltodict.unparse(task_info, pretty=True)
                        with open(taskfile, 'w', encoding='utf-8') as f:
                            f.write(xml_str)
                        self.subprocess.new_process(
                            (taskname, target, savepath, ee_object, start_date, end_date, self.settings['proxies'], ee_initialize, scale))
                elif is_calculatetiles_done and is_tiledownload_done and not is_tilestitch_done:
                    target = 'TileStitch'
                    sqlitename = f'{taskname}.nev'
                    if self.settings['enable_gpu']:
                        try:
                            e = self.subprocess.process_done_dict[taskname][f'gpu_exception']
                            if 'Error when trying to use GPU' in e:
                                self.settings['enable_gpu'] = False
                        except:
                            pass
                    with open(taskfile, 'r', encoding='utf-8') as file:
                        xml_str = file.read()
                    task_info = xmltodict.parse(xml_str)
                    task_info['data']['row']['is_TileDownload_done'] = True
                    task_info['data']['row']['downloadprogress'] = 100
                    xml_str = xmltodict.unparse(task_info, pretty=True)
                    with open(taskfile, 'w', encoding='utf-8') as f:
                        f.write(xml_str)
                    datainfo_url = self.get_datainfo_url(ee_object)
                    if self.settings['enable_gpu']:
                        kernel_func = self.get_cuda_kernel_func()
                    else:
                        kernel_func = None
                    self.subprocess.new_process(
                        (taskname, target, downloadpath, sqlitename, ee_object, downloadpolygon, scale, regionname, start_date, end_date,
                         datainfo_url, is_export_shp, self.settings['enable_gpu'], kernel_func))
                elif is_calculatetiles_done and is_tiledownload_done and is_tilestitch_done:
                    finishtime = time.time()
                    with open(taskfile, 'r', encoding='utf-8') as file:
                        xml_str = file.read()
                    task_info = xmltodict.parse(xml_str)
                    task_info['data']['row']['is_TileStitch_done'] = True
                    task_info['data']['row']['finishtime'] = finishtime
                    task_info['data']['row']['stitchprogress'] = 100
                    task_info['data']['row']['cropprogress'] = 100
                    xml_str = xmltodict.unparse(task_info, pretty=True)
                    with open(taskfile, 'w', encoding='utf-8') as f:
                        f.write(xml_str)
                    self.close_task_timer(taskname)
                    self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_executing'] = False
                    self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'finishtime'] = finishtime
                    self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_complete'] = True
                    index = self.stackwidget.currentIndex()
                    self.replace_widget(self.taskscrollarea, self.task_scrollarea)
                    self.all_tasks_radio.click()
                    self.stackwidget.setCurrentIndex(index)
                    self.subprocess.clear_dict(taskname)
                    notification.notify(message=f'任务： {taskname}已经完成', app_icon='resources/icon/Nevasa.ico', timeout=5)
        except Exception as e:
            traceback.print_exc()

    def close_task_timer(self, taskname):
        self.execute_task_timer[f'{taskname}'].stop()
        self.execute_task_timer[f'{taskname}'].deleteLater()
        self.exists_task.loc[self.exists_task['taskname'] == f'{taskname}', 'is_executing'] = False
        del self.execute_task_timer[f'{taskname}']

    @Slot()
    def on_delete_dataset_button_clicked(self):
        try:
            self.range_type_combobox.blockSignals(True)
            self.pick_region_sn_combobox.blockSignals(True)
            self.pick_region_sh_combobox.blockSignals(True)
            self.pick_region_xn_combobox.blockSignals(True)
            self.data_source_lv1_combobox.blockSignals(True)
            self.data_source_lv2_combobox.blockSignals(True)
            self.data_source_lv3_combobox.blockSignals(True)
            self.dataset_name_combobox.blockSignals(True)
            self.base_map_combobox.blockSignals(True)
            dataset_name = self.current_loading_dataset
            dataset_path = self.dataset_path / f'{dataset_name}.xml'
            dataset_html_2d = self.dataset_path / f'{dataset_name}_2d.html'
            dataset_html_3d = self.dataset_path / f'{dataset_name}_3d.html'
            self.exists_dataset = self.exists_dataset[self.exists_dataset['datasetname'] != dataset_name]
            self.range_type_combobox.setCurrentIndex(-1)
            self.pick_region_sn_combobox.setCurrentIndex(-1)
            self.pick_region_sn_combobox.setEnabled(False)
            self.pick_region_sh_combobox.setCurrentIndex(-1)
            self.pick_region_sh_combobox.setEnabled(False)
            self.pick_region_xn_combobox.setCurrentIndex(-1)
            self.pick_region_xn_combobox.setEnabled(False)
            self.dataset_name_combobox.setCurrentIndex(-1)
            self.data_source_lv1_combobox.setCurrentIndex(-1)
            self.data_source_lv2_combobox.setCurrentIndex(-1)
            self.data_source_lv3_combobox.setCurrentIndex(-1)
            self.base_map_combobox.setCurrentIndex(0)
            self.start_date_lineEdit.setText('')
            self.end_date_lineEdit.setText('')
            self.webEngineView.load(QUrl.fromLocalFile(self.temp_html_file))
            self.lon.setText("经度：96.951170")
            self.lat.setText("纬度：36.452220")
            self.zoom.setText("层级：4")
            self.dataset_name_combobox_update_items()
            self.current_loading_dataset = None
            self.range_type_combobox.blockSignals(False)
            self.pick_region_sn_combobox.blockSignals(False)
            self.pick_region_sh_combobox.blockSignals(False)
            self.pick_region_xn_combobox.blockSignals(False)
            self.data_source_lv1_combobox.blockSignals(False)
            self.data_source_lv2_combobox.blockSignals(False)
            self.data_source_lv3_combobox.blockSignals(False)
            self.dataset_name_combobox.update_placeholdertext()
            self.dataset_name_combobox.blockSignals(False)
            self.base_map_combobox.blockSignals(False)
            os.remove(dataset_path)
            os.remove(dataset_html_2d)
            os.remove(dataset_html_3d)
        except:
            pass

    @Slot()
    def on_new_dataset_button_clicked(self):
        self.dataset_name_combobox.blockSignals(True)
        self.range_type_combobox.setCurrentIndex(-1)
        self.range_type_combobox.setEnabled(True)
        self.pick_region_sn_combobox.setCurrentIndex(-1)
        self.pick_region_sn_combobox.setEnabled(False)
        self.pick_region_sh_combobox.setCurrentIndex(-1)
        self.pick_region_sh_combobox.setEnabled(False)
        self.pick_region_xn_combobox.setCurrentIndex(-1)
        self.pick_region_xn_combobox.setEnabled(False)
        self.dataset_name_combobox.setCurrentIndex(-1)
        self.current_loading_dataset = None
        self.webEngineView.load(QUrl.fromLocalFile(self.temp_html_file))
        self.lon.setText("经度：96.951170")
        self.lat.setText("纬度：36.452220")
        self.zoom.setText("层级：4")
        self.lon.show()
        self.lat.show()
        self.zoom.show()
        self.data_source_lv1_combobox.blockSignals(True)
        self.data_source_lv2_combobox.blockSignals(True)
        self.data_source_lv3_combobox.blockSignals(True)
        self.data_source_lv1_combobox.setCurrentIndex(-1)
        self.data_source_lv1_combobox.setEnabled(True)
        self.data_source_lv2_combobox.setCurrentIndex(-1)
        self.data_source_lv2_combobox.setEnabled(False)
        self.data_source_lv3_combobox.setCurrentIndex(-1)
        self.data_source_lv3_combobox.setEnabled(False)
        self.bands_combobox.clear()
        self.data_source_lv1_combobox.blockSignals(False)
        self.data_source_lv2_combobox.blockSignals(False)
        self.data_source_lv3_combobox.blockSignals(False)
        self.delete_dataset_button.setEnabled(False)
        self.dataset_name_combobox.blockSignals(False)
        self.load_html_timeout_timer.start(self.settings['loading_html_timeout_ms'])

    def get_regionname(self, datarangetype, picked_sn, picked_sh, picked_xn, adcode_picked_region_draw):
        if datarangetype == 0 or datarangetype == 1:
            regionname = '用户绘制区域'
        elif datarangetype == 2:
            cur = self.connection.cursor()
            province_name = cur.execute(f"select name from st_r_sn where adcode = '{str(picked_sn)}'")
            regionname = province_name.fetchone()[0]
            if not numpy.isnan(picked_sh):
                city_name = cur.execute(f"select name from st_r_sh where adcode = '{str(picked_sh)}'")
                regionname = regionname + city_name.fetchone()[0]
            if not numpy.isnan(picked_xn):
                country_name = cur.execute(f"select name from st_r_xn where adcode = '{str(picked_xn)}'")
                regionname = regionname + country_name.fetchone()[0]
            cur.close()
        elif datarangetype == 3:
            cur = self.connection.cursor()
            placeholders = ', '.join(['?'] * len(adcode_picked_region_draw))
            regionname_list = cur.execute(f"select group_concat(case when a.parent like '%0000' then d.name||a.name "
                                          f"else c.name||b.name||a.name end, '\n            ') "
                                          f"from (select * from st_r_xn union select * from st_r_sh) a left join st_r_sh b on a.parent=b.adcode "
                                          f"left join st_r_sn c on b.parent=c.adcode left join st_r_sn d on a.parent=d.adcode "
                                          f"where a.adcode in ({placeholders})",
                                          adcode_picked_region_draw).fetchall()[0]
            regionname = regionname_list[0]
            cur.close()
        elif datarangetype == 4:
            regionname = '用户自定义区域'
        else:
            regionname = '未知'
        return regionname

    def get_datainfo_url(self, ee_object):
        try:
            cur = self.connection.cursor()
            res = cur.execute(f"select url from ee_datacatalog where abbreviation = '{ee_object}' limit 1").fetchone()[0]
            if "hl=zh-cn" not in res:
                res = res + '?hl=zh-cn'
            cur.close()
            return res
        except:
            return 'https://developers.google.com/earth-engine/datasets'

    @Slot()
    def on_pick_region_sn_combobox_currentTextChanged(self):
        try:
            data = self.pick_region_sn_combobox_completer.get_filtered_list()
            if len(data) == 1:
                name = data[0]
                adcode = self.pick_region_sn_combobox.itemData(self.pick_region_sn_combobox.findText(name))
                if adcode is not None:
                    geometry = self.get_region('st_r_sn', adcode=adcode)[0][0]
                    geometry_without_holes = self.get_geometry_without_holes(geometry)
                    self.webEngineView_load_geometry(geometry_without_holes)
        except:
            pass

    @Slot()
    def on_pick_region_sn_combobox_linedit_editingFinished(self):
        try:
            data = self.pick_region_sn_combobox_completer.get_filtered_list()
            if len(data) == 1:
                name = data[0]
                index = self.pick_region_sn_combobox.findText(name)
                adcode = self.pick_region_sn_combobox.itemData(index)
                if adcode is not None:
                    self.pick_region_sn_combobox.setCurrentIndex(index)
        except:
            pass

    @Slot(int)
    def on_pick_region_sn_combobox_currentIndexChanged(self, index):
        self.pick_region_sh_combobox.clear()
        self.pick_region_xn_combobox.clear()
        self.pick_region_sh_combobox.setEnabled(True)
        self.pick_region_xn_combobox.setEnabled(False)
        name = self.pick_region_sn_combobox.currentText()
        adcode = self.pick_region_sn_combobox.itemData(index)  # 获取唯一标识符
        if adcode is not None:
            geometry = self.get_region('st_r_sn', adcode=adcode)[0][0]
            geometry_without_holes = self.get_geometry_without_holes(geometry)
            self.picked_region = ('st_r_sn', name, adcode, geometry_without_holes)
            self.webEngineView_load_geometry(geometry_without_holes)
        sh_result = self.get_region(region='st_r_sh', parcode=adcode)
        pick_region_sh_combobox_items = []
        for name, adcode in sh_result:
            self.pick_region_sh_combobox.addItem(name)
            self.pick_region_sh_combobox.setItemData(self.pick_region_sh_combobox.count() - 1, adcode)
            pick_region_sh_combobox_items.append(name)
        self.pick_region_sh_combobox_completer = customwidget.CustomCompleter(pick_region_sh_combobox_items)
        self.pick_region_sh_combobox.setCompleter(self.pick_region_sh_combobox_completer)
        self.pick_region_sh_combobox.setCurrentIndex(-1)
        self.dataset_name_combobox.update_placeholdertext()

    @Slot()
    def on_pick_region_sh_combobox_currentTextChanged(self):
        try:
            data = self.pick_region_sh_combobox_completer.get_filtered_list()
            if len(data) == 1:
                name = data[0]
                adcode = self.pick_region_sh_combobox.itemData(self.pick_region_sh_combobox.findText(name))
                if adcode is not None:
                    geometry = self.get_region('st_r_sh', adcode=adcode)[0][0]
                    geometry_without_holes = self.get_geometry_without_holes(geometry)
                    self.webEngineView_load_geometry(geometry_without_holes)
        except:
            pass

    @Slot()
    def on_pick_region_sh_combobox_linedit_editingFinished(self):
        try:
            data = self.pick_region_sh_combobox_completer.get_filtered_list()
            if len(data) == 1:
                name = data[0]
                index = self.pick_region_sh_combobox.findText(name)
                adcode = self.pick_region_sh_combobox.itemData(index)
                if adcode is not None:
                    self.pick_region_sh_combobox.setCurrentIndex(index)
        except:
            pass

    @Slot(int)
    def on_pick_region_sh_combobox_currentIndexChanged(self, index):
        self.pick_region_xn_combobox.clear()
        self.pick_region_xn_combobox.setEnabled(True)
        name = self.pick_region_sh_combobox.currentText()
        adcode = self.pick_region_sh_combobox.itemData(index)  # 获取唯一标识符
        if adcode is not None:
            geometry = self.get_region('st_r_sh', adcode=adcode)[0][0]
            geometry_without_holes = self.get_geometry_without_holes(geometry)
            self.picked_region = ('st_r_sh', name, adcode, geometry_without_holes)
            self.webEngineView_load_geometry(geometry_without_holes)
        xn_result = self.get_region(region='st_r_xn', parcode=adcode)
        pick_region_xn_combobox_items = []
        for name, adcode in xn_result:
            self.pick_region_xn_combobox.addItem(name)
            self.pick_region_xn_combobox.setItemData(self.pick_region_xn_combobox.count() - 1, adcode)
            pick_region_xn_combobox_items.append(name)
        self.pick_region_xn_combobox_completer = customwidget.CustomCompleter(pick_region_xn_combobox_items)
        self.pick_region_xn_combobox.setCompleter(self.pick_region_xn_combobox_completer)
        self.pick_region_xn_combobox.setCurrentIndex(-1)
        self.dataset_name_combobox.update_placeholdertext()

    @Slot()
    def on_pick_region_xn_combobox_currentTextChanged(self):
        try:
            data = self.pick_region_xn_combobox_completer.get_filtered_list()
            if len(data) == 1:
                name = data[0]
                adcode = self.pick_region_xn_combobox.itemData(self.pick_region_xn_combobox.findText(name))
                if adcode is not None:
                    geometry = self.get_region('st_r_xn', adcode=adcode)[0][0]
                    geometry_without_holes = self.get_geometry_without_holes(geometry)
                    self.webEngineView_load_geometry(geometry_without_holes)
        except:
            pass

    @Slot()
    def on_pick_region_xn_combobox_linedit_editingFinished(self):
        try:
            data = self.pick_region_xn_combobox_completer.get_filtered_list()
            if len(data) == 1:
                name = data[0]
                index = self.pick_region_xn_combobox.findText(name)
                adcode = self.pick_region_xn_combobox.itemData(index)
                if adcode is not None:
                    self.pick_region_xn_combobox.setCurrentIndex(index)
        except:
            pass

    @Slot(int)
    def on_pick_region_xn_combobox_currentIndexChanged(self, index):
        name = self.pick_region_xn_combobox.currentText()
        adcode = self.pick_region_xn_combobox.itemData(index)  # 获取唯一标识符
        if adcode is not None:
            geometry = self.get_region('st_r_xn', adcode=adcode)[0][0]
            geometry_without_holes = self.get_geometry_without_holes(geometry)
            self.picked_region = ('st_r_xn', name, adcode, geometry_without_holes)
            self.webEngineView_load_geometry(geometry_without_holes)
        self.dataset_name_combobox.update_placeholdertext()

    @staticmethod
    def find_index_by_itemdata(combobox, target_data):
        for index in range(combobox.count()):
            if combobox.itemData(index) == target_data:
                return index
        return -1

    @staticmethod
    def get_geometry_without_holes(wkt):
        geom = make_valid(loads(wkt))
        if isinstance(geom, GeometryCollection):
            valid_geoms = [geom for geom in geom.geoms if isinstance(geom, (Polygon, MultiPolygon))]
            geom = unary_union(valid_geoms)
        geometry_without_holes = MultiPolygon()
        if isinstance(geom, Polygon):
            geometry_without_holes = MultiPolygon(list(geometry_without_holes.geoms) + [geom])
        elif isinstance(geom, MultiPolygon):
            if isinstance(unary_union(geom.geoms), Polygon):
                geometry_without_holes = MultiPolygon(list(geometry_without_holes.geoms) + [unary_union(geom.geoms)])
            elif isinstance(unary_union(geom.geoms), MultiPolygon):
                geometry_without_holes = MultiPolygon(list(geometry_without_holes.geoms) + list(unary_union(geom.geoms).geoms))
        return dumps(geometry_without_holes)

    @Slot(int)
    def on_range_type_combobox_currentIndexChanged(self, index):
        if index == 0:
            self.webengine_loaded_geometry = None
            self.customregion_downloadwidget.hide()
            self.picked_region_draw_timer.stop()
            self.custom_region = None
            self.end_listening_js = True
            self.polygon_from_js['polygon'] = None
            self.picked_region_draw = None
            self.picked_region = (None, None, None, None)
            self.webEngineView.page().runJavaScript("closeDrawTool()")
            self.webEngineView.page().runJavaScript("openRectangleTool()")
            self.pick_region_sn_combobox.clear()
            self.pick_region_sh_combobox.clear()
            self.pick_region_xn_combobox.clear()
            self.pick_region_sn_combobox.setEnabled(False)
            self.pick_region_sh_combobox.setEnabled(False)
            self.pick_region_xn_combobox.setEnabled(False)
            self.draw_polygon_tips_label.setVisible(True)
            self.draw_polygon_tips_label.setText('请在图上绘制下载范围')
            self.custom_region = None
            self.dataset_name_combobox.update_placeholdertext()
        elif index == 1:
            self.webengine_loaded_geometry = None
            self.customregion_downloadwidget.hide()
            self.picked_region_draw_timer.stop()
            self.custom_region = None
            self.end_listening_js = True
            self.polygon_from_js['polygon'] = None
            self.picked_region_draw = None
            self.picked_region = (None, None, None, None)
            self.webEngineView.page().runJavaScript("closeDrawTool()")
            self.webEngineView.page().runJavaScript("openPolygonTool()")
            self.pick_region_sn_combobox.clear()
            self.pick_region_sh_combobox.clear()
            self.pick_region_xn_combobox.clear()
            self.pick_region_sn_combobox.setEnabled(False)
            self.pick_region_sh_combobox.setEnabled(False)
            self.pick_region_xn_combobox.setEnabled(False)
            self.draw_polygon_tips_label.setVisible(True)
            self.draw_polygon_tips_label.setText('请在图上绘制下载范围')
            self.custom_region = None
            self.dataset_name_combobox.update_placeholdertext()
        elif index == 2:
            self.webengine_loaded_geometry = None
            self.customregion_downloadwidget.hide()
            self.picked_region_draw_timer.stop()
            self.custom_region = None
            self.end_listening_js = True
            self.polygon_from_js['polygon'] = None
            self.picked_region_draw = None
            self.picked_region = (None, None, None, None)
            self.webEngineView.page().runJavaScript("removeOverlay()")
            self.webEngineView.page().runJavaScript("closeDrawTool()")
            self.draw_polygon_tips_label.setVisible(False)
            self.pick_region_sn_combobox.clear()
            self.pick_region_sh_combobox.clear()
            self.pick_region_xn_combobox.clear()
            self.pick_region_sn_combobox.setEnabled(True)
            self.pick_region_sh_combobox.setEnabled(False)
            self.pick_region_xn_combobox.setEnabled(False)
            sn_result = self.get_region(region='st_r_sn')
            pick_region_sn_combobox_items = []
            for name, adcode in sn_result:
                self.pick_region_sn_combobox.addItem(name)
                self.pick_region_sn_combobox.setItemData(self.pick_region_sn_combobox.count() - 1, adcode)
                pick_region_sn_combobox_items.append(name)
            self.pick_region_sn_combobox_completer = customwidget.CustomCompleter(pick_region_sn_combobox_items)
            self.pick_region_sn_combobox.setCompleter(self.pick_region_sn_combobox_completer)
            self.pick_region_sn_combobox.setCurrentIndex(-1)
            self.dataset_name_combobox.update_placeholdertext()
        elif index == 3:
            self.webengine_loaded_geometry = None
            self.customregion_downloadwidget.hide()
            self.picked_region_draw_timer.stop()
            self.end_listening_js = False
            self.webEngineView.page().runJavaScript("closeDrawTool()")
            self.webEngineView.page().runJavaScript("openPolygonTool()")
            self.pick_region_sn_combobox.clear()
            self.pick_region_sh_combobox.clear()
            self.pick_region_xn_combobox.clear()
            self.pick_region_sn_combobox.setEnabled(False)
            self.pick_region_sh_combobox.setEnabled(False)
            self.pick_region_xn_combobox.setEnabled(False)
            self.draw_polygon_tips_label.show()
            self.draw_polygon_tips_label.setText('绘制大概范围后<br>系统会自动选取涉及的区县')
            self.picked_region_draw_timer.start(50)
            self.dataset_name_combobox.update_placeholdertext()
        elif index == 4:
            self.webengine_loaded_geometry = None
            self.picked_region_draw_timer.stop()
            self.draw_polygon_tips_label.hide()
            self.webEngineView.page().runJavaScript("removeOverlay()")
            self.webEngineView.page().runJavaScript("closeDrawTool()")
            self.custom_region = None
            self.polygon_from_js['polygon'] = None
            self.end_listening_js = True
            self.picked_region_draw = None
            self.picked_region = (None, None, None, None)
            self.customregion_downloadwidget.show()
            self.pick_region_sn_combobox.clear()
            self.pick_region_sh_combobox.clear()
            self.pick_region_xn_combobox.clear()
            self.dataset_name_combobox.update_placeholdertext()

    @Slot()
    def on_dataset_name_combobox_currentIndexChanged(self):
        try:
            self.base_map_combobox.blockSignals(True)
            # self.range_type_combobox.blockSignals(True)
            self.draw_polygon_tips_label.hide()
            self.view_data_button.setText('')
            self.view_data_button.setIcon(QIcon())
            self.view_data_movie.setScaledSize(QSize(self.view_data_button.height(), self.view_data_button.height()))
            self.view_data_button_movie_label.show()
            self.view_data_movie.start()
            self.current_loading_dataset = self.dataset_name_combobox.lineEdit().text()
            result = self.exists_dataset[self.exists_dataset['datasetname'] == self.dataset_name_combobox.lineEdit().text()]
            self.lon.hide()
            self.lat.hide()
            self.zoom.hide()
            try:
                self.start_date_lineEdit.setText(result.iloc[0]['start_date'])
            except:
                self.start_date_lineEdit.setText('')
            try:
                self.end_date_lineEdit.setText(result.iloc[0]['end_date'])
            except:
                self.end_date_lineEdit.setText('')
            self.base_map_combobox.setCurrentText(result.iloc[0]['base_map'])
            self.data_source_lv1_combobox.setCurrentIndex(self.data_source_lv1_combobox.findText(result.iloc[0]['data_source_lv1']))
            self.data_source_lv2_combobox.setCurrentIndex(self.data_source_lv2_combobox.findText(result.iloc[0]['data_source_lv2']))
            self.data_source_lv3_combobox.setCurrentIndex(self.data_source_lv3_combobox.findText(result.iloc[0]['data_source_lv3']))
            self.current_loading_datasource = self.data_source_lv3_combobox.lineEdit().text()
            while True:
                try:
                    has_3d = self.dataset_info[self.current_loading_datasource]['has_3d']
                    break
                except:
                    time.sleep(0.01)
            if self.settings['default_map_visualize'] == '3d' and has_3d:
                self.current_loading_visualize = '3d'
                self.webEngineView.load(QUrl.fromLocalFile(str(self.dataset_path) + f'/{self.current_loading_dataset}_3d.html'))
            else:
                self.current_loading_visualize = '2d'
                self.webEngineView.load(QUrl.fromLocalFile(str(self.dataset_path) + f'/{self.current_loading_dataset}_2d.html'))
            self.range_type_combobox.setCurrentIndex(result.iloc[0]['datarangetype'])
            self.data_scale_combobox.setCurrentText(str(int(result.iloc[0]['data_scale'])))
            try:
                bands = tuple(map(str, result.iloc[0]['bands'].split(',')))
            except Exception as e:
                bands = None
            self.bands_combobox.select_items(bands)
            if result.iloc[0]['datarangetype'] == 0 or result.iloc[0]['datarangetype'] == 1:
                self.polygon_from_js['polygon'] = result.iloc[0]['datasetregion']
            elif result.iloc[0]['datarangetype'] == 2:
                index = self.find_index_by_itemdata(self.pick_region_sn_combobox, str(result.iloc[0]['picked_sn']))
                self.pick_region_sn_combobox.setCurrentIndex(index)
                if not numpy.isnan(result.iloc[0]['picked_sh']):
                    index = self.find_index_by_itemdata(self.pick_region_sh_combobox, str(result.iloc[0]['picked_sh']))
                    self.pick_region_sh_combobox.setCurrentIndex(index)
                    if not numpy.isnan(result.iloc[0]['picked_xn']):
                        index = self.find_index_by_itemdata(self.pick_region_xn_combobox, str(result.iloc[0]['picked_xn']))
                        self.pick_region_xn_combobox.setCurrentIndex(index)
            elif result.iloc[0]['datarangetype'] == 3:
                self.picked_region_draw = result.iloc[0]['datasetregion']
            elif result.iloc[0]['datarangetype'] == 4:
                self.custom_region = result.iloc[0]['datasetregion']
                self.customregion_downloadwidget.hide()
            self.range_type_combobox.blockSignals(True)
            self.data_source_lv1_combobox.blockSignals(True)
            self.disable_creategeemapwidget_widgets()
            self.draw_polygon_tips_label.hide()
            self.range_type_combobox.blockSignals(False)
            self.data_source_lv1_combobox.blockSignals(False)
            self.range_type_combobox.blockSignals(False)
            self.data_source_lv1_combobox.lineEdit().setCursorPosition(0)
            self.data_source_lv2_combobox.lineEdit().setCursorPosition(0)
            self.data_source_lv3_combobox.lineEdit().setCursorPosition(0)
        except Exception as e:
            traceback.print_exc()
        self.load_html_timeout_timer.start(self.settings['loading_html_timeout_ms'])
        self.base_map_combobox.blockSignals(False)

    @Slot(int)
    def on_data_source_lv1_combobox_currentIndexChanged(self, index):
        self.data_source_lv2_combobox.clear()
        self.bands_combobox.clear()
        self.data_source_lv2_combobox.setEnabled(True)
        lv1_id = self.data_source_lv1_combobox.itemData(index)  # 获取唯一标识符
        lv2_result, tooltip_dict = self.get_original_datasource(level=2, parent=lv1_id)
        pick_data_source_lv2_combobox_items = []
        for abbreviation, data_id, notes in lv2_result:
            self.data_source_lv2_combobox.addItem(abbreviation)
            self.data_source_lv2_combobox.setItemData(self.data_source_lv2_combobox.count() - 1, data_id)
            pick_data_source_lv2_combobox_items.append(abbreviation)
        self.data_source_lv2_combobox.setTootipDict(tooltip_dict)
        self.data_source_lv2_combobox_completer = customwidget.CustomCompleter(pick_data_source_lv2_combobox_items)
        self.data_source_lv2_combobox.setCompleter(self.data_source_lv2_combobox_completer)
        self.data_source_lv2_combobox.setCurrentIndex(-1)
        # self.data_source_lv3_combobox.setEnabled(False)
        self.data_source_lv1_combobox.lineEdit().setCursorPosition(0)

    @Slot(int)
    def on_data_source_lv2_combobox_currentIndexChanged(self, index):
        self.data_source_lv3_combobox.clear()
        self.bands_combobox.clear()
        self.data_source_lv3_combobox.setEnabled(True)
        lv2_id = self.data_source_lv2_combobox.itemData(index)  # 获取唯一标识符
        lv3_result, tooltip_dict = self.get_original_datasource(level=3, parent=lv2_id)
        pick_data_source_lv3_combobox_items = []
        self.data_source_lv3_combobox.blockSignals(True)
        for abbreviation, data_id, notes in lv3_result:
            self.data_source_lv3_combobox.addItem(abbreviation)
            self.data_source_lv3_combobox.setItemData(self.data_source_lv3_combobox.count() - 1, data_id)
            pick_data_source_lv3_combobox_items.append(abbreviation)
        self.data_source_lv3_combobox.setTootipDict(tooltip_dict)
        self.data_source_lv3_combobox_completer = customwidget.CustomCompleter(pick_data_source_lv3_combobox_items)
        self.data_source_lv3_combobox.setCompleter(self.data_source_lv3_combobox_completer)
        self.data_source_lv3_combobox.setCurrentIndex(-1)
        self.data_source_lv3_combobox.blockSignals(False)
        self.data_source_lv2_combobox.lineEdit().setCursorPosition(0)

    @Slot()
    def on_data_source_lv3_combobox_currentIndexChanged(self):
        self.bands_combobox.clear()
        datasetname = self.data_source_lv3_combobox.lineEdit().text()
        get_dataset_info_thread = threading.Thread(target=self.get_dataset_info, args=(datasetname,))
        get_dataset_info_thread.start()
        self.data_source_lv3_combobox.lineEdit().setCursorPosition(0)

    def loading_html_timeout(self):
        try:
            self.load_html_timeout_timer.stop()
        except:
            pass
        if not self.dataset_html_loaded and self.current_loading_dataset is not None:
            self.view_data_button_movie_label.hide()
            self.view_data_movie.stop()
            self.view_data_button.setText('预览数据集')
            self.view_data_button.setIcon(QIcon('resources/icon/view_dataset.png'))
            self.disable_creategeemapwidget_widgets(False)
            self.data_source_lv1_combobox.setEnabled(False)
            self.data_source_lv2_combobox.setEnabled(False)
            self.data_source_lv3_combobox.setEnabled(False)
            self.range_type_combobox.setEnabled(False)
            self.pick_region_sn_combobox.setEnabled(False)
            self.pick_region_sh_combobox.setEnabled(False)
            self.pick_region_xn_combobox.setEnabled(False)
            if self.dataset_info[self.data_source_lv3_combobox.currentText()]['need_scale']:
                self.data_scale_combobox.setEnabled(False)
            else:
                self.data_scale_combobox.setEnabled(True)
            if self.dataset_info[self.data_source_lv3_combobox.currentText()]['need_date'] == 'no':
                self.start_date_lineEdit.setEnabled(False)
                self.start_date_calendar_button.setEnabled(False)
                self.end_date_lineEdit.setEnabled(False)
                self.end_date_calendar_button.setEnabled(False)
            else:
                self.start_date_lineEdit.setEnabled(True)
                self.start_date_calendar_button.setEnabled(True)
                self.end_date_lineEdit.setEnabled(True)
                self.end_date_calendar_button.setEnabled(True)
        elif not self.dataset_html_loaded and self.current_loading_dataset is None:
            self.view_data_button_movie_label.hide()
            self.view_data_movie.stop()
            self.view_data_button.setText('预览数据集')
            self.view_data_button.setIcon(QIcon('resources/icon/view_dataset.png'))
            self.data_source_lv1_combobox.blockSignals(True)
            self.data_source_lv2_combobox.blockSignals(True)
            self.data_source_lv3_combobox.blockSignals(True)
            self.range_type_combobox.blockSignals(True)
            self.pick_region_sn_combobox.blockSignals(True)
            self.pick_region_sh_combobox.blockSignals(True)
            self.pick_region_xn_combobox.blockSignals(True)
            self.disable_creategeemapwidget_widgets(False)
            self.data_source_lv1_combobox.setEnabled(True)
            self.data_source_lv1_combobox.setCurrentIndex(-1)
            self.data_source_lv2_combobox.setEnabled(False)
            self.data_source_lv2_combobox.setCurrentIndex(-1)
            self.data_source_lv3_combobox.setEnabled(False)
            self.data_source_lv3_combobox.setCurrentIndex(-1)
            self.range_type_combobox.setEnabled(True)
            self.range_type_combobox.setCurrentIndex(-1)
            self.pick_region_sn_combobox.setEnabled(False)
            self.pick_region_sn_combobox.setCurrentIndex(-1)
            self.pick_region_sh_combobox.setEnabled(False)
            self.pick_region_sh_combobox.setCurrentIndex(-1)
            self.pick_region_xn_combobox.setEnabled(False)
            self.pick_region_xn_combobox.setCurrentIndex(-1)
            self.delete_dataset_button.setEnabled(False)
            self.download_dataset_button.setEnabled(False)
            self.data_source_lv1_combobox.blockSignals(False)
            self.data_source_lv2_combobox.blockSignals(False)
            self.data_source_lv3_combobox.blockSignals(False)
            self.range_type_combobox.blockSignals(False)
            self.pick_region_sn_combobox.blockSignals(False)
            self.pick_region_sh_combobox.blockSignals(False)
            self.pick_region_xn_combobox.blockSignals(False)

    def disable_creategeemapwidget_widgets(self, disabled=True):
        for toolbutton in self.creategeemapwidget.findChildren(QToolButton):
            toolbutton.setDisabled(disabled)
        for pushbutton in self.creategeemapwidget.findChildren(QPushButton):
            pushbutton.setDisabled(disabled)
        if not disabled:
            if self.range_type_combobox.currentIndex() != 2:
                for combobox in self.creategeemapwidget.findChildren(QComboBox):
                    if combobox not in (self.pick_region_sn_combobox, self.pick_region_sh_combobox, self.pick_region_xn_combobox):
                        combobox.setDisabled(disabled)
            else:
                for combobox in self.creategeemapwidget.findChildren(QComboBox):
                    if self.pick_region_sn_combobox.currentIndex() == -1:
                        if combobox in (self.pick_region_sh_combobox, self.pick_region_xn_combobox):
                            combobox.setDisabled(disabled)
                    elif self.pick_region_sn_combobox.currentIndex() != -1 and self.pick_region_sh_combobox.currentIndex() == -1:
                        if combobox != self.pick_region_xn_combobox:
                            combobox.setDisabled(disabled)
                    else:
                        combobox.setDisabled(disabled)
        else:
            for combobox in self.creategeemapwidget.findChildren(QComboBox):
                combobox.setDisabled(disabled)
        for lineEdit in self.creategeemapwidget.findChildren(QLineEdit):
            lineEdit.setDisabled(disabled)

    def get_bands_tooltip(self):
        bands_tooltip_dict = {}
        url = self.get_datainfo_url(self.data_source_lv3_combobox.currentText())
        response = requests.get(url, headers=self.request_headers)
        if response.status_code == 200:
            # 解析 HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find("table", class_="eecat")
            if table:
                headers = []
                thead = table.find("thead")
                if thead:
                    headers = [th.get_text(strip=True) for th in thead.find_all("th")]
                else:
                    # 没有 thead 时，用第一行作为表头
                    first_row = table.find("tr")
                    if first_row:
                        headers = [cell.get_text(strip=True) for cell in first_row.find_all(["th", "td"])]
                # 5. 获取数据行；若存在 <tbody> 则取 tbody 内的 tr，否则取所有 tr
                if table.find("tbody"):
                    rows = table.find("tbody").find_all("tr")
                else:
                    rows = table.find_all("tr")
                # 若第一行内容与 headers 相同，则跳过表头那一行
                if rows and [cell.get_text(strip=True) for cell in rows[0].find_all(["th", "td"])] == headers:
                    rows = rows[1:]
                # 6. 遍历每一行，并构造字典
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) < 2:  # 至少两列：key + value 部分
                        continue
                    # 第一列作为 key
                    key = cells[0].get_text(strip=True)
                    # 对后续列进行聚合，格式为 “表头: 内容”
                    agg_parts = []
                    # 从第二列开始（索引 1）
                    for i, cell in enumerate(cells[1:], start=1):
                        # 如果表头数据足够，则取对应表头；否则按“列1”，“列2”…命名
                        if i < len(headers):
                            header_name = headers[i]
                        else:
                            header_name = f"列{i + 1}"
                        content = cell.get_text(strip=True)
                        agg_parts.append(f"{header_name}: {content}")
                    # 使用 " | " 分隔各列的组合结果
                    value_str = " | ".join(agg_parts)
                    bands_tooltip_dict[key] = value_str
        return bands_tooltip_dict

    def get_dataset_info(self, datasetname):
        if datasetname:
            cur = self.connection.cursor()
            try:
                if datasetname not in self.dataset_info:
                    cur.execute('select need_date, need_scale, bands, has_3d from ee_datacatalog where abbreviation = ?', (datasetname,))
                    need_date, need_scale, bands, has_3d = cur.fetchone()
                    self.dataset_info[datasetname] = {}
                    self.dataset_info[datasetname]['start_date'], self.dataset_info[datasetname]['end_date'] = None, None
                    if bands is not None:
                        bands = tuple(a for a in bands.split(','))
                    else:
                        bands = ()
                    if need_scale == 0:
                        need_scale = False
                    else:
                        need_scale = True
                    if has_3d == 0:
                        has_3d = False
                    else:
                        has_3d = True
                    (self.dataset_info[datasetname]['need_date'], self.dataset_info[datasetname]['need_scale'],
                     self.dataset_info[datasetname]['bands'], self.dataset_info[datasetname]['has_3d']) = need_date, need_scale, bands, has_3d
                    try:
                        cur.execute('select start_date from ee_datacatalog where abbreviation = ?', (datasetname,))
                        start_date = cur.fetchone()[0]
                        if self.dataset_info[datasetname]['end_date'] is None or self.dataset_info[datasetname]['end_date'] == datetime(9999, 12, 31):
                            cur.execute('select url from ee_datacatalog where abbreviation = ?', (datasetname,))
                            url = cur.fetchone()[0]
                            response = requests.get(url, headers=self.request_headers)
                            if response.status_code == 200:
                                soup = BeautifulSoup(response.text, 'html.parser')
                                element = soup.select_one('#main-content > devsite-content > article > div.devsite-article-body.clearfix > div > '
                                                          'div.ee-container > div:nth-child(2) > div > dl.left > dd:nth-child(2)')
                                if element:
                                    start_str, end_str = element.get_text().split('–')
                                    start_date = datetime.strptime(start_str[:10], "%Y-%m-%d").date()
                                    end_date = datetime.strptime(end_str[:10], "%Y-%m-%d").date()
                                else:
                                    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                                    end_date = datetime(9999, 12, 31).date()
                            else:
                                end_date = datetime(9999, 12, 31).date()
                            self.dataset_info[datasetname]['start_date'], self.dataset_info[datasetname]['end_date'] = start_date, end_date
                    except Exception as e:
                        start_date, end_date = datetime(1000, 12, 31).date(), datetime(9999, 12, 31).date()
                        self.dataset_info[datasetname]['start_date'], self.dataset_info[datasetname]['end_date'] = start_date, end_date
                    print(self.dataset_info)
                else:
                    need_date, need_scale, bands, has_3d = (self.dataset_info[datasetname]['need_date'], self.dataset_info[datasetname]['need_scale'],
                                                            self.dataset_info[datasetname]['bands'], self.dataset_info[datasetname]['has_3d'])
                if need_date != 'no':
                    if self.dataset_info[datasetname]['end_date'] - timedelta(days=31) > self.dataset_info[datasetname]['start_date']:
                        self.start_date_lineEdit.setPlaceholderText(str(self.dataset_info[datasetname]['end_date'] - timedelta(days=31)))
                    else:
                        self.end_date_lineEdit.setPlaceholderText(str(self.dataset_info[datasetname]['start_date']))
                    self.end_date_lineEdit.setPlaceholderText(str(self.dataset_info[datasetname]['end_date']))
                    self.start_date_calendar.setMinimumDate(self.dataset_info[datasetname]['start_date'])
                    self.start_date_calendar.setMaximumDate(self.dataset_info[datasetname]['end_date'])
                    self.start_date_calendar_button.setEnabled(True)
                    self.start_date_lineEdit.setEnabled(True)
                    self.end_date_calendar.setMinimumDate(self.dataset_info[datasetname]['start_date'])
                    self.end_date_calendar.setMaximumDate(self.dataset_info[datasetname]['end_date'])
                    self.end_date_calendar_button.setEnabled(True)
                    self.end_date_lineEdit.setEnabled(True)
                else:
                    self.start_date_lineEdit.setEnabled(False)
                    self.start_date_calendar.hide()
                    self.start_date_calendar_button.setEnabled(False)
                    self.end_date_lineEdit.setEnabled(False)
                    self.end_date_calendar.hide()
                    self.end_date_calendar_button.setEnabled(False)
                if len(bands) > 0:
                    self.bands_combobox.setEnabled(True)
                    self.bands_combobox.clear()
                    self.bands_combobox.addItems([band for band in bands])
                    bands_tooltip_dict = self.get_bands_tooltip()
                    self.bands_combobox.set_tooltip_dict(bands_tooltip_dict)
                    # self.bands_combobox.select_all()
                else:
                    self.bands_combobox.clear()
                    self.bands_combobox.setCurrentIndex(-1)
                    self.bands_combobox.setEnabled(False)
                if need_scale:
                    self.data_scale_combobox.setEnabled(True)
                else:
                    self.data_scale_combobox.setEnabled(False)
            except Exception as e:
                pass
            finally:
                cur.close()

    def dataset_name_combobox_update_items(self):
        try:
            self.dataset_name_combobox.blockSignals(True)
            self.dataset_name_combobox.clear()
            items = self.exists_dataset['datasetname'].tolist()
            self.dataset_name_combobox.addItems(items)
            self.dataset_name_combobox_completer = customwidget.CustomCompleter(items)
            self.dataset_name_combobox.setCompleter(self.dataset_name_combobox_completer)
            self.dataset_name_combobox.setCurrentIndex(-1)
            self.dataset_name_combobox.blockSignals(False)
        except:
            pass

    def get_original_datasource(self, parent, level):
        cur = self.connection.cursor()
        results = cur.execute('select distinct abbreviation,id,notes from (select c.id, c.abbreviation,c.level,c.parent,c.notes from ee_datacatalog a '
                              'left join ee_datacatalog b on a.parent=b.id left join ee_datacatalog c on b.parent=c.id where a.status = 1 and a.level = 3 '
                              'union select b.id, b.abbreviation,b.level,b.parent,b.notes from ee_datacatalog a left join ee_datacatalog b on a.parent=b.id '
                              'left join ee_datacatalog c on b.parent=c.id where a.status = 1 and a.level = 3 union '
                              'select a.id, a.abbreviation,a.level,a.parent,a.notes from ee_datacatalog a left join ee_datacatalog b on a.parent=b.id '
                              'left join ee_datacatalog c on b.parent=c.id where a.status = 1 and a.level = 3)where parent = ? and level = ?',
                              (parent, level)).fetchall()
        tooltip_dict = {row[0]: row[2] for row in results}
        cur.close()
        return results, tooltip_dict

    def get_region(self, region, parcode=None, adcode=None):
        try:
            if adcode is None:
                if region == 'st_r_sn':
                    result = self.geo_sn[['name', 'adcode']].values.tolist()
                elif region == 'st_r_sh':
                    result = self.geo_sh.loc[self.geo_sh['parent'] == parcode, ['name', 'adcode']].values.tolist()
                elif region == 'st_r_xn':
                    result = self.geo_xn.loc[self.geo_xn['parent'] == parcode, ['name', 'adcode']].values.tolist()
            else:
                if region == 'st_r_sn':
                    result = [self.geo_sn.loc[self.geo_sn['adcode'] == adcode, 'geometry'].values.tolist()]
                elif region == 'st_r_sh':
                    result = [self.geo_sh.loc[self.geo_sh['adcode'] == adcode, 'geometry'].values.tolist()]
                elif region == 'st_r_xn':
                    result = [self.geo_xn.loc[self.geo_xn['adcode'] == adcode, 'geometry'].values.tolist()]
            return result
        except Exception as e:
            cur = self.connection.cursor()
            if adcode is None:
                sql = f'select name, adcode from {region} where parent= case when ? then ? else parent end order by adcode'
                cur.execute(sql, (parcode, parcode))
            else:
                sql = f'select geometry from {region} where adcode=?'
                cur.execute(sql, (adcode,))
            result = cur.fetchall()
            cur.close()
            return result

    def get_default_datasetname(self):
        count = [f"{i:02}" for i in range(1, 100)]
        if self.range_type_combobox.currentIndex() == 0:
            datasetname_list = [f'RectDataset_{item}' for item in count]
            datasetname_dataframe = pd.DataFrame(datasetname_list, columns=['default_datasetname'])
            useable_taskname = datasetname_dataframe[~datasetname_dataframe['default_datasetname'].isin(self.exists_dataset['datasetname'])]
            try:
                datasetname = useable_taskname.sort_values(by='default_datasetname').iloc[0]['default_datasetname']
            except:
                datasetname = f'RectDataset_{self.new_id()}'
            return datasetname
        elif self.range_type_combobox.currentIndex() == 1:
            datasetname_list = [f'PolygonDataset_{item}' for item in count]
            datasetname_dataframe = pd.DataFrame(datasetname_list, columns=['default_datasetname'])
            useable_taskname = datasetname_dataframe[~datasetname_dataframe['default_datasetname'].isin(self.exists_dataset['datasetname'])]
            try:
                datasetname = useable_taskname.sort_values(by='default_datasetname').iloc[0]['default_datasetname']
            except:
                datasetname = f'PolygonDataset_{self.new_id()}'
            return datasetname
        elif (self.range_type_combobox.currentIndex() == 2 and
              (self.pick_region_sn_combobox.currentIndex() != -1 or self.pick_region_sh_combobox.currentIndex() != -1 or
               self.pick_region_xn_combobox.currentIndex() != -1)):
            if self.pick_region_xn_combobox.currentIndex() != -1:
                region = f"{''.join(lazy_pinyin(self.pick_region_xn_combobox.currentText()[:2]))}".capitalize()
            elif self.pick_region_sh_combobox.currentIndex() != -1:
                region = f"{''.join(lazy_pinyin(self.pick_region_sh_combobox.currentText()[:2]))}".capitalize()
            elif self.pick_region_sn_combobox.currentIndex() != -1:
                region = f"{''.join(lazy_pinyin(self.pick_region_sn_combobox.currentText()[:2]))}".capitalize()
            datasetname_list = [f'{region}_{item}' for item in count]
            datasetname_dataframe = pd.DataFrame(datasetname_list, columns=['default_datasetname'])
            useable_taskname = datasetname_dataframe[~datasetname_dataframe['default_datasetname'].isin(self.exists_dataset['datasetname'])]
            try:
                datasetname = useable_taskname.sort_values(by='default_datasetname').iloc[0]['default_datasetname']
            except:
                datasetname = f'RegionDataset_{self.new_id()}'
            return datasetname
        elif self.range_type_combobox.currentIndex() == 3:
            datasetname_list = [f'PickDrawDataset_{item}' for item in count]
            datasetname_dataframe = pd.DataFrame(datasetname_list, columns=['default_datasetname'])
            useable_taskname = datasetname_dataframe[~datasetname_dataframe['default_datasetname'].isin(self.exists_dataset['datasetname'])]
            try:
                datasetname = useable_taskname.sort_values(by='default_datasetname').iloc[0]['default_datasetname']
            except:
                datasetname = f'RegionDrawDataset_{self.new_id()}'
            return datasetname
        elif self.range_type_combobox.currentIndex() == 4:
            datasetname_list = [f'CustomDataset_{item}' for item in count]
            datasetname_dataframe = pd.DataFrame(datasetname_list, columns=['default_datasetname'])
            useable_taskname = datasetname_dataframe[~datasetname_dataframe['default_datasetname'].isin(self.exists_dataset['datasetname'])]
            try:
                datasetname = useable_taskname.sort_values(by='default_datasetname').iloc[0]['default_datasetname']
            except:
                datasetname = f'CustomDataset_{self.new_id()}'
            return datasetname
        else:
            datasetname = ''
        return datasetname

    def is_valid_date(self):
        """验证日期是否正确"""
        date_str = self.sender().text()
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            if (self.sender() == self.end_date_lineEdit and self.start_date_lineEdit.text() != '' and
                    datetime.strptime(self.start_date_lineEdit.text(), "%Y-%m-%d") > datetime.strptime(self.end_date_lineEdit.text(), "%Y-%m-%d")):
                self.show_msg_box('开始日期应早于结束日期')
                self.sender().setText('')
            elif (self.sender() == self.start_date_lineEdit and self.end_date_lineEdit.text() != '' and
                  datetime.strptime(self.start_date_lineEdit.text(), "%Y-%m-%d") > datetime.strptime(self.end_date_lineEdit.text(), "%Y-%m-%d")):
                self.show_msg_box('开始日期应早于结束日期')
                self.sender().setText('')
        except ValueError:
            self.sender().setText('')

    @Slot()
    def on_start_date_calendar_button_clicked(self):
        self.start_date_calendar.raise_()
        if self.start_date_calendar.isVisible():
            self.start_date_calendar.hide()
            self.end_date_calendar.hide()
        else:
            self.start_date_calendar.show()
            self.end_date_calendar.hide()

    @Slot()
    def on_end_date_calendar_button_clicked(self):
        self.end_date_calendar.raise_()
        if self.end_date_calendar.isVisible():
            self.start_date_calendar.hide()
            self.end_date_calendar.hide()
        else:
            self.start_date_calendar.hide()
            self.end_date_calendar.show()

    @Slot()
    def on_start_date_calendar_clicked(self):
        self.start_date_lineEdit.setText(self.start_date_calendar.selectedDate().toString("yyyy-MM-dd"))
        if (self.end_date_lineEdit.text() != ''
                and (datetime.strptime(self.start_date_lineEdit.text(), "%Y-%m-%d") > datetime.strptime(self.end_date_lineEdit.text(), "%Y-%m-%d")
                     or self.end_date_lineEdit.text() == self.start_date_lineEdit.text())):
            self.show_msg_box('开始日期应早于结束日期')
            self.start_date_lineEdit.setText('')
        else:
            self.start_date_calendar.hide()

    @Slot()
    def on_end_date_calendar_clicked(self):
        self.end_date_lineEdit.setText(self.end_date_calendar.selectedDate().toString("yyyy-MM-dd"))
        if (self.start_date_lineEdit.text() != ''
                and (datetime.strptime(self.start_date_lineEdit.text(), "%Y-%m-%d") > datetime.strptime(self.end_date_lineEdit.text(), "%Y-%m-%d")
                     or self.end_date_lineEdit.text() == self.start_date_lineEdit.text())):
            self.show_msg_box('结束日期应晚于开始日期')
            self.end_date_lineEdit.setText('')
        else:
            self.end_date_calendar.hide()

    def webengine_toolbutton_widget(self):
        self.webtoolbuttonwidget = QWidget(parent=self.webwidget)
        self.webtoolbuttonwidget.setStyleSheet("background-color: rgba(0, 0, 0, 0)")
        self.webtoolbuttonwidget.setFixedSize(50, 100)
        self.webpushbuttonwidget_layout = QVBoxLayout()
        self.webpushbuttonwidget_layout.setSpacing(20)
        self.webtoolbuttonwidget.setLayout(self.webpushbuttonwidget_layout)
        self.reload_webengine_toolbutton_angle = 0
        self.reload_webengine_toolbutton = QToolButton(parent=self.webtoolbuttonwidget, ObjectName='reload_webengine_toolbutton')
        self.reload_webengine_toolbutton.setStyleSheet("background-color: rgb(243, 243, 243)")
        self.reload_webengine_toolbutton.setShortcut('F5')
        self.reload_webengine_toolbutton.setFixedSize(40, 40)
        self.reload_webengine_toolbutton_pixmap = QPixmap("resources/icon/reload.png").scaled(self.reload_webengine_toolbutton.size())
        self.reload_webengine_toolbutton.setIcon(self.reload_webengine_toolbutton_pixmap)
        self.switch_visiualize_toolbutton = QToolButton(parent=self.webtoolbuttonwidget, ObjectName='switch_visiualize_toolbutton')
        self.switch_visiualize_toolbutton.setStyleSheet("background-color: rgb(243, 243, 243)")
        self.switch_visiualize_toolbutton.setFixedSize(40, 40)
        self.switch_visiualize_toolbutton.setIcon(QIcon('resources/icon/3D.png'))
        self.webpushbuttonwidget_layout.addWidget(self.reload_webengine_toolbutton)
        self.webpushbuttonwidget_layout.addWidget(self.switch_visiualize_toolbutton)
        self.webpushbuttonwidget_layout.addStretch(1)
        return self.webtoolbuttonwidget

    def custom_region_download_widget(self):
        self.customregion_downloadwidget = QWidget(parent=self.central_widget)
        self.customregion_downloadwidget.setStyleSheet("background-color: rgb(243, 243, 243);")
        self.customregion_downloadwidget.setFixedSize(800, 540)
        self.customregion_downloadwidget_layout = QHBoxLayout()
        self.customregion_geodataview_canvas = geodata_view.PlotCanvas(parent=self.customregion_downloadwidget)
        self.customregion_geodataview_canvas.setMaximumWidth(450)
        self.customregion_downloadwidget_top_formlayout = QFormLayout()
        self.customregion_datatype_label = QLabel(text='数据类型：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_datatype_label)
        self.customregion_datatype_combobox = QComboBox(parent=self.customregion_downloadwidget, ObjectName='customregion_datatype_combobox')
        self.set_combobox_stylesheet(self.customregion_datatype_combobox)
        self.customregion_datatype_combobox.addItems(['文件', 'POSTGIS', '从剪贴板直接复制WKT'])
        self.customregion_test_engine = None
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_datatype_label, self.customregion_datatype_combobox)
        self.customregion_filename_label = QLabel(text='文件路径：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_filename_label)
        self.customregion_filename_lineedit = QLineEdit(parent=self.customregion_downloadwidget)
        self.set_lineedit_stylesheet(self.customregion_filename_lineedit)
        self.customregion_filename_button = QPushButton(text='浏览')
        self.customregion_filename_button.setMinimumHeight(20)
        self.customregion_filename_button.setMaximumHeight(20)
        self.customregion_filename_button.clicked.connect(lambda: self.choosepath(is_customregion_file=True))
        self.customregion_filename_button.setMinimumWidth(40)
        self.set_pushbutton_stylesheet(self.customregion_filename_button)
        self.customregion_downloadwidget_filename_layout = QHBoxLayout()
        self.customregion_downloadwidget_filename_layout.addWidget(self.customregion_filename_lineedit)
        self.customregion_downloadwidget_filename_layout.addWidget(self.customregion_filename_button)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_filename_label, self.customregion_downloadwidget_filename_layout)
        self.customregion_database_host_label = QLabel(text='主机：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_database_host_label)
        self.customregion_database_host_lineedit = QLineEdit(parent=self.customregion_downloadwidget)
        self.customregion_database_host_lineedit.setPlaceholderText('127.0.0.1')
        self.set_lineedit_stylesheet(self.customregion_database_host_lineedit)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_database_host_label, self.customregion_database_host_lineedit)
        self.customregion_database_port_label = QLabel(text='端口：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_database_port_label)
        self.customregion_database_port_lineedit = QLineEdit(parent=self.customregion_downloadwidget)
        self.customregion_database_port_lineedit.setPlaceholderText('5432')
        self.set_lineedit_stylesheet(self.customregion_database_port_lineedit)
        int_validator = QIntValidator()
        self.customregion_database_port_lineedit.setValidator(int_validator)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_database_port_label, self.customregion_database_port_lineedit)
        self.customregion_database_database_label = QLabel(text='数据库：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_database_database_label)
        self.customregion_database_database_lineedit = QLineEdit(parent=self.customregion_downloadwidget)
        self.customregion_database_database_lineedit.setPlaceholderText('postgres')
        self.set_lineedit_stylesheet(self.customregion_database_database_lineedit)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_database_database_label, self.customregion_database_database_lineedit)
        self.customregion_database_username_label = QLabel(text='用户名：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_database_username_label)
        self.customregion_database_username_lineedit = QLineEdit(parent=self.customregion_downloadwidget)
        self.customregion_database_username_lineedit.setPlaceholderText('postgres')
        self.set_lineedit_stylesheet(self.customregion_database_username_lineedit)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_database_username_label, self.customregion_database_username_lineedit)
        self.customregion_database_password_label = QLabel(text='密码：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_database_password_label)
        self.customregion_database_password_lineedit = QLineEdit(parent=self.customregion_downloadwidget)
        self.customregion_database_password_lineedit.setEchoMode(QLineEdit.EchoMode.Password)
        self.set_lineedit_stylesheet(self.customregion_database_password_lineedit)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_database_password_label, self.customregion_database_password_lineedit)
        self.customregion_database_tablename_label = QLabel(text='数据表：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_database_tablename_label)
        self.customregion_database_tablename_combobox = QComboBox(parent=self.customregion_downloadwidget,
                                                                  ObjectName='customregion_database_tablename_combobox')
        self.customregion_database_tablename_combobox.setPlaceholderText('请选择数据表')
        self.set_combobox_stylesheet(self.customregion_database_tablename_combobox)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_database_tablename_label, self.customregion_database_tablename_combobox)
        self.customregion_database_colname_label = QLabel(text='数据列：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_database_colname_label)
        self.customregion_database_colname_combobox = QComboBox(parent=self.customregion_downloadwidget, ObjectName='customregion_database_colname_combobox')
        self.set_combobox_stylesheet(self.customregion_database_colname_combobox)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_database_colname_label, self.customregion_database_colname_combobox)
        self.customregion_wkt_label = QLabel(text='wkt：', parent=self.customregion_downloadwidget)
        self.set_label_stylesheet(self.customregion_wkt_label)
        self.customregion_wkt_textedit = QTextEdit(parent=self.customregion_downloadwidget, ObjectName='customregion_wkt_textedit')
        self.customregion_wkt_textedit.setAcceptRichText(False)  # 禁止富文本输入
        self.set_textedit_stylesheet(self.customregion_wkt_textedit)
        self.customregion_wkt_textedit.setMaximumHeight(400)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_wkt_label, self.customregion_wkt_textedit)
        self.customregion_downloadwidget_button_layout = QHBoxLayout()
        self.customregion_connect_button = QPushButton(text='连接数据源', parent=self.customregion_downloadwidget, ObjectName='customregion_connect_button')
        self.customregion_connect_button.setMinimumHeight(20)
        self.customregion_connect_button.setMaximumHeight(20)
        self.set_pushbutton_stylesheet(self.customregion_connect_button)
        self.customregion_downloadwidget_button_layout.addWidget(self.customregion_connect_button)
        self.customregion_connect_movie = QMovie("resources/icon/spinner.gif")
        self.customregion_connect_movie.setScaledSize(QSize(16, 16))
        self.customregion_connect_test_label = QLabel(parent=self.customregion_connect_button)
        self.customregion_connect_test_label.setStyleSheet("background-color: rgba(0,0,0,0);")
        self.customregion_connect_test_label.setMovie(self.customregion_connect_movie)
        self.customregion_connect_test_label.setVisible(False)
        self.customregion_confirm_button = QPushButton(text='确认', parent=self.customregion_downloadwidget, ObjectName='customregion_confirm_button')
        self.customregion_confirm_button.setMinimumHeight(20)
        self.customregion_confirm_button.setMaximumHeight(20)
        self.set_pushbutton_stylesheet(self.customregion_confirm_button)
        # self.customregion_confirm_button.clicked.connect(self.customregion_confirm)
        self.customregion_downloadwidget_button_layout.addWidget(self.customregion_confirm_button)
        self.customregion_downloadwidget_top_formlayout.addRow(self.customregion_downloadwidget_button_layout)
        self.customregion_downloadwidget_layout.addLayout(self.customregion_downloadwidget_top_formlayout)
        self.customregion_downloadwidget_layout.addWidget(self.customregion_geodataview_canvas)
        self.customregion_downloadwidget.setLayout(self.customregion_downloadwidget_layout)
        self.customregion_downloadwidget_top_formlayout.setRowVisible(2, False)
        self.customregion_downloadwidget_top_formlayout.setRowVisible(3, False)
        self.customregion_downloadwidget_top_formlayout.setRowVisible(4, False)
        self.customregion_downloadwidget_top_formlayout.setRowVisible(5, False)
        self.customregion_downloadwidget_top_formlayout.setRowVisible(6, False)
        self.customregion_downloadwidget_top_formlayout.setRowVisible(7, False)
        self.customregion_downloadwidget_top_formlayout.setRowVisible(8, False)
        self.customregion_downloadwidget.hide()

    @Slot()
    def on_customregion_confirm_button_clicked(self):
        wkt_string = self.customregion_wkt_textedit.toPlainText().replace("'", "").replace('"', '')
        try:
            error = None
            is_polygon = self.check_is_polygon(wkt_string)
        except Exception as e:
            error = e
            is_polygon = False
        if wkt_string == '':
            self.show_msg_box('请选择或输入一个区域')
        elif error is not None:
            self.show_msg_box('不正确的wkt格式')
        elif is_polygon:
            if self.check_is_geography_coord(wkt_string):
                self.custom_region = wkt_string
                self.range_type_combobox.lineEdit().setText('自定义区域（已读取）')
                self.customregion_downloadwidget.hide()
                self.webEngineView_load_geometry(wkt_string)
                if self.customregion_test_engine == 'postgis':
                    self.customregion_postgis_con.close()
                    self.customregion_downloadwidget_top_formlayout.setRowVisible(7, False)
                    self.customregion_downloadwidget_top_formlayout.setRowVisible(8, False)
            else:
                self.show_msg_box('您选择的区域不是有效的地理坐标（经纬度）')
        else:
            self.show_msg_box('下载范围必须是一个区域（面）而非点或者线')

    @staticmethod
    def check_is_geography_coord(wkt_string):
        geometry = loads(wkt_string)
        if isinstance(geometry, Polygon):
            for x, y in geometry.exterior.coords:
                if not (-180 <= x <= 180 and -90 <= y <= 90):
                    return False
            return True
        elif isinstance(geometry, MultiPolygon):
            for poly in geometry.geoms:
                for x, y in poly.exterior.coords:
                    if not (-180 <= x <= 180 and -90 <= y <= 90):
                        return False
            return True
        elif isinstance(geometry, GeometryCollection):
            for poly in geometry.geoms:
                if isinstance(poly, Polygon):
                    for x, y in poly.exterior.coords:
                        if not (-180 <= x <= 180 and -90 <= y <= 90):
                            return False
                elif isinstance(poly, MultiPolygon):
                    for polygon in poly.geoms:
                        for x, y in polygon.exterior.coords:
                            if not (-180 <= x <= 180 and -90 <= y <= 90):
                                return False
            return True

    @staticmethod
    def check_is_polygon(wkt_string):
        geometry = loads(wkt_string)
        if isinstance(geometry, (Polygon, MultiPolygon)):
            return True
        elif isinstance(geometry, GeometryCollection):
            return any(isinstance(g, (Polygon, MultiPolygon)) for g in geometry.geoms)
        return False

    @Slot()
    def on_customregion_datatype_combobox_currentIndexChanged(self):
        if self.customregion_datatype_combobox.currentIndex() == 0:
            self.customregion_downloadwidget_top_formlayout.setRowVisible(1, True)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(2, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(3, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(4, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(5, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(6, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(7, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(8, False)
            self.customregion_connect_button.setEnabled(True)
        elif self.customregion_datatype_combobox.currentIndex() == 1:
            self.customregion_downloadwidget_top_formlayout.setRowVisible(1, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(2, True)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(3, True)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(4, True)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(5, True)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(6, True)
            self.customregion_connect_button.setEnabled(True)
            # self.customregion_downloadwidget_top_formlayout.setRowVisible(7, True)
            # self.customregion_downloadwidget_top_formlayout.setRowVisible(8, True)
        elif self.customregion_datatype_combobox.currentIndex() == 2:
            self.customregion_downloadwidget_top_formlayout.setRowVisible(1, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(2, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(3, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(4, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(5, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(6, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(7, False)
            self.customregion_downloadwidget_top_formlayout.setRowVisible(8, False)
            self.customregion_connect_button.setEnabled(False)
            clipboard = QApplication.clipboard()
            clipboard_text = clipboard.text()  # 获取文本内容
            try:
                geometry = loads(clipboard_text)
                if isinstance(geometry, Polygon) or isinstance(geometry, MultiPolygon):
                    self.customregion_wkt_textedit.setText(clipboard_text)
            except:
                pass

    @Slot()
    def on_customregion_database_tablename_combobox_currentIndexChanged(self):
        self.customregion_database_colname_combobox.blockSignals(True)
        self.customregion_database_colname_combobox.clear()
        self.customregion_database_colname_combobox.blockSignals(False)
        table_name = self.customregion_database_tablename_combobox.currentText()
        geo_col = [table_col[1] for table_col in self.customregion_postgis_tablesgeometrylist if table_col[0] == table_name]
        if len(geo_col) > 1:
            self.customregion_database_colname_combobox.blockSignals(True)
            self.customregion_database_colname_combobox.addItems(geo_col)
            self.customregion_database_colname_combobox.setCurrentIndex(-1)
            self.customregion_database_colname_combobox.blockSignals(False)
        else:
            self.customregion_database_colname_combobox.addItems(geo_col)

    @Slot()
    def on_customregion_database_colname_combobox_currentIndexChanged(self):
        table_name = self.customregion_database_tablename_combobox.currentText()
        geom_col = self.customregion_database_colname_combobox.currentText()
        self.geodata_view = geodata_view.ReadGeodata(engine='postgis', is_transparent_window=self.settings['transparent_window'],
                                                     postgis_con=self.customregion_postgis_con, table_name=table_name, geom_col=geom_col)
        if len(self.geodata_view.geodata) == 0:
            self.show_msg_box('选择的数据表内不包含任何数据')
        elif len(self.geodata_view.geodata) == 1:
            row = self.geodata_view.geodata.iloc[0]
            geometry = row[self.geodata_view.geodata.geometry.name]
            self.customregion_wkt_textedit.setText(geometry.wkt)
        else:
            self.geodata_view.setupui()
            self.geodata_view.closed.connect(self.get_selected_geodata)
            self.geodata_view.show()

    @Slot()
    def on_customregion_wkt_textedit_textChanged(self):
        try:
            geometry = loads(self.customregion_wkt_textedit.toPlainText().replace("'", "").replace('"', ''))
            self.customregion_geodataview_canvas.update_plot(data=geopandas.GeoDataFrame({'geometry': [geometry]}))
        except Exception as e:
            self.customregion_geodataview_canvas.update_plot(data=geopandas.GeoDataFrame({'geometry': []}))

    def restore_customregion_connect_button(self):
        self.set_pushbutton_stylesheet(self.customregion_connect_button)
        self.customregion_connect_button.setEnabled(True)
        self.customregion_connect_button.setText('连接数据源')

    def customregion_connect_test_result(self, message):
        QTimer.singleShot(3000, self.restore_customregion_connect_button)
        self.customregion_downloadwidget.setStyleSheet("background-color: rgb(243, 243, 243);")
        self.customregion_connect_test_label.hide()
        self.customregion_connect_movie.stop()
        if 'success' in message:
            self.set_pushbutton_special_stylesheet(pushbutton=self.customregion_connect_button, test_result=True)
            if 'file' in message:
                self.geodata_view = geodata_view.ReadGeodata(engine='file', is_transparent_window=self.settings['transparent_window'],
                                                             filename=self.customregion_test_filename)
                if len(self.geodata_view.geodata) == 0:
                    self.show_msg_box('选择的文件内不包含任何数据')
                elif len(self.geodata_view.geodata) == 1:
                    row = self.geodata_view.geodata.iloc[0]
                    geometry = row[self.geodata_view.geodata.geometry.name]
                    self.customregion_wkt_textedit.setText(geometry.wkt)
                else:
                    self.geodata_view.setupui()
                    self.geodata_view.closed.connect(self.get_selected_geodata)
                    self.geodata_view.show()
            elif 'postgis' in message:
                if len(self.customregion_postgis_tablelist) == 0:
                    self.show_msg_box('数据库连接成功，但是没有检测到空间数据表')
                else:
                    self.customregion_database_tablename_combobox.clear()
                    self.customregion_database_tablename_combobox.addItems(self.customregion_postgis_tablelist)
                    self.customregion_database_tablename_combobox.setCurrentIndex(-1)
                    self.customregion_downloadwidget_top_formlayout.setRowVisible(7, True)
                    self.customregion_downloadwidget_top_formlayout.setRowVisible(8, True)
        elif 'fail' in message:
            self.set_pushbutton_special_stylesheet(pushbutton=self.customregion_connect_button, test_result=False)
            self.show_msg_box('数据源连接失败')

    @Slot()
    def on_customregion_connect_button_clicked(self):
        current_focus = self.mainwindow.focusWidget()
        self.customregion_downloadwidget.setStyleSheet("background-color: rgb(243, 243, 243);")
        self.customregion_downloadwidget_top_formlayout.setRowVisible(7, False)
        self.customregion_downloadwidget_top_formlayout.setRowVisible(8, False)
        self.customregion_wkt_textedit.setText('')
        self.customregion_connect_test_label.setGeometry(int(self.customregion_connect_button.width() * 0.5 - 8), 2, 16, 16)
        if self.customregion_datatype_combobox.currentIndex() == 0:
            if self.customregion_filename_lineedit.text() != '':
                filename = self.customregion_filename_lineedit.text()
                if not os.path.exists(filename):
                    self.show_msg_box('不存在的文件路径')
                elif os.path.getsize(filename) / 1024 / 1024 > 100:
                    self.show_msg_box('不支持超过100MB的大型文件')
                else:
                    self.set_pushbutton_special_stylesheet(pushbutton=self.customregion_connect_button, is_testing=True)
                    if current_focus == self.customregion_connect_button:
                        self.customregion_downloadwidget.setFocus()
                        # 保持焦点在原控件上（如果焦点不在按钮上）
                    elif current_focus:
                        current_focus.setFocus()
                    self.customregion_filename_lineedit.setCursorPosition(0)
                    self.customregion_connect_test_label.show()
                    self.customregion_connect_movie.start()
                    self.customregion_test_engine = 'file'
                    self.customregion_test_filename = filename
                    customregion_connect_test_thread = threading.Thread(target=self.customregion_connect_test)
                    customregion_connect_test_thread.start()
            else:
                self.show_msg_box('请选择文件路径')
        if self.customregion_datatype_combobox.currentIndex() == 1:
            host = self.customregion_database_host_lineedit.text() if self.customregion_database_host_lineedit.text() != '' \
                else self.customregion_database_host_lineedit.placeholderText()
            port = self.customregion_database_host_lineedit.text() if self.customregion_database_port_lineedit.text() != '' \
                else self.customregion_database_port_lineedit.placeholderText()
            database = self.customregion_database_database_lineedit.text() if self.customregion_database_database_lineedit.text() != '' \
                else self.customregion_database_database_lineedit.placeholderText()
            username = self.customregion_database_username_lineedit.text() if self.customregion_database_database_lineedit.text() != '' \
                else self.customregion_database_database_lineedit.placeholderText()
            password = self.customregion_database_password_lineedit.text() if self.customregion_database_password_lineedit.text() != '' \
                else self.customregion_database_password_lineedit.placeholderText()
            if host != '' and port != '' and database != '' and username != '' and password != '':
                self.set_pushbutton_special_stylesheet(pushbutton=self.customregion_connect_button, is_testing=True)
                self.customregion_connect_test_label.show()
                self.customregion_connect_movie.start()
                self.customregion_postgis_connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
                self.customregion_test_engine = 'postgis'
                customregion_connect_test_thread = threading.Thread(target=self.customregion_connect_test)
                customregion_connect_test_thread.start()
            else:
                self.show_msg_box('必须输入数据库连接参数')

    def customregion_connect_test(self):
        if self.customregion_test_engine == 'file':
            try:
                self.geodata_view = geodata_view.ReadGeodata(engine='file', is_transparent_window=self.settings['transparent_window'],
                                                             filename=self.customregion_test_filename)
                self.datasource_connect_signal.emit('file success')
            except Exception as e:
                self.datasource_connect_signal.emit('file fail')
        elif self.customregion_test_engine == 'postgis':
            try:
                self.customregion_postgis_con = psycopg2.connect(self.customregion_postgis_connection_string)
                self.customregion_postgis_cursor = self.customregion_postgis_con.cursor()
                self.get_geotablelist_from_postgis()
                self.datasource_connect_signal.emit('postgis success')
            except:
                self.datasource_connect_signal.emit('postgis fail')

    def get_selected_geodata(self):
        try:
            self.customregion_wkt_textedit.setText(self.geodata_view.selected_data.wkt)
        except:
            pass

    def get_geotablelist_from_postgis(self):
        try:
            self.customregion_postgis_cursor.execute("select f_table_name,f_geometry_column,'geometry' from geometry_columns union "
                                                     "select f_table_name,f_geography_column,'grography' from geography_columns")
            table_list_result = self.customregion_postgis_cursor.fetchall()
            self.customregion_postgis_tablelist = [table[0] for table in table_list_result]
            self.customregion_postgis_tablesgeometrylist = table_list_result
        except:
            pass

    def task_widget(self):
        self.taskwidget = QWidget(parent=self.stackwidget)
        self.taskwidget_layout = QVBoxLayout()
        self.taskwidget_layout.setContentsMargins(40, 30, 40, 30)
        self.taskwidget.setLayout(self.taskwidget_layout)
        # 顶部layout，用于显示所有任务等控件
        self.taskwidget_top_layout = QHBoxLayout()
        self.taskwidget_top_layout.setSpacing(45)
        self.taskwidget_top_layout.setContentsMargins(0, 0, 60, 0)
        self.task_list_label = QLabel(text='任务列表:', parent=self.taskwidget)
        self.set_label_stylesheet(self.task_list_label)
        self.all_tasks_radio = QRadioButton(text='所有任务', parent=self.taskwidget)
        self.set_radiobutton_stylesheet(self.all_tasks_radio)
        self.all_tasks_radio.setCursor(Qt.CursorShape.PointingHandCursor)
        self.executing_tasks_radio = QRadioButton(text='正在进行', parent=self.taskwidget)
        self.set_radiobutton_stylesheet(self.executing_tasks_radio)
        self.executing_tasks_radio.setCursor(Qt.CursorShape.PointingHandCursor)
        self.complete_tasks_radio = QRadioButton(text='已完成', parent=self.taskwidget)
        self.set_radiobutton_stylesheet(self.complete_tasks_radio)
        self.complete_tasks_radio.setCursor(Qt.CursorShape.PointingHandCursor)
        self.task_radiogroup = QButtonGroup(parent=self.taskwidget, ObjectName='task_radiogroup')
        self.task_radiogroup.addButton(self.all_tasks_radio)
        self.task_radiogroup.addButton(self.executing_tasks_radio)
        self.task_radiogroup.addButton(self.complete_tasks_radio)
        self.task_radiogroup.buttonClicked.connect(self.task_radiogroup_buttonClicked)
        self.task_scrollarea()
        self.taskwidget_top_layout.addWidget(self.task_list_label)
        self.taskwidget_top_layout.addStretch(1)
        self.taskwidget_top_layout.addWidget(self.all_tasks_radio)
        self.taskwidget_top_layout.addWidget(self.executing_tasks_radio)
        self.taskwidget_top_layout.addWidget(self.complete_tasks_radio)
        self.taskwidget_layout.addLayout(self.taskwidget_top_layout)
        # self.taskwidget_layout.addWidget(self.tasklist_widget)
        self.taskwidget_layout.addWidget(self.taskscrollarea)
        # self.taskwidget_layout.addStretch(1)
        # for widget in self.taskwidget.findChildren(QWidget):
        #     widget.setStyleSheet("border: 1px solid red;")
        # self.taskwidget.setStyleSheet("border: 1px solid red;")
        return self.taskwidget

    def task_scrollarea(self):
        self.task_widget_dict = {}
        if self.settings['select_task'] == 'all_tasks':
            self.all_tasks_radio.setChecked(True)
            display_tasks = self.exists_task.copy()
        elif self.settings['select_task'] == 'executing_tasks':
            self.executing_tasks_radio.setChecked(True)
            if not self.exists_task.empty:
                display_tasks = self.exists_task[self.exists_task['is_executing']].copy()
        elif self.settings['select_task'] == 'complete_tasks':
            self.complete_tasks_radio.setChecked(True)
            if not self.exists_task.empty:
                display_tasks = self.exists_task[self.exists_task['is_complete']].copy()
        self.taskscrollarea = QScrollArea(parent=self.taskwidget)
        self.tasklist_widget = QWidget(parent=self.taskscrollarea)
        if self.settings['transparent_window']:
            self.tasklist_widget.setStyleSheet('background-color: rgba(0, 0, 0, 0)')
        self.tasklist_widget_layout = QVBoxLayout()
        self.tasklist_widget.setLayout(self.tasklist_widget_layout)
        self.taskscrollarea.setWidget(self.tasklist_widget)
        self.set_scrollarea_stylesheet(self.taskscrollarea)
        self.taskscrollarea.setWidgetResizable(True)
        try:
            for index, row in display_tasks.iterrows():
                task_parameter = row.to_dict()
                task_widget = customwidget.TaskListgWidget(task_parameter=task_parameter, main_instance=self, parent=self.tasklist_widget,
                                                           ObjectName=f'task_widget_{task_parameter['taskname']}')
                # task_widget.setStyleSheet('border: 1px solid red')
                self.tasklist_widget_layout.addWidget(task_widget)
                self.task_widget_dict[task_parameter['taskname']] = task_widget
        except Exception as e:
            print('task_list', e)
        self.tasklist_widget_layout.addStretch(1)
        return self.taskscrollarea

    def update_taskscrollarea(self):
        if self.settings['select_task'] == 'all_tasks':
            self.all_tasks_radio.setChecked(True)
            display_tasks = self.exists_task.copy()
        elif self.settings['select_task'] == 'executing_tasks':
            if not self.exists_task.empty:
                display_tasks = self.exists_task[self.exists_task['is_executing']].copy()
        elif self.settings['select_task'] == 'complete_tasks':
            if not self.exists_task.empty:
                display_tasks = self.exists_task[self.exists_task['is_complete']].copy()
        display_tasks = self.sort_downloadtasks(display_tasks)
        for index, row in display_tasks.iterrows():
            task_parameter = row.to_dict()
            taskname = task_parameter['taskname']
            if taskname not in self.task_widget_dict.keys():
                new_taskwidget = customwidget.TaskListgWidget(task_parameter=task_parameter, main_instance=self, parent=self.tasklist_widget,
                                                              ObjectName=f'task_widget_{task_parameter['taskname']}')
                # new_taskwidget.setStyleSheet('border: 1px solid red')
                self.tasklist_widget_layout.insertWidget(0, new_taskwidget, stretch=1)
                self.task_widget_dict[taskname] = new_taskwidget
        for key, value in self.task_widget_dict.items():
            if not (display_tasks['taskname'] == key).any():
                self.tasklist_widget_layout.removeWidget(self.task_widget_dict[key])
                self.task_widget_dict[key].deleteLater()
                self.task_widget_dict[key].setParent(None)
        valid_tasks = set(display_tasks['taskname'])
        filtered_dict = {k: v for k, v in self.task_widget_dict.items() if k in valid_tasks}
        self.task_widget_dict = filtered_dict
        self.tasklist_widget_layout.update()

    def task_radiogroup_buttonClicked(self, button=None):
        element_value = 'all_tasks'
        try:
            if button.text() == '所有任务':
                element_value = 'all_tasks'
            elif button.text() == '正在进行':
                element_value = 'executing_tasks'
            elif button.text() == '已完成':
                element_value = 'complete_tasks'
        except:
            pass
        self.sort_downloadtasks()
        self.settings['select_task'] = element_value
        self.replace_widget(self.taskscrollarea, self.task_scrollarea)
        self.stackwidget.setCurrentIndex(1)
        self.write_settings('select_task', element_value)

    def setting_widget(self):
        self.settingwidget = QWidget(parent=self.stackwidget)
        self.settingwidgetspacing = 15
        # 主布局为垂直布局
        self.settingwidget_layout = QVBoxLayout()
        self.settingwidget_layout.setContentsMargins(40, 30, 20, 0)
        self.settingwidget_layout.setSpacing(25)
        self.settingwidget.setLayout(self.settingwidget_layout)
        self.application_label = QLabel(text="软件设置：", parent=self.settingwidget)
        self.set_label_stylesheet(self.application_label)
        self.proxies_label = QLabel(text='代理设置：', parent=self.settingwidget)
        self.set_label_stylesheet(self.proxies_label)
        self.google_authenticate_label_widget = QWidget(parent=self.settingwidget)
        self.google_authenticate_label = QLabel(text='谷歌认证设置：', parent=self.google_authenticate_label_widget)
        self.google_authenticate_label_layout = QHBoxLayout()
        self.google_authenticate_label_layout.setContentsMargins(0, 0, 0, 0)
        self.google_authenticate_label_widget.setLayout(self.google_authenticate_label_layout)
        self.set_label_stylesheet(self.google_authenticate_label)
        self.google_authenticate_guidance_label = QLabel(text='<html><u>我应该如何获取谷歌认证信息？</u></html>', parent=self.google_authenticate_label_widget)
        self.set_label_stylesheet(self.google_authenticate_guidance_label, is_guidance_link=True)
        self.google_authenticate_guidance_label.setCursor(Qt.CursorShape.PointingHandCursor)
        self.google_authenticate_guidance_label.mousePressEvent = self.on_google_authenticate_guidance_click
        self.google_authenticate_guidance_label.hide()
        self.google_authenticate_label_layout.addWidget(self.google_authenticate_label)
        self.google_authenticate_label_layout.addWidget(self.google_authenticate_guidance_label)
        self.google_authenticate_label_layout.addStretch(1)
        self.license_label = QLabel(text='许可设置：', parent=self.settingwidget)
        self.set_label_stylesheet(self.license_label)
        self.application_setting_widget()
        self.proxies_setting_widget()
        self.google_authenticate_widget()
        self.license_widget()
        self.settingwidget_layout.addWidget(self.application_label)
        self.settingwidget_layout.addWidget(self.applicationsettingwidget)
        self.settingwidget_layout.addWidget(self.proxies_label)
        self.settingwidget_layout.addWidget(self.proxiessettingwidget)
        self.settingwidget_layout.addWidget(self.google_authenticate_label_widget)
        self.settingwidget_layout.addWidget(self.googleauthenticatewidget)
        self.settingwidget_layout.addWidget(self.license_label)
        self.settingwidget_layout.addWidget(self.licensewidget)
        self.settingwidget_layout.addStretch(1)
        for label in self.settingwidget.findChildren(QLabel):
            label.setMinimumHeight(20)
            label.setMaximumHeight(20)
            label.setMinimumWidth(60)
        for lineedit in self.settingwidget.findChildren(QLineEdit):
            lineedit.setMinimumHeight(20)
            lineedit.setMaximumHeight(20)
        for pushbutton in self.settingwidget.findChildren(QPushButton):
            if pushbutton != self.json_key_choose_button:
                pushbutton.setMinimumHeight(20)
                pushbutton.setMaximumHeight(20)
                pushbutton.setMinimumWidth(120)
                pushbutton.setMaximumWidth(120)
            else:
                pushbutton.setMinimumHeight(20)
                pushbutton.setMaximumHeight(20)
                pushbutton.setMinimumWidth(20)
                pushbutton.setMaximumWidth(20)
        # for widget in self.settingwidget.findChildren(QWidget):
        #     widget.setStyleSheet("border: 1px solid red;")
        return self.settingwidget

    def application_setting_widget(self):
        self.applicationsettingwidget = QWidget(parent=self.settingwidget)
        self.application_setting_widget_layout = QGridLayout()  # 网格布局，用于显示软件设置的相关内容
        self.application_setting_widget_layout.setSpacing(self.settingwidgetspacing)
        self.application_setting_widget_layout.setContentsMargins(30, 0, 30, 0)
        self.applicationsettingwidget.setLayout(self.application_setting_widget_layout)
        self.gui_setting_label = QLabel(text="界面风格配置(重启生效)：", parent=self.applicationsettingwidget)
        self.set_label_stylesheet(self.gui_setting_label)
        self.gui_setting_radio1 = QRadioButton(text='常规', parent=self.applicationsettingwidget)
        self.gui_setting_radio1.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.gui_setting_radio1)
        self.gui_setting_radio2 = QRadioButton(text='透明', parent=self.settingwidget)
        self.gui_setting_radio2.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.gui_setting_radio2)
        self.gui_setting_radiogroup = QButtonGroup(parent=self.settingwidget, ObjectName='gui_setting_radiogroup')
        self.gui_setting_radiogroup.addButton(self.gui_setting_radio1)
        self.gui_setting_radiogroup.addButton(self.gui_setting_radio2)
        if not self.settings['transparent_window']:
            self.gui_setting_radio1.setChecked(True)
        else:
            self.gui_setting_radio2.setChecked(True)
        self.when_click_closebutton_label = QLabel(text="点击退出按钮时：", parent=self.applicationsettingwidget)
        self.set_label_stylesheet(self.when_click_closebutton_label)
        self.when_click_closebutton_radio1 = QRadioButton(text='退出程序', parent=self.applicationsettingwidget)
        self.when_click_closebutton_radio1.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.when_click_closebutton_radio1)
        self.when_click_closebutton_radio2 = QRadioButton(text='隐藏到任务栏托盘', parent=self.applicationsettingwidget)
        self.when_click_closebutton_radio2.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.when_click_closebutton_radio2)
        self.when_click_closebutton_radiogroup = QButtonGroup(parent=self.settingwidget, ObjectName='when_click_closebutton_radiogroup')
        self.when_click_closebutton_radiogroup.addButton(self.when_click_closebutton_radio1)
        self.when_click_closebutton_radiogroup.addButton(self.when_click_closebutton_radio2)
        if self.settings['when_click_closebutton'] == 'quit':
            self.when_click_closebutton_radio1.setChecked(True)
        else:
            self.when_click_closebutton_radio2.setChecked(True)
        self.when_close_application_label = QLabel(text='关闭软件时，有正在进行的任务：', parent=self.applicationsettingwidget)
        self.set_label_stylesheet(self.when_close_application_label)
        self.when_close_application_radio1 = QRadioButton(text='提示我', parent=self.applicationsettingwidget)
        self.when_close_application_radio1.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.when_close_application_radio1)
        self.when_close_application_radio2 = QRadioButton(text='立即停止所有任务并退出', parent=self.applicationsettingwidget)
        self.when_close_application_radio2.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.when_close_application_radio2)
        self.when_close_application_radiogroup = QButtonGroup(parent=self.settingwidget, ObjectName='when_close_application_radiogroup')
        self.when_close_application_radiogroup.addButton(self.when_close_application_radio1)
        self.when_close_application_radiogroup.addButton(self.when_close_application_radio2)
        if self.settings['when_close_application'] == 'remind_me':
            self.when_close_application_radio1.setChecked(True)
        else:
            self.when_close_application_radio2.setChecked(True)
        self.default_map_visualize_label = QLabel(text='默认地图渲染形式：', parent=self.applicationsettingwidget)
        self.set_label_stylesheet(self.default_map_visualize_label)
        self.default_map_visualize_radio1 = QRadioButton(text='2d', parent=self.applicationsettingwidget)
        self.default_map_visualize_radio1.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.default_map_visualize_radio1)
        self.default_map_visualize_radio2 = QRadioButton(text='3d(如果支持)', parent=self.applicationsettingwidget)
        self.default_map_visualize_radio2.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.default_map_visualize_radio2)
        self.default_map_visualize_radiogroup = QButtonGroup(parent=self.settingwidget, ObjectName='default_map_visualize_radiogroup')
        self.default_map_visualize_radiogroup.addButton(self.default_map_visualize_radio1)
        self.default_map_visualize_radiogroup.addButton(self.default_map_visualize_radio2)
        if self.settings['default_map_visualize'] == '2d':
            self.default_map_visualize_radio1.setChecked(True)
        else:
            self.default_map_visualize_radio2.setChecked(True)
        self.enable_gpu_data_process_label = QLabel(text='启用GPU进行图像处理：', parent=self.applicationsettingwidget)
        self.set_label_stylesheet(self.enable_gpu_data_process_label)
        self.enable_gpu_data_process_radio1 = QRadioButton(text='关闭', parent=self.applicationsettingwidget)
        self.enable_gpu_data_process_radio1.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.enable_gpu_data_process_radio1)
        self.enable_gpu_data_process_radio2 = QRadioButton(text='开启', parent=self.applicationsettingwidget)
        self.enable_gpu_data_process_radio2.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.enable_gpu_data_process_radio2)
        self.default_map_visualize_radiogroup = QButtonGroup(parent=self.settingwidget, ObjectName='enable_gpu_data_process_radiogroup')
        self.default_map_visualize_radiogroup.addButton(self.enable_gpu_data_process_radio1)
        self.default_map_visualize_radiogroup.addButton(self.enable_gpu_data_process_radio2)
        self.enable_gpu_data_process_radio1.setEnabled(False)
        self.enable_gpu_data_process_radio2.setEnabled(False)
        if not self.settings['enable_gpu']:
            self.enable_gpu_data_process_radio1.setChecked(True)
        else:
            self.enable_gpu_data_process_radio2.setChecked(True)
        self.application_setting_widget_layout.addWidget(self.gui_setting_label, 0, 0)
        self.application_setting_widget_layout.addWidget(self.gui_setting_radio1, 0, 2)
        self.application_setting_widget_layout.addWidget(self.gui_setting_radio2, 0, 3)
        self.application_setting_widget_layout.addWidget(self.default_map_visualize_label, 0, 4)
        self.application_setting_widget_layout.addWidget(self.default_map_visualize_radio1, 0, 6)
        self.application_setting_widget_layout.addWidget(self.default_map_visualize_radio2, 0, 7)
        self.application_setting_widget_layout.addWidget(self.when_click_closebutton_label, 1, 0)
        self.application_setting_widget_layout.addWidget(self.when_click_closebutton_radio1, 1, 2)
        self.application_setting_widget_layout.addWidget(self.when_click_closebutton_radio2, 1, 3)
        self.application_setting_widget_layout.addWidget(self.enable_gpu_data_process_label, 1, 4)
        self.application_setting_widget_layout.addWidget(self.enable_gpu_data_process_radio1, 1, 6)
        self.application_setting_widget_layout.addWidget(self.enable_gpu_data_process_radio2, 1, 7)
        self.application_setting_widget_layout.addWidget(self.when_close_application_label, 2, 0)
        self.application_setting_widget_layout.addWidget(self.when_close_application_radio1, 2, 2)
        self.application_setting_widget_layout.addWidget(self.when_close_application_radio2, 2, 3)
        return self.applicationsettingwidget

    @Slot(QAbstractButton)
    def on_gui_setting_radiogroup_buttonClicked(self, button):
        element_value = True
        if button == self.gui_setting_radio1:
            element_value = False
            # self.settings['transparent_window'] = False
        elif button == self.gui_setting_radio2:
            element_value = True
            # self.settings['transparent_window'] = True
        self.write_settings('transparent_window', element_value)

    @Slot(QAbstractButton)
    def on_when_click_closebutton_radiogroup_buttonClicked(self, button):
        if button == self.when_click_closebutton_radio1:
            self.settings['when_click_closebutton'] = 'quit'
        elif button == self.when_click_closebutton_radio2:
            self.settings['when_click_closebutton'] = 'minimize'
        self.write_settings('when_click_closebutton', self.settings['when_click_closebutton'])

    @Slot(QAbstractButton)
    def on_when_close_application_radiogroup_buttonClicked(self, button):
        if button == self.when_close_application_radio1:
            self.settings['when_close_application'] = 'remind_me'
        elif button == self.when_close_application_radio2:
            self.settings['when_close_application'] = 'quit_immediately'
        self.write_settings('when_close_application', self.settings['when_close_application'])

    @Slot(QAbstractButton)
    def on_default_map_visualize_radiogroup_buttonClicked(self, button):
        if button == self.default_map_visualize_radio1:
            self.settings['default_map_visualize'] = '2d'
        elif button == self.default_map_visualize_radio2:
            self.settings['default_map_visualize'] = '3d'
        self.write_settings('default_map_visualize', self.settings['default_map_visualize'])

    @Slot(QAbstractButton)
    def on_enable_gpu_data_process_radiogroup_buttonClicked(self, button):
        if button == self.enable_gpu_data_process_radio1:
            self.settings['enable_gpu'] = False
        elif button == self.enable_gpu_data_process_radio2:
            self.settings['enable_gpu'] = True
            os.environ['NUMBA_CACHE_DIR'] = str(self.numbacache_path)
        self.write_settings('enable_gpu', self.settings['enable_gpu'])

    def reinitialization_ee_widget(self):
        self.reinitializationeewidget = QWidget(parent=self.settingwidget)
        self.reinitializationee_movie = QMovie('resources/icon/spinner.gif')
        self.reinitializationeewidget_layout = QHBoxLayout()
        self.reinitializationeewidget.setLayout(self.reinitializationeewidget_layout)
        self.reinitializationee_button = customwidget.RoundButton(hover_text='立即初始化', default_text='后初始化', countdown=60,
                                                                  parent=self.reinitializationeewidget, ObjectName='reinitializationee_button')
        self.reinitializationee_button.clicked.connect(self.on_reinitializationee_button_clicked)
        self.reinitializationee_button_label = QLabel(parent=self.reinitializationee_button)
        self.reinitializationee_button_label.setStyleSheet("background-color: rgba(0, 0, 0, 0);")
        self.reinitializationee_button_layout = QHBoxLayout()
        self.reinitializationee_button.setLayout(self.reinitializationee_button_layout)
        self.reinitializationee_button_layout.setContentsMargins(0, 0, 0, 0)
        self.reinitializationee_button_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.reinitializationee_button_layout.addWidget(self.reinitializationee_button_label)
        self.reinitializationee_button_label.hide()
        self.reinitializationeewidget_layout.addWidget(self.reinitializationee_button)
        return self.reinitializationeewidget

    def on_reinitializationee_button_clicked(self):
        self.reinitializationee_button.timer.stop()
        self.reinitializationee_button.setEnabled(False)
        self.reinitializationee_button.is_mouse_enter = False
        self.reinitializationee_button.text = ''
        self.reinitializationee_button.update()
        self.reinitializationee_movie.setScaledSize(QSize(self.reinitializationee_button_label.height(), self.reinitializationee_button_label.height()))
        self.reinitializationee_button_label.setMovie(self.reinitializationee_movie)
        self.reinitializationee_button_label.show()
        self.reinitializationee_movie.start()
        self.is_reinitialization_ee = True
        initialization_ee_thread = threading.Thread(target=self.initialization_ee)
        initialization_ee_thread.start()

    def proxies_setting_widget(self):
        # 整个代理设置widget
        self.proxiessettingwidget = QWidget(parent=self.settingwidget)
        self.proxies_setting_widget_layout = QHBoxLayout()
        self.proxy_test_movie = QMovie("resources/icon/spinner.gif")
        self.proxies_setting_widget_layout.setSpacing(60)
        self.proxies_setting_widget_layout.setContentsMargins(30, 0, 30, 0)
        self.proxiessettingwidget.setLayout(self.proxies_setting_widget_layout)
        # 左边新增代理部分，使用表单布局
        self.proxies_setting_left_layout = QFormLayout()
        self.proxies_setting_left_layout.setSpacing(self.settingwidgetspacing)
        self.proxies_setting_left_layout.setContentsMargins(0, 0, 0, 0)
        self.proxies_server_label = QLabel(text='服务器', parent=self.proxiessettingwidget)
        self.set_label_stylesheet(self.proxies_server_label)
        self.proxies_server_lineedit = QLineEdit(parent=self.proxiessettingwidget)
        self.set_lineedit_stylesheet(self.proxies_server_lineedit)
        self.proxies_port_label = QLabel(text='端口', parent=self.proxiessettingwidget)
        self.set_label_stylesheet(self.proxies_port_label)
        self.proxies_port_lineedit = QLineEdit(parent=self.proxiessettingwidget)
        self.set_lineedit_stylesheet(self.proxies_port_lineedit)
        self.proxies_type_label = QLabel(text='类型', parent=self.proxiessettingwidget)
        self.set_label_stylesheet(self.proxies_type_label)
        self.proxies_type_radio1 = QRadioButton(text='HTTP', parent=self.proxiessettingwidget)
        self.proxies_type_radio1.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.proxies_type_radio1)
        self.proxies_type_radio2 = QRadioButton(text='SOCKS5', parent=self.proxiessettingwidget)
        self.proxies_type_radio2.setCursor(Qt.CursorShape.PointingHandCursor)
        self.set_radiobutton_stylesheet(self.proxies_type_radio2)
        self.proxies_type_radiogroup = QButtonGroup(parent=self.proxiessettingwidget)
        self.proxies_type_radiogroup_layout = QHBoxLayout()
        self.proxies_type_radiogroup_layout.addWidget(self.proxies_type_radio1)
        self.proxies_type_radiogroup_layout.addWidget(self.proxies_type_radio2)
        self.proxies_type_radiogroup.addButton(self.proxies_type_radio1)
        self.proxies_type_radiogroup.addButton(self.proxies_type_radio2)
        self.proxies_username_label = QLabel(text='用户名', parent=self.proxiessettingwidget)
        self.set_label_stylesheet(self.proxies_username_label)
        self.proxies_username_lineedit = QLineEdit(parent=self.proxiessettingwidget)
        self.proxies_username_lineedit.setPlaceholderText("如无需验证请勿填写用户名、密码")
        self.set_lineedit_stylesheet(self.proxies_username_lineedit)
        self.proxies_password_label = QLabel(text='密码', parent=self.proxiessettingwidget)
        self.set_label_stylesheet(self.proxies_password_label)
        self.proxies_password_lineedit = QLineEdit(parent=self.proxiessettingwidget)
        self.set_lineedit_stylesheet(self.proxies_password_lineedit)
        self.proxies_password_lineedit.setEchoMode(QLineEdit.EchoMode.Password)
        self.proxies_setting_left_layout.addRow(self.proxies_server_label, self.proxies_server_lineedit)
        self.proxies_setting_left_layout.addRow(self.proxies_port_label, self.proxies_port_lineedit)
        self.proxies_setting_left_layout.addRow(self.proxies_type_label, self.proxies_type_radiogroup_layout)
        self.proxies_setting_left_layout.addRow(self.proxies_username_label, self.proxies_username_lineedit)
        self.proxies_setting_left_layout.addRow(self.proxies_password_label, self.proxies_password_lineedit)
        # 右边显示代理部分，使用表单布局
        self.proxies_setting_right_layout = QFormLayout()
        self.proxies_setting_right_layout.setSpacing(self.settingwidgetspacing)
        self.proxies_setting_right_layout.setContentsMargins(0, 0, 0, 0)
        self.proxies_test_button = QPushButton(text='测试代理', parent=self.settingwidget, ObjectName='proxies_test_button')
        self.proxies_test_button_layout = QHBoxLayout()
        self.proxies_test_button_layout.setContentsMargins(0, 0, 0, 0)
        self.proxies_test_button.setLayout(self.proxies_test_button_layout)
        self.set_pushbutton_stylesheet(self.proxies_test_button)
        if len(self.settings['proxies']) >= self.settings['max_proxy_server']:
            self.proxies_test_button.setEnabled(False)
            self.set_pushbutton_stylesheet(self.proxies_test_button)
        self.proxies_test_label = QLabel(parent=self.proxies_test_button)
        self.proxies_test_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.proxies_test_label.setStyleSheet("background-color: rgba(0, 0, 0, 0);")
        self.proxies_test_button_layout.addWidget(self.proxies_test_label)
        self.proxies_test_label.hide()
        self.proxy_label_button_dict = {}
        for i in range(1, 5):
            proxy_label = QLabel(text='', parent=self.proxiessettingwidget, ObjectName=f'proxy_label_{i}')
            proxy_delete_button = QPushButton(text='删除', parent=self.proxiessettingwidget, ObjectName=f'proxy_delete_button_{i}')
            proxy_delete_button.clicked.connect(self.on_proxy_delete_button_clicked)
            proxy_delete_button.hide()
            self.set_pushbutton_stylesheet(proxy_delete_button)
            self.set_label_stylesheet(proxy_label)
            self.proxies_setting_right_layout.addRow(proxy_label, proxy_delete_button)
            self.proxy_label_button_dict[str(i)] = (proxy_label, proxy_delete_button)
        self.proxies_setting_right_layout.addRow(self.proxies_test_button, )
        self.proxies_setting_widget_layout.addLayout(self.proxies_setting_left_layout, stretch=1)
        self.proxies_setting_widget_layout.addLayout(self.proxies_setting_right_layout, stretch=1)
        self.show_saved_proxy()
        return self.proxiessettingwidget

    def show_saved_proxy(self):
        proxy_list = list(self.settings['proxies'].values())
        for i in range(1, len(proxy_list) + 1):
            proxy_label, proxy_delete_button = self.proxy_label_button_dict[str(i)]
            proxy_label.setText(f'代理服务器{i}: {proxy_list[i - 1]['http']}')
            proxy_delete_button.show()
        for i in range(len(proxy_list) + 1, 5):
            proxy_label, proxy_delete_button = self.proxy_label_button_dict[str(i)]
            proxy_label.setText('')
            proxy_delete_button.hide()

    def on_proxy_delete_button_clicked(self):
        current_focus = self.mainwindow.focusWidget()
        num = self.sender().objectName().replace('proxy_delete_button_', '')
        proxy_id = list(self.settings['proxies'].keys())[int(num) - 1]
        self.settings['proxies'].pop(proxy_id)
        self.settings['display_proxies'].pop(proxy_id)
        proxy_urls = {}
        for proxy in self.settings['proxies']:
            proxy_url = self.settings['display_proxies'][proxy]['http']
            proxy_urls[proxy] = proxy_url
        self.write_settings('proxies', proxy_urls)
        try:
            keyring.delete_password(f'Nevasa_proxy_{proxy_id}', self.settings['proxies_username'][proxy_id])
        except:
            pass
        try:
            del self.settings['proxies_username'][proxy_id]
            self.write_settings('proxies_username', self.settings['proxies_username'])
        except:
            pass
        self.show_saved_proxy()
        if current_focus == self.proxies_test_button:
            self.settingwidget.setFocus()
        # 保持焦点在原控件上（如果焦点不在按钮上）
        elif current_focus:
            self.settingwidget.setFocus()

    @Slot()
    def on_proxies_test_button_clicked(self):
        if self.proxies_server_lineedit.text() == '':
            self.show_msg_box('必须输入服务器地址')
        elif self.proxies_port_lineedit.text() == '':
            self.show_msg_box('必须输入端口')
        elif not self.proxies_type_radio1.isChecked() and not self.proxies_type_radio2.isChecked():
            self.show_msg_box('必须选择代理类型')
        else:
            current_focus = self.mainwindow.focusWidget()
            self.proxy_test_movie.setScaledSize(QSize(self.proxies_test_label.height(), self.proxies_test_label.height()))
            self.proxies_test_label.setMovie(self.proxy_test_movie)
            self.proxy_test_movie.setScaledSize(QSize(self.proxies_test_label.size().height(), self.proxies_test_label.size().height()))
            self.proxies_test_label.show()
            self.proxy_test_movie.start()
            self.set_pushbutton_special_stylesheet(self.proxies_test_button, is_testing=True)
            if current_focus == self.proxies_test_button:
                self.settingwidget.setFocus()
            # 保持焦点在原控件上（如果焦点不在按钮上）
            elif current_focus:
                current_focus.setFocus()
            if self.proxies_type_radio1.isChecked():
                proxies_type = 'http'
            elif self.proxies_type_radio2.isChecked():
                proxies_type = 'socks5'
            proxy_username = self.proxies_username_lineedit.text()
            proxy_password = self.proxies_password_lineedit.text()
            if proxy_username != '':
                proxy_url = f'{proxies_type}://{proxy_username}:{proxy_password}@{self.proxies_server_lineedit.text()}:{self.proxies_port_lineedit.text()}'
            else:
                proxy_url = f'{proxies_type}://{self.proxies_server_lineedit.text()}:{self.proxies_port_lineedit.text()}'
            display_proxy_url = f'{proxies_type}://{self.proxies_server_lineedit.text()}:{self.proxies_port_lineedit.text()}'
            self.testing_proxy = {'http': proxy_url, 'https': proxy_url}
            self.testing_display_proxy = {'http': display_proxy_url, 'https': display_proxy_url}
            proxy_test_threading = threading.Thread(target=self.test.proxy_test, args=(self.testing_proxy, self.test_result_dict))
            proxy_test_threading.start()
            proxy_test_timer = QTimer()
            proxy_test_timer.timeout.connect(lambda: self.on_proxy_testing(proxy_test_timer))  # Connect timeout signal to slot
            proxy_test_timer.start(500)

    def on_proxy_testing(self, timer):
        try:
            result = self.test_result_dict['proxy_test_result']
            if result is not None:
                self.set_pushbutton_special_stylesheet(self.proxies_test_button)
                self.test_result_dict['proxy_test_result'] = None  # 立即置空测试结果，以防止干扰下次测试
                timer.stop()
                if result:
                    if self.testing_proxy not in self.settings['proxies'].values():
                        new_proxy_id = self.new_id()
                        self.settings['proxies'][new_proxy_id] = self.testing_proxy
                        self.settings['display_proxies'][new_proxy_id] = self.testing_display_proxy
                        proxy_urls = {}
                        for proxy in self.settings['display_proxies']:
                            proxy_url = self.settings['display_proxies'][proxy]['http']
                            proxy_urls[proxy] = proxy_url
                        self.write_settings('proxies', proxy_urls)
                        if self.proxies_username_lineedit.text() != '':
                            self.settings['proxies_username'][new_proxy_id] = self.proxies_username_lineedit.text()
                            self.write_settings('proxies_username', self.settings['proxies_username'])
                            keyring.set_password(f'Nevasa_proxy_{new_proxy_id}', self.proxies_username_lineedit.text(), self.proxies_password_lineedit.text())
                        self.show_saved_proxy()
                        self.proxies_server_lineedit.setText('')
                        self.proxies_port_lineedit.setText('')
                        self.proxies_username_lineedit.setText('')
                        self.proxies_password_lineedit.setText('')
                        self.proxies_type_radiogroup.setExclusive(False)
                        self.proxies_type_radio1.setChecked(False)
                        self.proxies_type_radio2.setChecked(False)
                        self.proxies_type_radiogroup.setExclusive(True)
                        self.set_proxy(True)
                        if self.settings['project_id'] is not None and self.settings['json_file'] is not None and self.settings['service_account'] is not None:
                            self.webEngineView.load(QUrl.fromLocalFile(self.temp_html_file))
                            self.lon.setText("经度：96.951170")
                            self.lat.setText("纬度：36.452220")
                            self.zoom.setText("层级：4")
                elif not result:
                    self.set_pushbutton_special_stylesheet(self.proxies_test_button, test_result=False)
                self.proxies_test_label.hide()
                QTimer.singleShot(5000, lambda: self.resotre_proxy_test_button())
        except Exception as e:
            print(e)

    def resotre_proxy_test_button(self):
        self.proxies_test_button.setText("测试代理")
        if len(self.settings['proxies']) >= self.settings['max_proxy_server']:
            self.proxies_test_button.setEnabled(False)
            self.set_pushbutton_stylesheet(self.proxies_test_button)
        else:
            self.proxies_test_button.setEnabled(True)
            self.set_pushbutton_stylesheet(self.proxies_test_button)

    def google_authenticate_widget(self):
        self.googleauthenticatewidget = QWidget(parent=self.settingwidget)
        self.google_authenticate_widget_layout = QHBoxLayout()
        self.google_authenticate_widget_layout.setSpacing(60)
        self.google_authenticate_widget_layout.setContentsMargins(30, 0, 30, 0)
        self.googleauthenticatewidget.setLayout(self.google_authenticate_widget_layout)
        # 左边新增谷歌认证部分，使用表单布局
        self.googleauthenticatewidget_left_layout = QFormLayout()
        self.googleauthenticatewidget_left_layout.setSpacing(self.settingwidgetspacing)
        self.googleauthenticatewidget_left_layout.setContentsMargins(0, 0, 0, 0)
        self.project_id_label = QLabel(text='项目ID', parent=self.googleauthenticatewidget)
        self.set_label_stylesheet(self.project_id_label)
        self.project_id_lineedit = QLineEdit(parent=self.googleauthenticatewidget)
        self.project_id_lineedit.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.set_lineedit_stylesheet(self.project_id_lineedit)
        self.service_account_label = QLabel(text='服务账号', parent=self.googleauthenticatewidget)
        self.set_label_stylesheet(self.service_account_label)
        self.service_account_lineedit = QLineEdit(parent=self.googleauthenticatewidget)
        self.set_lineedit_stylesheet(self.service_account_lineedit)
        self.service_account_lineedit.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.json_key_label = QLabel(text='JSON-KEY', parent=self.googleauthenticatewidget)
        self.set_label_stylesheet(self.json_key_label)
        self.json_key_lineedit = QLineEdit(parent=self.googleauthenticatewidget)
        self.json_key_lineedit.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.set_lineedit_stylesheet(self.json_key_lineedit)
        self.json_key_choose_button = QPushButton(text='...', parent=self.googleauthenticatewidget)
        self.set_pushbutton_stylesheet(self.json_key_choose_button)
        self.json_key_choose_button.clicked.connect(lambda: self.choosepath(is_json_key=True))
        self.json_key_layout = QHBoxLayout()
        self.json_key_layout.setSpacing(0)
        self.json_key_layout.addWidget(self.json_key_lineedit)
        self.json_key_layout.addWidget(self.json_key_choose_button)
        self.googleauthenticatewidget_left_layout.addRow(self.project_id_label, self.project_id_lineedit)
        self.googleauthenticatewidget_left_layout.addRow(self.service_account_label, self.service_account_lineedit)
        self.googleauthenticatewidget_left_layout.addRow(self.json_key_label, self.json_key_layout)
        # 右边按钮部分，使用表单布局
        self.googleauthenticatewidget_right_layout = QFormLayout()
        self.googleauthenticatewidget_right_layout.setSpacing(self.settingwidgetspacing)
        self.googleauthenticatewidget_right_layout.setContentsMargins(0, 0, 0, 0)
        self.google_authenticate_delete_button = QPushButton(text='删除认证信息', parent=self.googleauthenticatewidget,
                                                             ObjectName='google_authenticate_delete_button')
        self.set_pushbutton_stylesheet(self.google_authenticate_delete_button)
        self.google_authenticate_test_button = QPushButton(text='测试认证信息', parent=self.googleauthenticatewidget,
                                                           ObjectName='google_authenticate_test_button')
        self.google_authenticate_test_button_layout = QHBoxLayout()
        self.google_authenticate_test_button_layout.setContentsMargins(0, 0, 0, 0)
        self.google_authenticate_test_button.setLayout(self.google_authenticate_test_button_layout)
        self.set_pushbutton_stylesheet(self.google_authenticate_test_button)
        self.google_auth_test_movie = QMovie("resources/icon/spinner.gif")
        self.google_authenticate_test_label = QLabel(parent=self.google_authenticate_test_button)
        self.google_authenticate_test_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.google_authenticate_test_button_layout.addWidget(self.google_authenticate_test_label)
        # self.google_authenticate_test_label.setStyleSheet("border: 1px solid red;")
        self.google_authenticate_test_label.setStyleSheet("background-color: rgba(0,0,0,0);")
        self.google_authenticate_test_label.hide()
        self.google_auth_copy_exception_button = QPushButton(text='复制错误信息', parent=self.googleauthenticatewidget,
                                                             ObjectName='google_auth_copy_exception_button')
        self.set_pushbutton_stylesheet(self.google_auth_copy_exception_button)
        self.google_auth_copy_exception_button.hide()
        self.googleauthenticatewidget_right_layout.addRow(self.google_authenticate_delete_button, QLabel('', parent=self.googleauthenticatewidget))
        self.googleauthenticatewidget_right_layout.addRow(QLabel('', parent=self.googleauthenticatewidget))
        self.googleauthenticatewidget_right_layout.addRow(self.google_authenticate_test_button, self.google_auth_copy_exception_button)
        self.google_authenticate_widget_layout.addLayout(self.googleauthenticatewidget_left_layout, stretch=1)
        self.google_authenticate_widget_layout.addLayout(self.googleauthenticatewidget_right_layout, stretch=1)
        if self.settings['project_id'] is None and self.settings['json_file'] is None and self.settings['service_account'] is None:
            self.google_authenticate_delete_button.hide()
        else:
            self.project_id_lineedit.blockSignals(True)
            self.service_account_lineedit.blockSignals(True)
            self.json_key_lineedit.blockSignals(True)
            self.project_id_lineedit.setText(self.settings['project_id'])
            self.project_id_lineedit.setCursorPosition(0)
            self.project_id_lineedit.setReadOnly(True)
            self.service_account_lineedit.setText(self.settings['service_account'])
            self.service_account_lineedit.setCursorPosition(0)
            self.service_account_lineedit.setReadOnly(True)
            self.json_key_lineedit.setText(os.path.basename(self.settings['json_file']))
            self.json_key_lineedit.setCursorPosition(0)
            self.json_key_lineedit.setReadOnly(True)
            self.google_authenticate_test_button.hide()
            self.json_key_choose_button.setEnabled(False)
            self.project_id_lineedit.blockSignals(False)
            self.service_account_lineedit.blockSignals(False)
            self.json_key_lineedit.blockSignals(False)
        return self.googleauthenticatewidget

    @Slot()
    def on_google_authenticate_delete_button_clicked(self):
        self.project_id_lineedit.setText('')
        self.project_id_lineedit.setReadOnly(False)
        self.service_account_lineedit.setText('')
        self.service_account_lineedit.setReadOnly(False)
        self.json_key_lineedit.setText('')
        self.json_key_lineedit.setReadOnly(False)
        self.json_key_choose_button.setEnabled(True)
        self.settings['project_id'] = None
        self.settings['service_account'] = None
        self.settings['json_file'] = None
        self.google_authenticate_test_button.show()
        self.write_settings('project_id', None)
        self.write_settings('service_account', None)
        self.write_settings('json_file', None)
        if self.temp_pdf_file is None:
            self.temp_pdf_file = self.get_pdf_html_from_db(pdf_id=1)
        self.webEngineView.load(QUrl.fromLocalFile(self.temp_pdf_file))
        self.google_authenticate_guidance_label.show()
        self.view_toolbutton.setEnabled(True)
        self.download_toolbutton.setEnabled(False)
        self.task_toolbutton.setEnabled(False)
        self.google_authenticate_delete_button.setEnabled(False)
        self.google_authenticate_delete_button.hide()

    @Slot()
    def on_google_auth_copy_exception_button_clicked(self):
        try:
            clipboard = QApplication.clipboard()
            clipboard.setText(str(self.test_result_dict['google_auth_test_exception']))
            self.show_msg_box('已复制错误信息到剪贴板')
        except:
            pass

    # noinspection PyUnresolvedReferences
    @Slot()
    def on_google_authenticate_test_button_clicked(self):
        if self.project_id_lineedit.text() == '':
            self.show_msg_box('必须输入项目ID')
        elif self.service_account_lineedit.text() == '':
            self.show_msg_box('必须输入服务账户名称')
        elif self.json_key_lineedit.text() == '':
            self.show_msg_box('必须提供json_key')
        elif not os.path.exists(self.json_key_lineedit.text()):
            self.show_msg_box('不存在的json_key路径')
        else:
            current_focus = self.focusWidget()
            self.google_auth_copy_exception_button.hide()
            project_id = self.project_id_lineedit.text()
            service_account = self.service_account_lineedit.text()
            json_file = self.json_key_lineedit.text()
            self.google_auth_test_movie.setScaledSize(QSize(self.google_authenticate_test_label.height(), self.google_authenticate_test_label.height()))
            self.google_authenticate_test_label.setMovie(self.google_auth_test_movie)
            self.google_authenticate_test_label.show()
            self.google_auth_test_movie.start()
            self.google_authenticate_test_button.setText("")
            self.set_pushbutton_special_stylesheet(self.google_authenticate_test_button, is_testing=True)
            if current_focus == self.google_authenticate_test_button:
                self.settingwidget.setFocus()
            # 保持焦点在原控件上（如果焦点不在按钮上）
            elif current_focus:
                current_focus.setFocus()
            self.testing_auth = (project_id, service_account, json_file)
            auth_test_process_threading = threading.Thread(target=self.test.google_auth_test, args=(project_id, self.test_result_dict, self.settings['proxies'],
                                                                                                    service_account, json_file))
            auth_test_process_threading.start()
            auth_test_timer = QTimer()
            auth_test_timer.timeout.connect(lambda: self.on_auth_testing(auth_test_timer, project_id))  # Connect timeout signal to slot
            auth_test_timer.start(500)

    def on_auth_testing(self, timer, project_id):
        try:
            result = self.test_result_dict['google_auth_test_result']
            google_auth_test_exception = str(self.test_result_dict['google_auth_test_exception'])
            if result is not None:
                self.test_result_dict['google_auth_test_result'] = None  # 立即置空测试结果，以防止干扰下次测试
                timer.stop()
                timer.deleteLater()
                if result:
                    self.set_pushbutton_special_stylesheet(self.google_authenticate_test_button)
                    if not os.path.isfile(os.path.join(self.setting_path, os.path.basename(self.testing_auth[2]))):
                        shutil.copy(self.testing_auth[2], self.setting_path)
                    destination = os.path.join(self.setting_path, os.path.basename(self.testing_auth[2]))
                    self.write_settings('project_id', self.testing_auth[0])
                    self.write_settings('service_account', self.testing_auth[1])
                    self.write_settings('json_file', destination)
                    self.settings['project_id'] = self.testing_auth[0]
                    self.settings['service_account'] = self.testing_auth[1]
                    self.settings['json_file'] = destination
                    initialization_ee_thread = threading.Thread(target=self.initialization_ee)
                    initialization_ee_thread.start()
                    self.webEngineView.load(QUrl.fromLocalFile(self.temp_html_file))
                    self.download_toolbutton.setEnabled(True)
                    self.task_toolbutton.setEnabled(True)
                    self.view_toolbutton.setEnabled(True)
                else:
                    if "cann't connect to google server" in google_auth_test_exception:
                        self.show_msg_box('无法连接到谷歌服务器，请检查你的VPN或网络设置')
                    elif "not found or deleted" in google_auth_test_exception:
                        self.show_msg_box('未找到该项目，请检查项目ID和服务账号是否存在拼写错误')
                    elif 'is not registered to use Earth Engine' in google_auth_test_exception:
                        self.show_msg_box('指定的项目尚未在谷歌地球引擎中注册<br>请在浏览器中完成注册后重试')
                        webbrowser.open(f'https://code.earthengine.google.com/register?project={project_id}')
                    self.set_pushbutton_special_stylesheet(self.google_authenticate_test_button, test_result=False)
                    self.google_auth_copy_exception_button.show()
                self.google_authenticate_test_label.hide()
                QTimer.singleShot(5000, lambda: self.resotre_google_auth_test_button(result))
        except Exception as e:
            pass

    def resotre_google_auth_test_button(self, result):
        self.google_authenticate_test_button.setText("测试认证信息")
        self.google_authenticate_test_button.setEnabled(True)
        if result:
            self.project_id_lineedit.setText(self.settings['project_id'])
            self.project_id_lineedit.setCursorPosition(0)
            self.project_id_lineedit.setReadOnly(True)
            self.service_account_lineedit.setText(self.settings['service_account'])
            self.service_account_lineedit.setCursorPosition(0)
            self.service_account_lineedit.setReadOnly(True)
            self.json_key_lineedit.setText(os.path.basename(self.settings['json_file']))
            self.json_key_lineedit.setCursorPosition(0)
            self.json_key_lineedit.setReadOnly(True)
            self.google_authenticate_guidance_label.hide()
            self.set_pushbutton_stylesheet(self.google_authenticate_test_button)
            self.google_authenticate_delete_button.show()
            self.google_authenticate_test_button.hide()
            self.json_key_choose_button.setEnabled(False)
            self.google_authenticate_delete_button.setEnabled(True)
            if self.settingwidget.isVisible():
                self.view_toolbutton.click()
        else:
            self.set_pushbutton_stylesheet(self.google_authenticate_test_button)
            self.google_authenticate_test_button.show()

    def on_google_authenticate_guidance_click(self, event):
        self.view_toolbutton.click()

    def license_widget(self):
        self.licensewidget = QWidget(parent=self.settingwidget)
        self.license_widget_layout = QHBoxLayout()
        self.license_widget_layout.setSpacing(60)
        self.license_widget_layout.setContentsMargins(30, 0, 30, 0)
        self.licensewidget.setLayout(self.license_widget_layout)
        # 左边新增许可widget
        self.licensewidget_left_layout = QFormLayout()
        self.licensewidget_left_layout.setSpacing(self.settingwidgetspacing)
        self.licensewidget_left_layout.setContentsMargins(0, 0, 0, 0)
        self.license_file_label = QLabel(text='许可文件', parent=self.licensewidget)
        self.set_label_stylesheet(self.license_file_label)
        self.license_file_lineedit = QLineEdit(parent=self.licensewidget)
        self.license_file_lineedit.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.set_lineedit_stylesheet(self.license_file_lineedit)
        self.license_file_lineedit.setReadOnly(True)
        self.licensewidget_left_layout.addRow(self.license_file_label, self.license_file_lineedit)
        # 右边导入许可部分，使用表单布局
        self.licensewidget_right_layout = QFormLayout()
        self.licensewidget_right_layout.setSpacing(self.settingwidgetspacing)
        self.licensewidget_right_layout.setContentsMargins(0, 0, 0, 0)
        self.generate_host_info_button = QPushButton(text='生成本机信息', parent=self.licensewidget, ObjectName='generate_host_info_button')
        self.set_pushbutton_stylesheet(self.generate_host_info_button)
        self.update_license_button = QPushButton(text='更新许可文件', parent=self.licensewidget, ObjectName='update_license_button')
        self.set_pushbutton_stylesheet(self.update_license_button)
        self.licensewidget_right_layout.addRow(self.generate_host_info_button, self.update_license_button)
        self.license_widget_layout.addLayout(self.licensewidget_left_layout, stretch=1)
        self.license_widget_layout.addLayout(self.licensewidget_right_layout, stretch=1)
        return self.licensewidget

    @Slot()
    def on_generate_host_info_button_clicked(self):
        path = self.choosepath()
        if path is not None:
            filename = self.license.write_computer_info(path)
            self.show_msg_box(f'已生成本机信息文件{filename}')
            try:
                if self.os_system == 'Windows':
                    os.startfile(path)
                elif self.os_system == 'Darwin':  # macOS
                    subprocess.call(['open', path])
                elif self.os_system == 'Linux':  # Linux
                    subprocess.call(['xdg-open', path])
            except:
                pass

    @Slot()
    def on_update_license_button_clicked(self):
        license_file = self.choosepath(is_license=True)
        if license_file != '':
            try:
                with open(license_file, 'r') as file:
                    xml_str = file.read()
                license_info = xmltodict.parse(xml_str)
                nee_license = license_info['root']['license']
                license_check, license_enddate = self.license.check_license(nee_license=nee_license)
                license_enddate = license_enddate[:4] + '-' + license_enddate[4:6] + '-' + license_enddate[6:8]
            except:
                license_check, license_enddate = False, None
            if license_check:
                try:
                    shutil.copy(license_file, self.license_path)
                except Exception as e:
                    print(e)
                license_chain_a, license_chain_b = self.license.regenerate_dna_chain(chain_a=nee_license)
                try:
                    keyring.delete_password(service_name='Nevasa', username='Nevasa_Master_password')
                except:
                    pass
                keyring.set_password(service_name='Nevasa', username='Nevasa_Master_password', password=license_chain_b)
                self.settings['license_path'] = str(os.path.basename(license_file))
                self.write_settings('license_path', str(os.path.basename(license_file)))
                self.license_file_lineedit.setText(f"{self.settings['license_path']}  (有效期至{license_enddate})")
                self.replace_widget(self.taskwidget, self.task_widget)
                self.stackwidget.setCurrentIndex(2)
                self.task_toolbutton.setEnabled(True)
                self.show_msg_box(f'已成功更新许可<br>有效期至{license_enddate}')
            else:
                self.show_msg_box(f'不正确的许可文件')

    def set_proxy(self, set_proxy: bool, proxy_id=None):
        if set_proxy:
            try:
                if proxy_id is None:
                    proxy = self.settings['proxies'][random.choice(list(self.settings['proxies'].keys()))]['http']
                else:
                    proxy = self.settings['proxies'][proxy_id]['http']
                if 'http' in proxy:
                    proxytype = QNetworkProxy.ProxyType.HttpProxy
                elif 'socks5' in proxy:
                    proxytype = QNetworkProxy.ProxyType.Socks5Proxy
                # 创建并设置 QNetworkProxy
                QNetworkProxy.setApplicationProxy(QNetworkProxy(proxytype, proxy.split("://")[1].split(":")[0], int(proxy.split(":")[2])))
                os.environ['https_proxy'] = proxy
            except Exception as e:
                QNetworkProxy.setApplicationProxy(QNetworkProxy())
        else:
            QNetworkProxy.setApplicationProxy(QNetworkProxy())

    def get_all_settings(self):
        def get_setting(key, function=None):
            try:
                if function is None:
                    self.settings[key] = settings['root'][key]
                else:
                    self.settings[key] = function(settings['root'][key])
            except:
                pass

        try:
            file_path = self.setting_path / "settings.xml"
            if os.path.exists(file_path):
                with open(file_path, 'r') as file:
                    xml_str = file.read()
                settings = xmltodict.parse(xml_str)
                get_setting(key='project_id')
                get_setting(key='loading_html_timeout_ms', function=int)
                get_setting(key='json_file')
                get_setting(key='service_account')
                get_setting(key='max_download_fail', function=int)
                get_setting(key='transparent_window', function=lambda value: True if value.lower() == 'true' else False)
                get_setting(key='enable_gpu', function=lambda value: True if value.lower() == 'true' else False)
                get_setting(key='max_display_task', function=int)
                get_setting(key='max_display_dataset', function=int)
                get_setting(key='map_opacity', function=int)
                get_setting(key='min_zoom_3d', function=int)
                get_setting(key='default_map_visualize', function=int)
                get_setting(key='max_download_try', function=int)
                get_setting(key='license_path')
                get_setting(key='select_task')
                get_setting(key='last_save_path')
                get_setting(key='when_click_closebutton')
                get_setting(key='when_close_application')
                get_setting(key='license_server_url')
                get_setting(key='max_proxy_server', function=lambda value: int(value) if int(value) < 5 else 4)
                try:
                    for proxy in settings['root']['proxies']:
                        if settings['root']['proxies'][proxy] is not None:
                            display_proxy_url = settings['root']['proxies'][proxy]
                            try:
                                username = settings['root']['proxies_username'][proxy]
                                password = keyring.get_password(f'Nevasa_proxy_{proxy}', username)
                                proxy_url = display_proxy_url.replace("//", f"//{username}:{password}@")
                            except:
                                proxy_url = display_proxy_url
                            display_proxy_url = {'http': display_proxy_url, 'https': display_proxy_url}
                            proxy_url = {'http': proxy_url, 'https': proxy_url}
                            self.settings['display_proxies'][proxy] = display_proxy_url
                            self.settings['proxies'][proxy] = proxy_url
                except:
                    pass
                try:
                    for proxy_username in settings['root']['proxies_username']:
                        if settings['root']['proxies_username'][proxy_username] is not None:
                            self.settings['proxies_username'][proxy_username] = settings['root']['proxies_username'][proxy_username]
                except:
                    pass
        except Exception as e:
            pass

    def choosepath(self, is_json_key=False, is_license=False, is_customregion_file=False):
        if is_json_key:
            jsonpath = QFileDialog.getOpenFileName(self.settingwidget, caption="请选择json-key", dir='',
                                                   filter="Json文件 (*.json);;所有文件 (*)", options=QFileDialog.Option.ReadOnly)
            if jsonpath:
                self.json_key_lineedit.setText(jsonpath[0])
                self.json_key_lineedit.setCursorPosition(0)
        elif not is_json_key and not is_license and not is_customregion_file:
            savepath = QFileDialog.getExistingDirectory(self.settingwidget, caption="请选择文件夹", dir=self.settings['last_save_path'])
            if savepath:
                self.savepath_lineEdit.setText(savepath)
                self.savepath_lineEdit.setCursorPosition(0)
                return savepath
        elif is_license:
            licensepath = QFileDialog.getOpenFileName(self.settingwidget, caption="请选择许可文件", dir='',
                                                      filter="lic文件 (*.lic);;所有文件 (*)", options=QFileDialog.Option.ReadOnly)
            if licensepath:
                return licensepath[0]
        elif is_customregion_file:
            filepath = QFileDialog.getOpenFileName(self.settingwidget, caption="请选择数据源文件", dir='',
                                                   filter="所有支持的文件类型 (*.shp;*.json;*.geojson;*.gpkg;*.kml;*.gml);;Shapefile文件 (*.shp);;"
                                                          "GeoJson文件 (*.json;*.geojson);;GeoPackage文件 (*.gpkg);;KML文件 (*.kml);;"
                                                          "GML文件 (*.gml);;所有文件 (*)", options=QFileDialog.Option.ReadOnly)
            if filepath:
                self.customregion_filename_lineedit.setText(filepath[0])
                self.customregion_filename_lineedit.setCursorPosition(0)

    def write_settings(self, element_tag, element_value):
        file_path = self.setting_path / "settings.xml"
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                xml_str = file.read()
            settings = xmltodict.parse(xml_str)
        else:
            settings = {'root': {}}
        settings['root'][element_tag] = element_value
        xml_str = xmltodict.unparse(settings, pretty=True)
        with open(file_path, 'w') as f:
            f.write(xml_str)

    @staticmethod
    def new_id():
        new_id = 'q' + str(uuid.uuid4()).replace('-', '')
        return new_id

    def get_all_tasks(self):
        self.exists_task = pd.DataFrame(columns=['index', 'datasetname', 'createtime', 'data_source_lv1', 'data_source_lv2', 'data_source_lv3',
                                                 'base_map', 'datarangetype', 'picked_sn', 'picked_sh', 'picked_xn', 'data_scale', 'start_date',
                                                 'end_date', 'downloadpath', 'taskname', 'finishtime', 'downloadprogress', 'stitchprogress', 'cropprogress'
                                                                                                                                             'is_CalculateTiles_done',
                                                 'is_TileDownload_done', 'is_TileStitch_done', 'speed_calculate_list', 'is_complete',
                                                 'is_executing', 'is_download_failed', 'remaining_time', 'this_start_time', 'bands', 'datasetregion'])
        try:
            task_file = self.task_path
            xml_files_with_time = [(file.name, file.stat().st_ctime) for file in task_file.iterdir() if file.is_file() and file.suffix == '.xml']
            # 按创建时间排序，降序排序，即最近创建的文件在前
            xml_files_with_time.sort(key=lambda x: x[1], reverse=True)
            # 获取创建时间最近的 N 个文件
            latest_xml_files = [file[0] for file in xml_files_with_time[:self.settings['max_display_task']]]
            for task_xml in latest_xml_files:
                try:
                    task = pd.read_xml(task_file / task_xml)
                    if task.loc[0, 'is_CalculateTiles_done'] and task.loc[0, 'is_TileDownload_done'] and task.loc[0, 'is_TileStitch_done']:
                        task['is_complete'] = True
                    else:
                        task['is_complete'] = False
                    task['is_executing'], task['is_download_failed'] = False, False
                    task['remaining_time'], task['this_start_time'], task['speed_calculate_list'] = None, None, None
                    self.exists_task = (
                        self.exists_task.copy() if task.empty else task.copy() if self.exists_task.empty else pd.concat([self.exists_task, task]))
                except Exception as e:
                    pass
            self.sort_downloadtasks()
        except Exception as e:
            pass

    def sort_downloadtasks(self, tasks=None):
        def sort(tasks):
            tasks["sort_key"] = tasks.apply(
                lambda row: (
                    0 if row["is_executing"]
                    else 1 if (not row["is_executing"] and not row["is_complete"])
                    else 2
                ),
                axis=1  # 正确位置
            )
            tasks = tasks.sort_values(
                by=["sort_key", "createtime"],
                ascending=[True, False]
            ).drop(columns=["sort_key"])
            return tasks

        if tasks is None:
            tasks = self.exists_task
            self.exists_task = sort(tasks)
        else:
            return sort(tasks)

    @staticmethod
    def is_valid_path(path):
        if os.path.exists(path):
            return True
        else:
            try:
                os.makedirs(path)
                return True
            except:
                return False

    @staticmethod
    def to_geojson(wkt_string):
        shape = loads(wkt_string)  # 将 WKT 转换为 Shapely 对象
        geojson_geometry = shape.__geo_interface__  # 将 Shapely 对象转换为 GeoJSON 字典
        geojson_string = geojson.dumps(geojson_geometry, indent=2)  # 将 GeoJSON 集合输出为字符串
        return geojson_string

    def show_msg_box(self, message):
        msg_box = QMessageBox()
        msg_box.setWindowIcon(self.mainwindow.icon)
        msg_box.setFixedSize(300, 150)
        msg_box.setWindowTitle("提示")
        self.set_messagebox_stylesheet(msg_box)
        msg_box.setIcon(QMessageBox.Icon.Critical)
        msg_box.setText(message)
        msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg_box.exec()

    def get_all_datasets(self):
        self.exists_dataset = pd.DataFrame(columns=['index', 'datasetname', 'createtime', 'data_source_lv1', 'data_source_lv2', 'data_source_lv3',
                                                    'datasetregion', 'base_map',
                                                    'datarangetype', 'picked_sn', 'picked_sh', 'picked_xn', 'data_scale', 'start_date', 'end_date'], )
        try:
            dataset_file = self.dataset_path
            xml_files_with_time = [(file.name, file.stat().st_ctime) for file in dataset_file.iterdir() if file.is_file() and file.suffix == '.xml']
            # 按创建时间排序，降序排序，即最近创建的文件在前
            xml_files_with_time.sort(key=lambda x: x[1], reverse=True)
            # 获取创建时间最近的 N 个文件
            latest_xml_files = [file[0] for file in xml_files_with_time[:self.settings['max_display_dataset']]]
            for dataset_xml in latest_xml_files:
                try:
                    dataset = pd.read_xml(dataset_file / dataset_xml)
                    self.exists_dataset = (self.exists_dataset.copy() if dataset.empty
                                           else dataset.copy() if self.exists_dataset.empty else pd.concat([self.exists_dataset, dataset]))
                except Exception as e:
                    pass
        except:
            pass

    def set_toolbutton_stylesheet(self, button, hover=True):
        if hover:
            try:
                if not self.settings['transparent_window'] or button.parent() == self.creategeemapwidget:
                    color = 'black'
                else:
                    color = 'white'
            except:
                color = 'white'
            button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextUnderIcon)
            button.setStyleSheet(f"""
                    QToolButton {{
                        color: {color};
                        background-color: transparent;  /* 默认背景 */
                        border: none;
                        border-radius: 10px;
                        padding: 0;  /* 去除内边距 */
                    }}
                    QToolButton:hover {{
                        color: black;
                        background-color: lightblue;  /* 鼠标进入时背景 */
                    }}
                    QToolButton:pressed {{
                        background-color: lightblue;  /* 鼠标按下时背景 */
                        border: none;
                        border-radius: 10px;
                        padding: 0;  /* 去除内边距 */
                    }}
            """)
        else:
            button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonIconOnly)
            button.setStyleSheet("""
                    QToolButton {
                        background-color: lightblue;  /* 默认背景 */
                        border: none;
                        border-radius: 10px;
                        padding: 0;  /* 去除内边距 */
                    }
                    QToolButton:pressed {
                        background-color: lightblue;  /* 鼠标按下时背景 */
                    }
            """)

    def set_radiobutton_stylesheet(self, button):
        if not self.settings['transparent_window']:
            color = 'blue'
        else:
            color = 'pink'
        button.setStyleSheet(f"""
                QRadioButton {{
                    color: {self.color};
                }}
                QRadioButton:disabled {{
                    color: gray;
                }}
                QRadioButton::indicator {{
                    width: 10px;
                    height: 10px;
                    border: 0.5px solid gray;
                    border-radius: 5px;
                }}
                QRadioButton::indicator:checked {{
                    background-color: #70eaaf;
                    border: 0.5px solid {color};
                    border-radius: 5px;
                }}
                QRadioButton::indicator:hover {{
                    border: 0.5px solid {color};  /* 鼠标悬停时边框变为蓝色 */
                }}
                QRadioButton::indicator:disabled {{
                    background-color: lightgray;
                    border: 0.5px solid gray;
                }}
        """)

    def set_label_stylesheet(self, label, is_guidance_link=False):
        if is_guidance_link:
            label.setStyleSheet("""
                        color: blue; 
                        QLabel {
                            font-size: 18px;
                            padding: 20px;
                        }
                        QLabel:hover {
                            background-color : lightgreen;
                        }
                    """)
        else:
            try:
                if (not self.settings['transparent_window'] or label.parent() == self.creategeemapwidget or label.parent() == self.newtaskwidget or
                        label.parent() == self.customregion_downloadwidget):
                    color = 'black'
                else:
                    color = 'white'
            except:
                color = 'white'
            label.setStyleSheet(f"""
                        color: {color}; 
                        QLabel {{
                           font-size: 18px;
                           padding: 20px;
                        }}
                    """)

    @staticmethod
    def set_checkbox_stylesheet(checkbox):
        checkbox.setStyleSheet("""
                QCheckBox {
                    border: none;
                    border-radius: 3px; /* 圆角 */
                }
                QCheckBox::indicator:unchecked {
                    border: 0.5px solid rgb(219,219,219);
                    background-color: rgb(243,243,243); /* 未选中时背景颜色 */
                    border-radius: 3px;
                }
        """)

    @staticmethod
    def set_lineedit_stylesheet(lineedit):
        lineedit.setStyleSheet("""
                QLineEdit {
                    border: 0.5px solid rgb(219,219,219); /* 边框颜色和宽度 */
                    border-radius: 3px; /* 圆角 */
                    padding: 0.5px; /* 内边距 */
                    background-color: #f0f0f0; /* 背景颜色 */
                    color: rgb(0,0,0);
                }
                QLineEdit:focus {
                    border: 0.5px solid rgb(219,219,219); /* 聚焦时边框颜色 */
                }
                QLineEdit:disabled {
                    background-color: rgb(199,199,199); /* 禁用状态背景色 */
                    color: rgb(120,120,120); /* 禁用状态文本颜色 */
                }
        """)

    @staticmethod
    def set_textedit_stylesheet(textedit):
        textedit.setStyleSheet("""
                QTextEdit {
                    border: 0.5px solid rgb(219,219,219); /* 边框颜色和宽度 */
                    border-radius: 3px; /* 圆角 */
                    padding: 0.5px; /* 内边距 */
                    background-color: #f0f0f0; /* 背景颜色 */
                    color: rgb(0,0,0);
                }
                QTextEdit:focus {
                    border: 0.5px solid rgb(219,219,219); /* 聚焦时边框颜色 */
                }
                QTextEdit:disabled {
                    background-color: rgb(199,199,199); /* 禁用状态背景色 */
                    color: rgb(120,120,120); /* 禁用状态文本颜色 */
                }
                QScrollBar:vertical {
                    border: none; /* 去掉边框 */
                    background: rgb(255,255,255); /* 滚动条背景颜色 */
                    width: 10px; /* 滚动条宽度 */
                    margin: 0px 0 0px 0; /* 滚动条上下间距 */
                }
                QScrollBar::handle:vertical {
                    background: rgb(219,219,219); /* 滚动条滑块颜色 */
                    min-height: 20px; /* 滚动条滑块最小高度 */
                    border-radius: 3px; /* 滚动条滑块圆角 */
                }
                QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                    background: none; /* 上下按钮背景 */
                    height: 0px; /* 不显示按钮 */
                }
                QScrollBar::handle:vertical:hover {
                    background: lightblue; /* 滚动条滑块悬停颜色 */
                }
        """)

    def set_pushbutton_stylesheet(self, pushbutton):
        try:
            if pushbutton != self.json_key_choose_button:
                background = 'rgb(199,199,199)'
                color = 'rgb(120,120,120)'
            else:
                color = 'black'
                background = 'rgb(243,243,243)'
        except:
            color = 'rgb(120,120,120)'
            background = 'rgb(199,199,199)'
        pushbutton.setStyleSheet(f"""
                QPushButton {{
                    background-color: rgb(243,243,243); /* 正常状态背景色 */
                    color: black; /* 正常状态文本颜色 */
                    border: 0.5px solid rgb(219,219,219); /* 边框颜色 */
                    border-radius: 3px; /* 圆角 */
                    padding: 0px; /* 内边距 */
                }}
                QPushButton:hover {{
                    background-color: lightblue; /* 悬停时背景色 */
                }}
                QPushButton:pressed {{
                    background-color: lightblue; /* 按下时背景色 */
                }}
                QPushButton:disabled {{
                    background-color: {background}; /* 禁用状态背景色 */
                    color: {color}; /* 禁用状态文本颜色 */
                }}
        """)

    @staticmethod
    def set_pushbutton_special_stylesheet(pushbutton, test_result=True, is_testing=False):
        pushbutton.setEnabled(False)
        if is_testing:
            pushbutton.setText('')
            pushbutton.setStyleSheet("""
                QPushButton:disabled {
                    border: 0.5px solid rgb(219,219,219); /* 边框颜色 */
                    border-radius: 3px; /* 圆角 */
                    background-color: rgb(243,243,243); /* 禁用状态背景色 */
                    color: rgb(120,120,120); /* 禁用状态文本颜色 */
                }
            """)
        else:
            if test_result:
                pushbutton.setText('✔')
                pushbutton.setStyleSheet("""
                    QPushButton:disabled {
                        border: none; /* 边框颜色 */
                        border-radius: 3px; /* 圆角 */
                        background-color: #98FB98; /* 禁用状态背景色 */
                        color: black; /* 禁用状态文本颜色 */
                    }
                """)
            else:
                pushbutton.setText('✕')
                pushbutton.setStyleSheet("""
                    QPushButton:disabled {
                        border: none; /* 边框颜色 */
                        border-radius: 3px; /* 圆角 */
                        background-color: #F08080; /* 禁用状态背景色 */
                        color: black; /* 禁用状态文本颜色 */
                    }
                """)

    @staticmethod
    def set_combobox_stylesheet(combobox):
        combobox.setStyleSheet("""
                QComboBox {
                    border: 0.5px solid rgb(219,219,219); /* 边框颜色和宽度 */
                    border-radius: 3px; /* 圆角 */
                    padding: 0.5px; /* 内边距 */
                    background-color: #f0f0f0; /* 背景颜色 */
                    color: black;
                    font-size: 12px;
                }
                QComboBox:disabled {
                    background-color: rgb(199,199,199); /* 禁用状态背景颜色 (灰色) */
                    color: rgb(120,120,120); /* 禁用状态文本颜色 (浅灰色) */
                }
                QComboBox::drop-down {
                    border: 0.5px solid rgb(219,219,219); /* 去掉边框 */
                    width: 15px; /* 下拉按钮宽度 */
                }
                QComboBox::down-arrow {
                    image: url('resources/icon/arrow.png');
                    width: 15px; /* 箭头宽度 */
                    height: 15px; /* 箭头高度 */
                }
                QScrollBar:vertical {
                    border: none; /* 去掉边框 */
                    background: rgb(255,255,255); /* 滚动条背景颜色 */
                    width: 10px; /* 滚动条宽度 */
                    margin: 0px 0 0px 0; /* 滚动条上下间距 */
                }
                QScrollBar::handle:vertical {
                    background: rgb(219,219,219); /* 滚动条滑块颜色 */
                    min-height: 20px; /* 滚动条滑块最小高度 */
                    border-radius: 3px; /* 滚动条滑块圆角 */
                }
                QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                    background: none; /* 上下按钮背景 */
                    height: 0px; /* 不显示按钮 */
                }
                QScrollBar::handle:vertical:hover {
                    background: lightblue; /* 滚动条滑块悬停颜色 */
                }
                QComboBox QAbstractItemView {
                    background-color: rgb(243,243,243); /* 下拉框背景颜色 */
                    border: 0.5px solid rgb(219,219,219); /* 下拉框边框 */
                    selection-background-color: rgb(164,186,206); /* 选中项背景颜色 */
                    selection-color: black; /* 选中项文本颜色 */
                    padding: 0px; /* 内边距 */
                    color: black;
                }
        """)

    @staticmethod
    def set_scrollarea_stylesheet(scroll_area):
        scroll_area.setStyleSheet("""
                QScrollArea {
                    background-color: transparent;
                    border: none; /* 滚动区域边框 */
                }
                QScrollBar:vertical {       /* 垂直滚动条 */
                    border: none; /* 去掉边框 */
                    background: rgb(255,255,255); /* 滚动条背景颜色 */
                    width: 10px; /* 滚动条宽度 */
                    margin: 0px 0 0px 0; /* 滚动条上下间距 */
                }
                QScrollBar::handle:vertical {
                    background: rgb(219,219,219); /* 滚动条滑块颜色 */
                    min-height: 20px; /* 滚动条滑块最小高度 */
                    border-radius: 3px; /* 滚动条滑块圆角 */
                }
                QScrollBar::add-line:vertical,
                QScrollBar::sub-line:vertical {
                    background: none;        /* 滚动条上下按钮背景 */
                    height: 0;               /* 隐藏按钮 */
                }
                QScrollBar::up-arrow:vertical,
                QScrollBar::down-arrow:vertical {
                    background: none;        /* 隐藏箭头 */
                }
                QScrollBar:horizontal {      /* 水平滚动条滑块 */
                    border: none; /* 去掉边框 */
                    background: rgb(255,255,255); /* 滚动条背景颜色 */
                    width: 10px; /* 滚动条宽度 */
                    margin: 0px 0 0px 0; /* 滚动条上下间距 */
                }
                QScrollBar::handle:horizontal {
                    background: rgb(219,219,219); /* 滚动条滑块颜色 */
                    min-height: 20px; /* 滚动条滑块最小高度 */
                    border-radius: 3px; /* 滚动条滑块圆角 */
                }
                QScrollBar::add-line:horizontal,
                QScrollBar::sub-line:horizontal {
                    background: none;        /* 隐藏按钮 */
                    width: 0;                /* 隐藏按钮 */
                }
                QScrollBar::left-arrow:horizontal,
                QScrollBar::right-arrow:horizontal {
                    background: none;        /* 隐藏箭头 */
                }
        """)

    @staticmethod
    def set_messagebox_stylesheet(messagebox):
        # messagebox.setWindowFlags(messagebox.windowFlags() | QtCore.Qt.WindowType.FramelessWindowHint)
        button_box = messagebox.findChild(QDialogButtonBox)
        messagebox.setStyleSheet("""
                background-color: rgb(243, 243, 243);
                color: black;
            """)
        button_box.setStyleSheet("""
                QPushButton {
                    background-color: rgb(243,243,243); /* 正常状态背景色 */
                    color: black; /* 正常状态文本颜色 */
                    border: 0.5px solid rgb(219,219,219); /* 边框颜色 */
                    border-radius: 3px; /* 圆角 */
                    padding: 0px; /* 内边距 */
                    min-width: 60px;  /* 设置最小宽度 */
                    min-height: 20px;  /* 设置最小高度 */
                    font-size: 12px; /* 字体大小 */
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
    def set_calendar_stylesheet(calendar):
        calendar.setVerticalHeaderFormat(QCalendarWidget.VerticalHeaderFormat.NoVerticalHeader)
        calendar.setHorizontalHeaderFormat(QCalendarWidget.HorizontalHeaderFormat.ShortDayNames)
        calendar.setStyleSheet("""
                    QCalendarWidget {
                        background-color: rgb(243,243,243);
                        border: 1px solid #c4c4c4;
                        border-radius: 10px;
                    }
                    QCalendarWidget QToolButton {
                        font-size: 14px;
                        height: 20px;
                        width: 50px;
                        border-radius: 3px;
                        color: black;
                    }
                    /* 设置切换按钮的悬停和按下效果 */
                    QCalendarWidget QToolButton:hover {
                        background-color: #d0d0d0;
                    }
                    QCalendarWidget QToolButton:pressed {
                        background-color: #c0c0c0;
                    }
                """)
        prev_button = calendar.findChild(QWidget, "qt_calendar_prevmonth")
        next_button = calendar.findChild(QWidget, "qt_calendar_nextmonth")
        # 隐藏这两个按钮
        if prev_button:
            prev_button.setVisible(False)
        if next_button:
            next_button.setVisible(False)
