#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2024/8/17

import os
import random
import ee
from PySide6 import QtWidgets, QtCore
import requests


class Backend(QtCore.QObject):
    def __init__(self, main_instance):
        super().__init__()
        self.main_instance = main_instance
        self.map_zoom = 4
        self.map_center = [36.452220, 96.951170]
        self.main_instance.polygon_from_js['polygon'] = None

    @QtCore.Slot(list)
    def coordinate_from_js(self, coordinate):
        self.main_instance.lon.setText("经度：" + f'{coordinate[0]:.5f}')
        self.main_instance.lat.setText("纬度：" + f'{coordinate[1]:.5f}')

    @QtCore.Slot(int)
    def zoom_from_js(self, zoom):
        self.main_instance.zoom.setText("层级：" + str(zoom))

    @QtCore.Slot(str)
    def polygon_from_js(self, polygon):
        self.main_instance.polygon_from_js['polygon'] = polygon


class AuthAndKeyTest:
    def __init__(self):
        self.test_url = 'https://www.google.com'

    def proxy_test(self, proxy, test_result):
        try:
            response = requests.get(self.test_url, proxies=proxy, timeout=10)  # 设置请求超时时间为10秒
            code = response.status_code
            if code == 200:
                result = True
            else:
                result = False
        except Exception as e:
            print(e)
            result = False
        test_result['proxy_test_result'] = result

    def google_auth_test(self, project, test_result, proxy=None, service_account=None, json_file=None):
        if proxy is not None and len(proxy) > 0:
            random_value = random.choice(list(proxy.values()))
            os.environ['HTTPS_PROXY'] = random_value['http']
        try:
            response = requests.get(self.test_url, proxies=proxy, timeout=10)  # 设置请求超时时间为10秒
            code = response.status_code
            if code == 200:
                network_test_result = True
            else:
                network_test_result = False
        except Exception as e:
            network_test_result = False
        if network_test_result:
            try:
                credentials = ee.ServiceAccountCredentials(service_account, json_file)
                ee.Initialize(credentials=credentials, project=project)
                result = True
                test_result['google_auth_test_exception'] = None
            except Exception as e:
                result = False
                test_result['google_auth_test_exception'] = e
            test_result['google_auth_test_result'] = result
        else:
            result = False
            test_result['google_auth_test_exception'] = Exception("cann't connect to google server, please check ur network or VPN")
            test_result['google_auth_test_result'] = result


if __name__ == '__main__':
    a = AuthAndKeyTest()
    test_result = {}
    a.google_auth_test(proxy={'qweq': {'http': 'http://127.0.0.1:10809', 'https': 'http://127.0.0.1:10809'}},
                       service_account='ee-test@ee-test-objective.iam.gserviceaccount.com',
                       json_file='C:\\Users\\B_Snowflake\\Downloads\\ee-test-objective-b17955d7634c.json',
                       project='ee-test-objective', test_result=test_result)
    print(test_result)
