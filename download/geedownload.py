#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2024/11/7

import math
import multiprocessing
import os
import queue as q
import random
import sqlite3
import time
import cv2
import geemap
import ee
import numpy
import pandas as pd
import requests
import shapely
import concurrent.futures
from requests.adapters import HTTPAdapter
from shapely.validation import make_valid
from shapely.wkt import dumps, loads
from dbutils.pooled_db import PooledDB
from urllib3 import Retry
from threading import Condition
from map_engine import map_engine


class GeeImageCalculate:
    def __init__(self, taskname: str, proxies, ee_object, savepath: str, polygon: str, ee_initialize, start_date, end_date, scale, bands,
                 calculate_progress_info, calculate_process_done, queue):
        self.ee_initialize = ee_initialize
        self.bands = bands
        self.network_session = None
        self.database_session = None
        self.ee_object = ee_object
        if scale is not None:
            self.scale = int(scale)
        else:
            self.scale = scale
        self.taskname = taskname
        self.proxies = proxies
        self.all_proxies = proxies
        self.start_date = start_date
        self.end_date = end_date
        self.savepath = savepath
        self.polygon = make_valid(loads(polygon))
        self.customlandcover = map_engine.CustomLandcover()
        x_min, y_min, x_max, y_max = self.polygon.bounds
        # 计算矩形框（左上点、右下点）
        wkt_polygon = f"polygon(({x_min} {y_min},{x_min} {y_max},{x_max} {y_max},{x_max} {y_min},{x_min} {y_min}))"
        self.geometry_rec = make_valid(loads(wkt_polygon))
        self.rec = (x_min, y_max, x_max, y_min)
        self.database_conn()
        self.get_proxies()
        self.create_network_session()
        self.is_rectangle = self.geometry_rec.equals(self.polygon)
        self.final_download_tiles = []
        self.progress_info = calculate_progress_info
        self.queue = queue
        self.process_done = calculate_process_done

    def write_to_queue(self):
        data = (self.progress_info, self.process_done, self.taskname)
        self.queue.put_nowait(data)

    def database_conn(self):
        if os.path.exists(os.path.join(self.savepath, f'{self.taskname}.nev')):
            os.remove(os.path.join(self.savepath, f'{self.taskname}.nev'))
        self.database_session = sqlite3.connect(os.path.join(self.savepath, f'{self.taskname}.nev'), check_same_thread=False)

    def insert_task_info(self):
        if self.bands is None:
            channels = 3
            is_raster = False
            bands = None
        else:
            if self.ee_object == "Global Multi-resolution Terrain":
                channels = 1
                is_raster = True
                bands = ','.join(band for band in self.bands)
            else:
                channels = 3
                is_raster = False
                bands = ','.join(band for band in self.bands)
        cur = self.database_session.cursor()
        cur.execute('create table task_info(channels int, is_raster bool, bands text, dtype text)')
        cur.execute('insert into task_info(channels,is_raster,bands) values(?,?,?)', (channels, is_raster, bands))
        self.database_session.commit()

    def create_network_session(self):
        self.network_session = requests.Session()
        retries = Retry(total=10, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(pool_maxsize=50, max_retries=retries)
        self.network_session.mount('http://', adapter)
        self.network_session.mount('https://', adapter)
        # 连接到VPN
        self.network_session.proxies = self.proxies
        return self.network_session

    def get_proxies(self):
        proxies_list = {k: v for k, v in self.all_proxies.items() if 'http' in v}
        random_value = random.choice(list(proxies_list.values()))
        self.proxies = random_value

    @staticmethod
    def get_tile_geometry(tilex, tiley, zoom, buffer=False, distance=None):
        # 根据瓦片编号，生成该瓦片的geometry，以用于空间计算
        n = 2 ** zoom
        x_min = tilex / n * 360.0 - 180.0
        y_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * tiley / n))))
        x_max = ((tilex + 1) / n * 360.0 - 180.0)
        y_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (tiley + 1) / n))))
        wkt_polygon = f"polygon(({x_min} {y_min},{x_min} {y_max},{x_max} {y_max},{x_max} {y_min},{x_min} {y_min}))"
        if not buffer:
            tile_geometry = loads(wkt_polygon)
        else:
            tile_geometry = loads(wkt_polygon).buffer(distance=distance)
        return tile_geometry

    @staticmethod
    def wkt_to_eegeometry(wkt):
        shape = loads(wkt)
        # 如果是多边形（Polygon）
        if isinstance(shape, shapely.geometry.Polygon):
            # 获取外部坐标和内部孔的坐标
            coordinates = [list(shape.exterior.coords)] + [list(interior.coords) for interior in shape.interiors]
            return ee.Geometry.Polygon(coordinates)
        # 如果是多多边形（MultiPolygon）
        elif isinstance(shape, shapely.geometry.MultiPolygon):
            # 获取每个多边形的坐标（外部边界和内部孔）
            multipolygon_coordinates = []
            for polygon in shape.geoms:
                coordinates = [list(polygon.exterior.coords)] + [list(interior.coords) for interior in polygon.interiors]
                multipolygon_coordinates.append(coordinates)
            return ee.Geometry.MultiPolygon(multipolygon_coordinates)

    @staticmethod
    def latlon_to_tile(lat, lon, zoom):
        """根据经纬度和缩放级别计算瓦片坐标"""
        n = 2 ** zoom
        # Web Mercator 坐标系 (EPSG:3857)
        tile_x = int((lon + 180) / 360 * n)
        tile_y = int((1 - math.log(math.tan(math.radians(lat)) + (1 / math.cos(math.radians(lat)))) / math.pi) / 2 * n)
        return tile_x, tile_y

    def get_tile_form_geometry(self, zoom_level):
        """从WKT几何字符串计算该区域中心点所在的瓦片编号"""
        # 获取几何的中心点 (center)
        center = self.polygon.centroid
        return self.latlon_to_tile(center.y, center.x, zoom_level)

    def eeobject_download_test(self, zoom):
        tile_x, tile_y = self.get_tile_form_geometry(zoom)
        self.buffer_distance = {
            '10': 0.0018,
            '11': 0.0006,
            '12': 0.00048,
            '13': 0.00024,
            '14': 0.00012,
            '15': 0.00006,
            '16': 0.00003,
            '17': 0.000012,
            '18': 0.000006,
            '19': 0.0000036,
            '20': 0.0000018,
            '21': 0.0000006,
        }
        region = self.wkt_to_eegeometry(dumps(self.get_tile_geometry(tile_x, tile_y, zoom)))
        region_buffer = self.wkt_to_eegeometry(dumps(self.get_tile_geometry(tile_x, tile_y, zoom, True, self.buffer_distance[str(zoom)])))
        if self.ee_object == "Dynamic World":
            ee_object = self.customlandcover.dynamic_world(start_date=self.start_date, end_date=self.end_date)
        elif self.ee_object == 'JRC Monthly Water History':
            ee_object = self.customlandcover.jrc_monthly_water_history(start_date=self.start_date, end_date=self.end_date)
        elif self.ee_object == 'Global Multi-resolution Terrain':
            ee_object = ee.Image('USGS/GMTED2010_FULL').select(self.bands[0])
        elif self.ee_object == 'CFSV2':
            ee_object = self.customlandcover.CFSV2(start_date=self.start_date, end_date=self.end_date, band=self.bands[0])
        try:
            buffer_image = geemap.ee_to_numpy(ee_object=ee_object, scale=self.scale, region=region_buffer)
            image = geemap.ee_to_numpy(ee_object=ee_object, scale=self.scale, region=region)
            if image.shape[-1] == 3:  # 如果是 RGB 图像
                image = cv2.cvtColor(image, cv2.COLOR_RGB2BGR)  # 转换为 BGR 格式（OpenCV 默认格式）
            # 获取图像的高度和宽度
            self.tile_height, self.tile_width = image.shape[:2]
            return True
        except Exception as e:
            print(e)
            if 'must be less than or equal to' in str(e):
                return False
            else:
                return True

    def download_size_test(self):
        test_zoom_list = [k for k in range(10, 21)]
        for k in test_zoom_list:
            test_result = self.eeobject_download_test(k)
            if test_result:
                self.downloadzoom = (k,)
                return k

    def get_all_child_tiles(self, tilex, tiley, zoom):
        # 对于空间关系为包含或不相交的瓦片，计算其所有下级瓦片编号
        all_child_tiles = []
        geometry = None
        for downloadzoom in self.downloadzoom:
            if downloadzoom > zoom:
                child_tilex_min = tilex * (2 ** (downloadzoom - zoom))
                child_tiley_min = tiley * (2 ** (downloadzoom - zoom))
                child_tilex_max = (tilex + 1) * (2 ** (downloadzoom - zoom)) - 1
                child_tiley_max = (tiley + 1) * (2 ** (downloadzoom - zoom)) - 1
                for x in range(child_tilex_min, child_tilex_max + 1):
                    for y in range(child_tiley_min, child_tiley_max + 1):
                        all_child_tiles.append((x, y, downloadzoom, geometry))
        return all_child_tiles

    @staticmethod
    def get_rec_info(rec, zoom):
        # 根据矩形经纬度计算行列号,仅计算指定级别下的行列号编码
        n = 2 ** zoom
        start_tilex = int((rec[0] + 180) / 360 * n)
        start_tiley = int((1 - math.log(math.tan(math.radians(rec[1])) + (1 / math.cos(math.radians(rec[1])))) / math.pi) / 2 * n)
        end_tilex = int((rec[2] + 180) / 360 * n)
        end_tiley = int((1 - math.log(math.tan(math.radians(rec[3])) + (1 / math.cos(math.radians(rec[3])))) / math.pi) / 2 * n)
        return start_tilex, end_tilex, start_tiley, end_tiley, zoom

    def get_child_tiles(self, tilex, tiley, zoom):
        # 对于空间关系为相交的瓦片，计算其下一级的瓦片编号
        child_tiles = []
        next_zoom = zoom + 1
        geometry = None
        if next_zoom <= max(self.downloadzoom):
            child_tilex_min = tilex * (2 ** (next_zoom - zoom))
            child_tiley_min = tiley * (2 ** (next_zoom - zoom))
            child_tilex_max = (tilex + 1) * (2 ** (next_zoom - zoom)) - 1
            child_tiley_max = (tiley + 1) * (2 ** (next_zoom - zoom)) - 1
            for x in range(child_tilex_min, child_tilex_max + 1):
                for y in range(child_tiley_min, child_tiley_max + 1):
                    child_tiles.append((x, y, next_zoom, geometry))
        return tuple(child_tiles)

    def worker(self):
        os.environ['https_proxy'] = self.proxies['http']
        self.insert_task_info()
        credentials = ee.ServiceAccountCredentials(self.ee_initialize[0], self.ee_initialize[1])
        ee.Initialize(credentials=credentials, project=self.ee_initialize[2])
        self.download_size_test()
        if not self.is_rectangle:
            self.tile_calculate_task()
            for task in self.task_list:
                results = self.tile_spatial_calculate(task)
                self.final_download_tiles.extend(results[0])
                self.task_list.extend(results[1])
                # download_tiles_dataframe = pd.DataFrame(data=self.final_download_tiles, columns=['x', 'y', 'z', 'geometry'])
                if len(self.final_download_tiles) > 50000:
                    download_tiles_dataframe = pd.DataFrame(data=self.final_download_tiles, columns=['x', 'y', 'z', 'geometry'])
                    self.to_sqlite(download_tiles_dataframe)
                    self.final_download_tiles = []
        else:
            for zoom in self.downloadzoom:
                start_tilex, end_tilex, start_tiley, end_tiley, zoom = self.get_rec_info(self.rec, zoom)
                for i in range(start_tilex, end_tilex + 1):
                    for j in range(start_tiley, end_tiley + 1):
                        self.final_download_tiles.append((i, j, zoom, None))
                        # download_tiles_dataframe = pd.DataFrame(data=self.final_download_tiles, columns=['x', 'y', 'z', 'geometry'])
                        if len(self.final_download_tiles) > 50000:
                            download_tiles_dataframe = pd.DataFrame(data=self.final_download_tiles, columns=['x', 'y', 'z', 'geometry'])
                            self.to_sqlite(download_tiles_dataframe)
                            self.final_download_tiles = []
        download_tiles_dataframe = pd.DataFrame(data=self.final_download_tiles, columns=['x', 'y', 'z', 'geometry'])
        self.to_sqlite(download_tiles_dataframe)
        self.progress_info['is_task_complete'] = True
        self.write_to_queue()

    def tile_calculate_task(self):
        zoom = 1
        start_tilex, end_tilex, start_tiley, end_tiley, z = self.get_rec_info(self.rec, zoom)
        self.task_list = [(i, j, z, None) for i in range(start_tilex, end_tilex + 1) for j in range(start_tiley, end_tiley + 1)]
        return self.task_list

    def tile_spatial_calculate(self, tile: tuple):
        tile_geometry = self.get_tile_geometry(tile[0], tile[1], tile[2])
        download_tiles = []
        new_task = []
        is_intersects = self.polygon.intersects(tile_geometry)
        is_contains = False
        if is_intersects:
            is_contains = self.polygon.contains(tile_geometry)
        if is_contains:
            # 如果空间关系为包含，则该层级本级及下级所有瓦片均在下载范围内
            contained_tiles = self.get_all_child_tiles(tile[0], tile[1], tile[2])
            # 将该瓦片所有下级瓦片加入下载列表
            download_tiles.extend(contained_tiles)
            # 如果用户需求该层级，则将该瓦片加入下载列表
            if tile[2] in self.downloadzoom:
                download_tiles.extend([(tile[0], tile[1], tile[2], str(tile_geometry))])
        elif is_intersects and not is_contains:
            # 如果空间关系为相交但不包含，则该层级本级瓦片在下载范围内，但下级瓦片仍需计算
            uncontained_intersects_child_tiles = self.get_child_tiles(tile[0], tile[1], tile[2])
            # 如果用户需求该层级，则将该瓦片加入下载列表
            if tile[2] in self.downloadzoom:
                download_tiles.extend([(tile[0], tile[1], tile[2], str(tile_geometry))])
            # 将该瓦片下级瓦片加入计算列表
            new_task.extend(uncontained_intersects_child_tiles)
        return download_tiles, new_task, tile

    def to_sqlite(self, dataframe: pd.DataFrame):
        conn = self.database_session
        distinct_zoom_dataframe = dataframe.drop_duplicates(subset=['z'])
        for index, row in distinct_zoom_dataframe.iterrows():
            if self.bands is not None:
                for bands in self.bands:
                    zoom = row['z']
                    dataframe.loc[:, 'image'] = None
                    dataframe.loc[:, 'dtype'] = None
                    dataframe.loc[:, 'shape'] = None
                    dataframe.loc[:, 'bands'] = bands
                    dataframe.loc[:, 'raster'] = None
                    dataframe.loc[:, 'status'] = None
                    dataframe.loc[:, 'stitch_status'] = None
                    dataframe.loc[:, 'width'] = self.tile_width
                    dataframe.loc[:, 'height'] = self.tile_height
                    dataframe.loc[:, 'error'] = None
                    dataframe.loc[:, 'cost'] = None
                    dataframe['geometry'] = dataframe.apply(
                        lambda row: dumps(self.get_tile_geometry(row['x'], row['y'], row['z'], True, distance=self.buffer_distance[str(row['z'])])), axis=1)
                    if zoom <= 10:
                        data_to_sqlite = dataframe.loc[dataframe['z'] == zoom,]
                        data_to_sqlite = data_to_sqlite.copy()
                        data_to_sqlite.to_sql(name='tiles_10', con=conn, index=False, if_exists='append', dtype={
                            'x': 'INTEGER',
                            'y': 'INTEGER',
                            'z': 'INTEGER',
                            'geometry': 'TEXT',
                            'image': 'BLOB',
                            'dtype': 'TEXT',
                            'shape': 'TEXT',
                            'bands': 'TEXT',
                            'raster': 'TEXT',
                            'status': 'INTEGER',
                            'stitch_status': 'INTEGER',
                            'width': 'INTEGER',
                            'height': 'INTEGER',
                            'error': 'TEXT',
                            'cost': 'REAL',
                        })
                    else:
                        data_to_sqlite = dataframe.loc[dataframe['z'] == zoom,]
                        data_to_sqlite = data_to_sqlite.copy()
                        data_to_sqlite.to_sql(name='tiles_' + str(zoom), con=conn, index=False, if_exists='append', dtype={
                            'x': 'INTEGER',
                            'y': 'INTEGER',
                            'z': 'INTEGER',
                            'geometry': 'TEXT',
                            'image': 'BLOB',
                            'dtype': 'TEXT',
                            'shape': 'TEXT',
                            'bands': 'TEXT',
                            'raster': 'TEXT',
                            'status': 'INTEGER',
                            'stitch_status': 'INTEGER',
                            'width': 'INTEGER',
                            'height': 'INTEGER',
                            'error': 'TEXT',
                            'cost': 'REAL',
                        })
            else:
                zoom = row['z']
                dataframe.loc[:, 'image'] = None
                dataframe.loc[:, 'dtype'] = None
                dataframe.loc[:, 'shape'] = None
                dataframe.loc[:, 'bands'] = None
                dataframe.loc[:, 'raster'] = None
                dataframe.loc[:, 'status'] = None
                dataframe.loc[:, 'stitch_status'] = None
                dataframe.loc[:, 'width'] = self.tile_width
                dataframe.loc[:, 'height'] = self.tile_height
                dataframe.loc[:, 'error'] = None
                dataframe.loc[:, 'cost'] = None
                dataframe['geometry'] = dataframe.apply(
                    lambda row: dumps(self.get_tile_geometry(row['x'], row['y'], row['z'], True, distance=self.buffer_distance[str(row['z'])])), axis=1)
                if zoom <= 10:
                    data_to_sqlite = dataframe.loc[dataframe['z'] == zoom,]
                    data_to_sqlite = data_to_sqlite.copy()
                    data_to_sqlite.to_sql(name='tiles_10', con=conn, index=False, if_exists='append', dtype={
                        'x': 'INTEGER',
                        'y': 'INTEGER',
                        'z': 'INTEGER',
                        'geometry': 'TEXT',
                        'image': 'BLOB',
                        'dtype': 'TEXT',
                        'shape': 'TEXT',
                        'bands': 'TEXT',
                        'raster': 'TEXT',
                        'status': 'INTEGER',
                        'stitch_status': 'INTEGER',
                        'width': 'INTEGER',
                        'height': 'INTEGER',
                        'error': 'TEXT',
                        'cost': 'REAL',
                    })
                else:
                    data_to_sqlite = dataframe.loc[dataframe['z'] == zoom,]
                    data_to_sqlite = data_to_sqlite.copy()
                    data_to_sqlite.to_sql(name='tiles_' + str(zoom), con=conn, index=False, if_exists='append', dtype={
                        'x': 'INTEGER',
                        'y': 'INTEGER',
                        'z': 'INTEGER',
                        'geometry': 'TEXT',
                        'image': 'BLOB',
                        'dtype': 'TEXT',
                        'shape': 'TEXT',
                        'bands': 'TEXT',
                        'raster': 'TEXT',
                        'status': 'INTEGER',
                        'stitch_status': 'INTEGER',
                        'width': 'INTEGER',
                        'height': 'INTEGER',
                        'error': 'TEXT',
                        'cost': 'REAL',
                    })


class GeeImageDownload:
    def __init__(self, taskname: str, savepath: str, objective: str, start_date: str, end_date: str, proxies: dict, ee_initialize, scale,
                 progress_info: dict, process_done: dict, signal, queue):
        self.is_raster = False
        self.exception = None
        self.customlandcover = map_engine.CustomLandcover()
        self.ee_initialize = ee_initialize
        if scale is not None:
            self.scale = int(scale)
        else:
            self.scale = scale
        self.download_results = q.Queue()
        self.all_proxies = proxies
        self.network_session = None
        self.database_session = None
        self.download_complete = False
        self.start_date = start_date
        self.end_date = end_date
        self.taskname = taskname
        self.splite_size = 5000
        self.queue = queue
        self.process_done = process_done
        self.image_results = []
        self.savepath = savepath
        self.objective = objective
        self.pool_count = multiprocessing.cpu_count()
        self.progress_info = progress_info
        self.condition = Condition()
        self.bands = []
        self.signal = signal

    def write_to_queue(self):
        data = (self.progress_info, self.process_done, self.taskname)
        self.queue.put(data)

    def get_ee_object(self):
        if self.bands is None:
            if self.objective == 'Dynamic World':
                self.ee_object = {'default': self.customlandcover.dynamic_world(start_date=self.start_date, end_date=self.end_date)}
            elif self.objective == 'JRC Monthly Water History':
                self.ee_object = {'default': self.customlandcover.jrc_monthly_water_history(start_date=self.start_date, end_date=self.end_date)}
        else:
            if self.objective == 'CFSV2':
                self.ee_object = {}
                for band in self.bands:
                    self.ee_object[band] = self.customlandcover.CFSV2(start_date=self.start_date, end_date=self.end_date, band=band)
            elif self.objective == 'Global Multi-resolution Terrain':
                self.ee_object = {'default': ee.Image('USGS/GMTED2010_FULL')}
        return self.ee_object

    def download_info(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        cur.execute('create table if not exists download_info(table_name text primary key, total int, success int, fail int, start_time float,end_time float)')
        try:
            cur.execute(
                "insert into download_info(table_name,total,success,fail) select name,0,0,0 FROM sqlite_master WHERE type='table' and name like 'tiles_%' and name not like '%rs%'")
            conn.commit()
        except Exception as e:
            pass
        table = cur.execute("select name FROM sqlite_master WHERE type='table' and name like 'tiles_%'")
        for table_name in table.fetchall():
            cur.execute(f"update download_info set total = (select count(1) from {table_name[0]}) where table_name='{table_name[0]}'")
        conn.commit()
        conn.close()

    def get_download_progress(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        download_progress_all_res = cur.execute('select sum(total),sum(success),sum(fail),min(start_time),max(end_time) from download_info')
        total, success, fail, all_st_time, all_ed_time = download_progress_all_res.fetchone()
        if (success + fail) / total != 1:
            all_ed_time = None
        self.progress_info['download_total'] = total
        self.progress_info['download_success'] = success
        self.progress_info['download_fail'] = 0
        self.progress_info['st_time'] = all_st_time
        self.progress_info['ed_time'] = all_ed_time
        self.write_to_queue()
        conn.close()

    def reshape_table(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        table_sql = "SELECT name FROM sqlite_master WHERE type='table' and name like 'tiles_%' and name not like 'tiles_%_part_%' and name<>'tiles_10' and name not like '%rs%'"
        try:
            cur.execute(f'create table if not exists "tiles_10_rs" as select * from "tiles_10" where 1=2')
        except Exception as e:
            pass
        table_name_result = cur.execute(table_sql)
        table_names = table_name_result.fetchall()
        for rows in table_names:
            table_name = rows[0]
            counts = cur.execute(f"select count(*) from {table_name}")
            num = counts.fetchone()[0] // self.splite_size
            if num > 0:
                x = cur.execute(f"select x from {table_name} order by x")
                x_all = x.fetchall()
                if x_all[-1][0] - x_all[0][0] > num * 2:
                    for i in range(num + 1):
                        x_splite_start = x_all[i * self.splite_size][0]
                        try:
                            x_splite_end = x_all[(i + 1) * self.splite_size][0]
                        except:
                            x_splite_end = x_all[-1][0] + 1
                        cur.execute(f'drop table if exists "{table_name}_part_{i + 1}"')
                        create_sql = f'create table "{table_name}_part_{i + 1}" as select * from "{table_name}" where x < {x_splite_end} and x >= {x_splite_start}'
                        create_sql2 = f'create table if not exists "{table_name}_part_{i + 1}_rs" as select * from "{table_name}" where 1=2'
                        cur.execute(create_sql)
                        cur.execute(create_sql2)
                else:
                    y = cur.execute(f"select y from {table_name} order by y")
                    y_all = y.fetchall()
                    for i in range(num + 1):
                        y_splite_start = y_all[i * self.splite_size][0]
                        try:
                            y_splite_end = y_all[(i + 1) * self.splite_size][0]
                        except:
                            y_splite_end = y_all[-1][0] + 1
                        cur.execute(f'drop table if exists "{table_name}_part_{i + 1}"')
                        create_sql = f'create table "{table_name}_part_{i + 1}" as select * from "{table_name}" where y < {y_splite_end} and y >= {y_splite_start}'
                        create_sql2 = f'create table if not exists "{table_name}_part_{i + 1}_rs" as select * from "{table_name}" where 1=2'
                        cur.execute(create_sql)
                        cur.execute(create_sql2)
                cur.execute(f'drop table "{table_name}"')
            else:
                cur.execute(f'create table if not exists "{table_name}_rs" as select * from "{table_name}" where 1=2')
        conn.close()

    def get_cropping_size(self, table_name):
        conn = self.database_session.connection()
        cur = conn.cursor()
        result = cur.execute(f'select width,height from {table_name} limit 1')
        rs = result.fetchall()
        self.cropping_size_width = rs[0][0] + 2
        self.cropping_size_height = rs[0][1] + 2
        conn.close()

    def get_bands(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        bands_rs = cur.execute('select bands from task_info')
        bands_str = bands_rs.fetchone()[0]
        if bands_str is not None:
            self.bands = tuple(map(str, bands_str.split(',')))
        else:
            self.bands = None
        conn.close()

    def get_download_parameter(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        try:
            sql = "SELECT name FROM sqlite_master WHERE type='table' and name like 'tiles_%' and name not like '%rs%'"
            table_name_result = cur.execute(sql)
            table_names = table_name_result.fetchall()
            for rows in table_names:
                table_name = rows[0]
                self.get_cropping_size(table_name)
                cur.execute(
                    f"update download_info set start_time= case when start_time is null then {time.time()} else start_time end where table_name = '{table_name}'")
                conn.commit()
                sql = (
                    f'select x,y,z,bands,geometry,width,height from "{table_name}" where (x,y,z,bands) in (select x,y,z,bands from "{table_name}_rs" where status = -1) '
                    f'or (x,y,z,bands) not in (select x,y,z,bands from "{table_name}_rs")')
                download_paramete_result = cur.execute(sql)
                while True:
                    row = download_paramete_result.fetchone()
                    if not row:
                        cur.execute(
                            f"update download_info set start_time= case when end_time is null then {time.time()} else end_time end where table_name = '{table_name}'")
                        conn.commit()
                        break
                    x, y, z, bands, geometry, width, height = row
                    yield x, y, z, bands, width, height, geometry, table_name
        except Exception as e:
            return None
        conn.close()

    def get_proxies(self):
        random_value = random.choice(list(self.all_proxies.values()))
        self.proxies = random_value

    def create_network_session(self):
        self.network_session = requests.Session()
        retries = Retry(total=10, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(pool_maxsize=50, max_retries=retries)
        self.network_session.mount('http://', adapter)
        self.network_session.mount('https://', adapter)
        # 连接到VPN
        self.get_proxies()
        self.network_session.proxies = self.proxies
        return self.network_session

    def database_conn_pool(self):
        engine = PooledDB(
            creator=sqlite3,  # 使用sqlite3作为数据库连接
            maxconnections=30,  # 最多30个连接
            database=self.savepath,
            check_same_thread=False
        )
        self.database_session = engine
        return self.database_session

    def listening(self):  # 监听来自主进程的终止信号，如果有，安全关闭进程
        while True:
            if self.signal.is_set() or self.download_complete or self.exception is not None:
                if self.signal.is_set() or self.exception is not None:
                    for f in self.futures:
                        f.cancel()
                break
            time.sleep(1)

    def multiworker(self):
        self.download_count = 0
        self.create_network_session()
        os.environ['https_proxy'] = self.proxies['http']
        credentials = ee.ServiceAccountCredentials(self.ee_initialize[0], self.ee_initialize[1])
        ee.Initialize(credentials=credentials, project=self.ee_initialize[2])
        self.database_conn_pool()
        self.reshape_table()
        self.download_info()
        self.get_bands()
        self.get_ee_object()
        self.get_download_progress()
        listening_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        listening_thread.submit(self.listening)
        to_sqlite_results = []
        to_sqlite_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        to_sqlite_result = to_sqlite_thread.submit(self.to_sqlite)
        to_sqlite_results.append(to_sqlite_result)
        self.progress_info['this_st_time'] = time.time()
        self.write_to_queue()
        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            # 同时提交多个任务给线程池执行下载任务
            self.futures = [executor.submit(self.download, download_parameter) for download_parameter in self.get_download_parameter()]
        if self.exception is not None:
            raise self.exception
        self.download_complete = True
        concurrent.futures.wait(to_sqlite_results)
        self.progress_info['is_task_complete'] = True
        self.write_to_queue()

    def download(self, parameter: tuple):
        with self.condition:
            while self.download_results.qsize() > 10000:
                self.condition.wait()
        st = time.time()
        x, y, z, bands, no_buffer_width, no_buffer_height, geometry, table_name = parameter
        region = GeeImageCalculate.wkt_to_eegeometry(geometry)
        try:
            if bands is not None:
                if self.objective == 'Global Multi-resolution Terrain':
                    image = geemap.ee_to_numpy(ee_object=self.ee_object['default'], bands=[bands,], scale=self.scale, region=region)  # 从接口获取numpy数组形式图像
                elif self.objective == 'CFSV2':
                    image = geemap.ee_to_numpy(ee_object=self.ee_object[bands], scale=self.scale, region=region)
            else:
                if self.objective == 'Dynamic World':
                    image = geemap.ee_to_numpy(ee_object=self.ee_object['default'], scale=self.scale, region=region)
                if self.objective == 'JRC Monthly Water History':
                    image = geemap.ee_to_numpy(ee_object=self.ee_object['default'], scale=self.scale, region=region)
            # 获取图像的高度和宽度
            height, width = image.shape[:2]
            if self.cropping_size_height < height and self.cropping_size_width < width:
                # 计算几何中心
                center_x, center_y = width // 2, height // 2
                # 计算裁剪区域的左上角和右下角
                crop_x1 = center_x - self.cropping_size_width // 2  # 左边界
                crop_y1 = center_y - self.cropping_size_height // 2  # 上边界
                crop_x2 = crop_x1 + self.cropping_size_width  # 右边界
                crop_y2 = crop_y1 + self.cropping_size_height  # 下边界
                # 裁剪图像
                image = image[crop_y1:crop_y2, crop_x1:crop_x2]
            # success, image = cv2.imencode('.tif', image)  # 将裁剪后的图像重新编码为 .tif 格式
            if str(image.dtype) in ('float16', 'float32', 'float64') and self.bands is None:
                image = self.normalize8(image)
            if image.shape[-1] == 3:
                image[:, :, [0, 1, 2]] = image[:, :, [2, 1, 0]]
            finial_image = image.tobytes()  # 返回字节流数据
            dtype = str(image.dtype)
            shape = str(image.shape)
            bands = bands
            status = 1
            error = None
        except Exception as e:
            finial_image = None
            dtype = None
            shape = None
            bands = None
            status = -1
            error = str(e)
        cost = time.time() - st
        self.download_results.put((table_name, x, y, z, finial_image, dtype, shape, bands, status, no_buffer_width, no_buffer_height, error, cost))
        self.download_count = self.download_count + 1

    @staticmethod
    def normalize8(image):
        mn = image.min()
        mx = image.max()
        mx -= mn
        image = ((image - mn) / mx) * 255
        return image.astype(numpy.uint8)

    def to_sqlite(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        while True:
            try:
                # 如果队列中已无值，且下载已完成，结束数据库写入
                if self.download_results.empty() and self.download_complete:
                    all_tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' and name like 'tiles_%' and name like '%rs%'")
                    for row in all_tables.fetchall():
                        all_table = row[0]
                        update_info_sql = f'update task_info set dtype = (select distinct dtype from {all_table} limit 1)'
                        cur.execute(update_info_sql)
                        index_sql1 = f'CREATE INDEX {all_table}_index ON "{all_table}" (x, y, z)'
                        index_sql2 = f'CREATE INDEX {all_table}_index_status ON "{all_table}" status'
                        index_sql3 = f'CREATE INDEX {all_table}_index_stitch_status ON "{all_table}" stitch_status'
                        try:
                            cur.execute(index_sql1)
                        except:
                            pass
                        try:
                            cur.execute(index_sql2)
                        except:
                            pass
                        try:
                            cur.execute(index_sql3)
                        except:
                            pass
                        # 去重
                        sql = (f'DELETE FROM "{all_table}" WHERE (status=-1 AND (x,y,z,bands) IN (SELECT  x, y, z, bands FROM "{all_table}" GROUP BY x, y, z '
                               f'HAVING count(*)> 1)) OR (status = 1 AND (rowid NOT IN (SELECT max(rowid) FROM "{all_table}" GROUP BY x,y,z,bands '
                               f'HAVING count(*)>1) AND (x,y,z,bands,status) IN (SELECT x,y,z,bands,status FROM "{all_table}" GROUP BY x,y,z,bands,status '
                               f'HAVING count(*)>1)))')
                        cur.execute(sql)
                    conn.commit()
                    break
                download_result = []
                table_name_last = None
                download_success = 0
                download_fail = 0
                i = 0
                while i < 1000:
                    i += 1
                    try:
                        download = self.download_results.get(block=False, timeout=1)
                    except Exception as e:
                        break
                    table_name = download[0]
                    if table_name == table_name_last or table_name_last is None:
                        download_result.append({'x': download[1], 'y': download[2], 'z': download[3], 'image': download[4], 'dtype': download[5],
                                                'shape': download[6], 'bands': download[7], 'status': download[8], 'width': download[9], 'height': download[10],
                                                'error': download[11], 'cost': download[12]})
                        if download[8] == 1:
                            download_success = download_success + 1
                        else:
                            download_fail = download_fail + 1
                        table_name_last = table_name
                    elif table_name_last != table_name and table_name_last is not None:
                        self.download_results.put(download)
                        break
                if len(download_result) != 0:
                    # 构造SQL批量插入语句
                    insert_sql = (f'INSERT INTO "{table_name_last}_rs" (x,y,z,image,dtype,shape,bands,status,width,height,error,cost) '
                                  f'VALUES(:x,:y,:z,:image,:dtype,:shape,:bands,:status,:width,:height,:error,:cost)')
                    cur.executemany(insert_sql, download_result)
                    cur.execute(f'update download_info set success = success + {download_success} where table_name = "{table_name_last}"')
                    # conn.execute(text(f'update download_info set fail = fail + {download_fail} where table_name = "{table_name_last}"'))
                    conn.commit()
                    self.progress_info['download_success'] = self.progress_info['download_success'] + download_success
                    self.progress_info['download_fail'] = self.progress_info['download_fail'] + download_fail
                    self.write_to_queue()
            except Exception as e:
                print(e)
                self.exception = e
        conn.close()


# if __name__ == '__main__':
#     start_time = time.time()
#     a = GeeImageCalculate(taskname='qwe', proxies={'qwe': {'http': 'http://127.0.0.1:10809', 'https': 'https://127.0.0.1:10809'}}, ee_object='GOOGLE/DYNAMICWORLD/V1',
#                            start_date='2023-11-01', end_date='2024-11-01', savepath='d:/test/qwe',
#                            polygon='MULTIPOLYGON(((109.754502 34.212239,109.739996 34.217219,109.731074 34.236337,109.691069 34.243003,109.668562 34.238586,109.659122 34.245894,109.627751 34.240031,109.575831 34.262516,109.523622 34.255851,109.500252 34.268698,109.477688 34.294465,109.456045 34.298237,109.450577 34.326724,109.458981 34.350869,109.461859 34.394169,109.445799 34.397376,109.442633 34.414128,109.423753 34.423505,109.424501 34.441934,109.413277 34.46068,109.391346 34.468529,109.399692 34.49976,109.391346 34.529138,109.418054 34.536741,109.429106 34.549385,109.449771 34.553625,109.437222 34.595621,109.42335 34.679548,109.430775 34.688016,109.421163 34.706866,109.429854 34.713814,109.410859 34.743117,109.365098 34.739285,109.358651 34.716929,109.340749 34.69872,109.286641 34.687697,109.280424 34.697442,109.254176 34.6988,109.243873 34.687297,109.203407 34.697682,109.173072 34.691931,109.168237 34.666525,109.156034 34.644709,109.139053 34.639514,109.138823 34.588823,109.108891 34.577705,109.092889 34.588983,109.06123 34.593542,109.011842 34.586184,108.998315 34.576426,108.955201 34.565866,108.974081 34.541383,108.971146 34.502962,108.976441 34.44594,108.955834 34.444979,108.961015 34.424787,108.929817 34.40443,108.913469 34.406033,108.89292 34.390802,108.859879 34.388638,108.828048 34.365947,108.803239 34.359531,108.794316 34.334185,108.796792 34.296713,108.750282 34.296151,108.744756 34.276886,108.700952 34.273675,108.692087 34.238987,108.669753 34.243966,108.650125 34.265808,108.619444 34.25826,108.593369 34.239308,108.56407 34.22959,108.529706 34.23216,108.468115 34.205731,108.457351 34.209186,108.425922 34.200348,108.418266 34.20549,108.37452 34.207659,108.351495 34.196812,108.328585 34.195928,108.293185 34.209989,108.264865 34.199464,108.237638 34.199625,108.20195 34.221718,108.156534 34.225092,108.143928 34.218585,108.086769 34.231036,108.027941 34.228144,108.030013 34.208623,108.011421 34.161047,108.003074 34.155742,108.00106 34.103953,107.976481 34.075069,107.972567 34.062113,107.943038 34.042312,107.941944 34.006079,107.949887 33.985379,107.915638 33.986104,107.908213 33.999314,107.87759 33.993918,107.846564 33.975148,107.8103 33.995045,107.784455 33.980626,107.76736 33.954521,107.71377 33.92736,107.717626 33.910431,107.704387 33.857282,107.708589 33.846794,107.697422 33.817903,107.679463 33.808298,107.68591 33.796996,107.665821 33.760013,107.687924 33.744746,107.695868 33.72366,107.719353 33.725761,107.737427 33.717923,107.763503 33.719781,107.830102 33.69788,107.854278 33.694081,107.90309 33.748947,107.931583 33.731255,107.951672 33.745797,107.96612 33.734648,108.008312 33.728993,108.054247 33.709842,108.073818 33.723417,108.098396 33.723498,108.113823 33.712589,108.132243 33.721882,108.149281 33.712347,108.180191 33.711862,108.191761 33.725276,108.233148 33.726649,108.259281 33.754763,108.288292 33.767282,108.307 33.76292,108.345969 33.793848,108.369454 33.793202,108.404855 33.763324,108.443709 33.781979,108.466445 33.800629,108.484405 33.804262,108.544557 33.788438,108.560444 33.765101,108.584274 33.764536,108.585425 33.780122,108.602579 33.791426,108.617717 33.825974,108.650067 33.841549,108.697843 33.828315,108.741014 33.841468,108.755923 33.839048,108.756038 33.812657,108.778487 33.798046,108.805771 33.817177,108.84002 33.815966,108.854238 33.826539,108.874788 33.824522,108.913239 33.848649,108.922161 33.869383,108.944265 33.86664,109.007985 33.883256,109.051502 33.923169,109.070555 33.906641,109.107682 33.907609,109.120576 33.876481,109.164207 33.871319,109.18493 33.874303,109.207379 33.867043,109.251874 33.880675,109.289462 33.87027,109.299823 33.856556,109.316918 33.85462,109.355945 33.867608,109.399059 33.845422,109.442518 33.843566,109.470263 33.851796,109.508541 33.848004,109.537322 33.862042,109.540142 33.875513,109.576118 33.897691,109.592984 33.896804,109.603633 33.940982,109.562994 33.960726,109.533235 33.982077,109.545496 34.025244,109.56121 34.034181,109.552806 34.044727,109.567139 34.064688,109.584235 34.072092,109.634313 34.075633,109.658835 34.105481,109.669886 34.099931,109.694753 34.109101,109.739824 34.09792,109.742184 34.12189,109.755078 34.127197,109.78098 34.160083,109.789787 34.145049,109.822194 34.155742,109.801415 34.160887,109.778102 34.183392,109.745465 34.187491,109.754502 34.212239)),((109.174166 33.808702,109.162366 33.812657,109.147284 33.799337,109.154019 33.791103,109.174166 33.808702)))', )
#     a.worker()
#     end_time = time.time()
#     # mulstart_time = time.time()
#     # b = CalculateTiles((1, 2, 3, ), 'MULTIPOLYGON(((101.167885 27.198311,101.170349 27.175421,101.145095 27.103523,101.157414 27.094999,101.136472 27.023584,101.228863 26.981992,101.227015 26.959057,101.264587 26.955323,101.267667 26.903034,101.311399 26.903034,101.365602 26.883819,101.399478 26.841642,101.358826 26.771669,101.387159 26.753501,101.389623 26.723036,101.435819 26.740675,101.458608 26.731054,101.445674 26.77434,101.466 26.786629,101.513427 26.768463,101.453065 26.692563,101.481398 26.673313,101.461072 26.640687,101.461688 26.606447,101.402558 26.604841,101.395783 26.591998,101.422884 26.53151,101.458608 26.49563,101.506652 26.499915,101.530057 26.467239,101.565782 26.454381,101.637847 26.388995,101.635383 26.357361,101.660636 26.346635,101.64031 26.318745,101.597195 26.303187,101.586108 26.279579,101.630455 26.224832,101.690202 26.241473,101.737013 26.219463,101.773353 26.168448,101.807846 26.156093,101.796759 26.114723,101.839875 26.082477,101.835563 26.04592,101.857737 26.049146,101.899621 26.099139,101.929186 26.105588,101.954439 26.084627,102.020961 26.096451,102.080091 26.065275,102.107808 26.068501,102.152156 26.10935,102.174946 26.146961,102.242699 26.190468,102.245163 26.212483,102.349257 26.244694,102.392372 26.296749,102.440416 26.300505,102.542046 26.338591,102.570995 26.362723,102.629509 26.336982,102.638748 26.307479,102.60056 26.250598,102.659074 26.221611,102.709581 26.210336,102.739762 26.268846,102.785342 26.298895,102.833385 26.306406,102.878964 26.364332,102.893131 26.338591,102.975667 26.340736,102.998457 26.371839,102.988602 26.413117,102.989833 26.482775,103.030485 26.485989,103.052659 26.514374,103.052659 26.555602,103.035413 26.556673,103.026174 26.664221,103.005232 26.679195,103.008312 26.710741,102.983674 26.76686,102.991681 26.775409,102.966428 26.837904,102.949181 26.843244,102.896211 26.91264,102.894979 27.001724,102.870957 27.026782,102.913457 27.133886,102.904218 27.227584,102.883276 27.258444,102.883892 27.299401,102.899906 27.317481,102.941174 27.405711,102.989833 27.367983,103.055739 27.40943,103.080992 27.396679,103.141355 27.420586,103.144434 27.450331,103.19063 27.523596,103.232514 27.56976,103.2861 27.561802,103.29226 27.632872,103.349542 27.678459,103.369868 27.708664,103.393274 27.709194,103.461027 27.779638,103.487512 27.794992,103.509686 27.843687,103.502295 27.910343,103.55465 27.978543,103.515846 27.965329,103.486281 28.033495,103.459179 28.021345,103.430846 28.044587,103.470266 28.122204,103.533092 28.168641,103.573128 28.230877,103.643961 28.260401,103.692004 28.232459,103.701859 28.198709,103.740048 28.23615,103.770845 28.233514,103.828743 28.285173,103.877402 28.316262,103.85338 28.356822,103.860156 28.383677,103.828743 28.44,103.829975 28.459995,103.781931 28.525216,103.802873 28.563068,103.838598 28.587244,103.833054 28.605109,103.850917 28.66709,103.887873 28.61982,103.910047 28.631377,103.953779 28.600906,104.05972 28.6277,104.09606 28.603533,104.117618 28.634003,104.170589 28.642932,104.230951 28.635579,104.252509 28.660788,104.277147 28.631902,104.314719 28.615617,104.372617 28.649235,104.425588 28.626649,104.417581 28.598279,104.375697 28.5946,104.355987 28.555183,104.323342 28.540989,104.260516 28.536257,104.267908 28.499448,104.254357 28.403683,104.282074 28.343128,104.314103 28.306778,104.343052 28.334173,104.384936 28.329959,104.392943 28.291497,104.420045 28.269889,104.44961 28.269889,104.462544 28.241422,104.442834 28.211366,104.402182 28.202928,104.406494 28.173389,104.444682 28.16231,104.448994 28.113758,104.40095 28.091586,104.373233 28.051454,104.304248 28.050926,104.30856 28.036136,104.362762 28.012891,104.40095 27.952114,104.44961 27.927794,104.508124 27.878078,104.52537 27.889187,104.573413 27.840512,104.607906 27.857974,104.63316 27.850567,104.676275 27.880723,104.743413 27.901881,104.761891 27.884426,104.796999 27.901352,104.842579 27.900294,104.888158 27.914574,104.918339 27.938897,104.903557 27.962158,104.975006 28.020816,104.980549 28.063073,105.002107 28.064129,105.061853 28.096866,105.119752 28.07205,105.168411 28.071522,105.186889 28.054623,105.167795 28.021345,105.186273 27.995454,105.218302 27.990698,105.247867 28.009193,105.270657 27.99704,105.284823 27.935725,105.233084 27.895534,105.25957 27.827811,105.313157 27.810874,105.273736 27.794992,105.293447 27.770637,105.290367 27.712373,105.308229 27.704955,105.353809 27.748924,105.44004 27.775402,105.508409 27.769048,105.560148 27.71979,105.605112 27.715552,105.62359 27.666269,105.664242 27.683759,105.720292 27.683759,105.722756 27.706015,105.76772 27.7182,105.848408 27.707074,105.868118 27.732504,105.922937 27.746805,105.92848 27.729855,105.985146 27.749983,106.023335 27.746805,106.063987 27.776991,106.120653 27.779638,106.193334 27.75422,106.242609 27.767459,106.306667 27.808756,106.337464 27.859033,106.325145 27.898708,106.304819 27.899237,106.307899 27.936782,106.328225 27.952643,106.286341 28.007079,106.246305 28.011835,106.266631 28.066769,106.206885 28.134343,106.145291 28.162837,106.093552 28.162837,105.975907 28.107952,105.943878 28.143314,105.895219 28.119565,105.860727 28.159672,105.889676 28.237732,105.848408 28.255656,105.824386 28.306251,105.78743 28.335753,105.76464 28.308359,105.76464 28.308359,105.737539 28.30309,105.730147 28.271997,105.68888 28.284119,105.639604 28.324164,105.655003 28.362615,105.643916 28.431053,105.612503 28.438947,105.62359 28.517854,105.68272 28.534154,105.693191 28.58882,105.712901 28.586718,105.74493 28.616668,105.757249 28.590397,105.78435 28.610889,105.808372 28.599855,105.884748 28.595126,105.889676 28.670765,105.937719 28.686517,105.966668 28.761041,106.001161 28.743727,106.030726 28.694917,106.085544 28.681792,106.103407 28.636104,106.14837 28.642932,106.17116 28.629275,106.184711 28.58882,106.254928 28.539412,106.2925 28.537309,106.304819 28.505233,106.349167 28.473674,106.379348 28.479986,106.37442 28.525742,106.33192 28.55308,106.346703 28.583565,106.304203 28.64976,106.305435 28.704365,106.274022 28.739004,106.267863 28.779402,106.245689 28.817686,106.264783 28.845997,106.206885 28.904691,106.173008 28.920407,106.14837 28.901548,106.101559 28.898928,106.070762 28.919884,106.049204 28.906263,106.040581 28.955498,106.001161 28.973824,105.969132 28.965971,105.910002 28.920407,105.852719 28.927217,105.830546 28.944501,105.797285 28.936121,105.801596 28.958116,105.762176 28.9911,105.766488 29.013607,105.74185 29.039249,105.757865 29.069068,105.728916 29.1062,105.752321 29.129727,105.728916 29.134432,105.703662 29.176766,105.712285 29.219082,105.695039 29.287482,105.647612 29.253027,105.631597 29.280174,105.557684 29.278608,105.521344 29.264513,105.513337 29.283306,105.459134 29.288526,105.465294 29.322969,105.42033 29.31149,105.418482 29.352185,105.441888 29.400686,105.426489 29.419454,105.372903 29.421018,105.399388 29.43874,105.387069 29.455416,105.387069 29.455416,105.334099 29.441345,105.337794 29.459064,105.305149 29.53199,105.296526 29.571035,105.332867 29.592374,105.347649 29.621512,105.38091 29.628275,105.419714 29.688082,105.476996 29.674564,105.481924 29.718232,105.529351 29.707836,105.574931 29.744216,105.582938 29.819013,105.610655 29.837184,105.707974 29.840818,105.738771 29.891159,105.717213 29.893753,105.70243 29.924879,105.730763 29.95755,105.723372 29.975177,105.753553 30.018196,105.719677 30.042548,105.687032 30.038922,105.676561 30.06793,105.638988 30.076216,105.642068 30.101072,105.582938 30.12385,105.574315 30.130579,105.596489 30.159043,105.536127 30.152834,105.550909 30.179222,105.556453 30.187499,105.558916 30.18543,105.56138 30.183878,105.642684 30.186464,105.662394 30.210258,105.619894 30.234045,105.624822 30.275918,105.670401 30.254208,105.720292 30.252657,105.720292 30.252657,105.714749 30.322939,105.754785 30.342567,105.760329 30.384393,105.792357 30.427234,105.825618 30.436006,105.84656 30.410203,105.900763 30.405042,105.943263 30.372002,106.031958 30.373551,106.07261 30.333786,106.132972 30.30279,106.132356 30.323972,106.168696 30.303823,106.180399 30.233011,106.232754 30.185947,106.260471 30.19681,106.260471 30.204051,106.260471 30.207672,106.264167 30.20974,106.296196 30.205603,106.306667 30.238182,106.334384 30.225772,106.349167 30.24542,106.401521 30.242318,106.428623 30.254725,106.43971 30.308473,106.49884 30.295556,106.545035 30.296589,106.560434 30.31519,106.611557 30.292455,106.642354 30.246454,106.612789 30.235596,106.612789 30.235596,106.612173 30.235596,106.612173 30.235596,106.611557 30.235596,106.612173 30.235596,106.611557 30.235596,106.631883 30.186464,106.677462 30.156974,106.672535 30.122297,106.700252 30.111944,106.699636 30.074145,106.724274 30.058607,106.732281 30.027005,106.785252 30.01716,106.825904 30.03115,106.825904 30.03115,106.83699 30.049801,106.862244 30.033223,106.913367 30.025451,106.94478 30.037367,106.976193 30.083467,106.975577 30.088127,106.976809 30.088127,106.977425 30.087609,106.978656 30.087609,106.979888 30.088127,106.980504 30.087609,106.981736 30.08502,107.02054 30.036849,107.053801 30.043584,107.058113 30.043066,107.084598 30.063786,107.080286 30.094341,107.103076 30.090198,107.221337 30.213878,107.257677 30.267131,107.288474 30.337402,107.338981 30.386459,107.368546 30.468508,107.408582 30.521623,107.443075 30.53348,107.427676 30.547397,107.485575 30.598408,107.516987 30.644759,107.477567 30.664837,107.458473 30.704981,107.424597 30.74048,107.454162 30.771851,107.454162 30.771851,107.498509 30.809381,107.483111 30.838675,107.515756 30.854603,107.57735 30.847924,107.645103 30.821202,107.693146 30.875665,107.739957 30.884396,107.760899 30.862823,107.763979 30.817091,107.788001 30.81966,107.851443 30.792931,107.956152 30.882855,107.994956 30.908533,107.948145 30.918802,107.942602 30.989114,107.983254 30.983983,108.00358 31.025533,108.060246 31.052197,108.026985 31.061938,108.009123 31.109602,108.025753 31.116263,108.089811 31.204859,108.07626 31.231985,108.031297 31.217144,108.038688 31.252964,108.095354 31.268311,108.185898 31.336831,108.153869 31.371073,108.216079 31.41041,108.224086 31.464024,108.193289 31.467598,108.191441 31.492096,108.233941 31.506894,108.254883 31.49873,108.344194 31.512506,108.339266 31.539033,108.386078 31.544134,108.390389 31.591555,108.442744 31.633856,108.468614 31.636404,108.519121 31.665952,108.546838 31.665442,108.514809 31.693963,108.50557 31.734182,108.535135 31.757592,108.462454 31.780488,108.455063 31.814059,108.429194 31.809482,108.391005 31.829822,108.386078 31.854226,108.343578 31.860834,108.259194 31.967006,108.307238 31.997463,108.351585 31.971575,108.370063 31.988835,108.329411 32.020299,108.362056 32.035521,108.344194 32.067477,108.372527 32.077112,108.42981 32.061391,108.452599 32.090296,108.399628 32.147065,108.379303 32.153652,108.379303 32.153652,108.379918 32.154158,108.379918 32.154158,108.370063 32.172397,108.399013 32.194176,108.480317 32.182527,108.509882 32.201266,108.507418 32.245819,108.469846 32.270618,108.414411 32.252399,108.389773 32.263533,108.310933 32.232152,108.240716 32.274666,108.179738 32.221521,108.156948 32.239239,108.143398 32.219495,108.086731 32.233165,108.018362 32.2119,108.024521 32.177462,107.979558 32.146051,107.924739 32.197215,107.890247 32.214432,107.864377 32.201266,107.812022 32.247844,107.753508 32.338399,107.707929 32.331826,107.680827 32.397035,107.648183 32.413709,107.598291 32.411688,107.527458 32.38238,107.489886 32.425328,107.456625 32.41775,107.460937 32.453612,107.438763 32.465732,107.436299 32.529835,107.382097 32.54043,107.356843 32.506622,107.313727 32.489965,107.287858 32.457147,107.263836 32.403099,107.212097 32.428864,107.189924 32.468256,107.127098 32.482393,107.080286 32.542448,107.108004 32.600951,107.098765 32.649338,107.05996 32.686115,107.066736 32.708779,107.012533 32.721367,106.912751 32.704247,106.903512 32.721367,106.854853 32.724388,106.82344 32.705254,106.793259 32.712807,106.783404 32.735967,106.733513 32.739491,106.670071 32.694678,106.626955 32.682086,106.585687 32.68813,106.517934 32.668485,106.498224 32.649338,106.451412 32.65992,106.421231 32.616579,106.389203 32.62666,106.347935 32.671003,106.301123 32.680071,106.267863 32.673522,106.254928 32.693671,106.17424 32.6977,106.120037 32.719856,106.071378 32.758114,106.07261 32.76365,106.093552 32.82402,106.071378 32.828546,106.044277 32.864747,106.011632 32.829552,105.969132 32.849162,105.93156 32.826032,105.893371 32.838603,105.849024 32.817985,105.825002 32.824523,105.822538 32.770192,105.779423 32.750061,105.768952 32.767676,105.719061 32.759624,105.677793 32.726402,105.596489 32.69921,105.585402 32.728919,105.563844 32.724891,105.555221 32.794343,105.534279 32.790822,105.524424 32.847654,105.495475 32.873292,105.49917 32.911986,105.467757 32.930071,105.414171 32.922034,105.408011 32.885857,105.38091 32.876307,105.396308 32.85067,105.396308 32.85067,105.427721 32.784281,105.454207 32.767173,105.448663 32.732946,105.368591 32.712807,105.347033 32.68259,105.297758 32.656897,105.263265 32.652362,105.219534 32.666469,105.215222 32.63674,105.185041 32.617587,105.111128 32.593893,105.0791 32.637244,105.026745 32.650346,104.925115 32.607505,104.881999 32.600951,104.845659 32.653873,104.820405 32.662943,104.795768 32.643292,104.739717 32.635228,104.696601 32.673522,104.643015 32.661935,104.592508 32.695685,104.582653 32.722374,104.526602 32.728416,104.51182 32.753585,104.458849 32.748551,104.363994 32.822511,104.294393 32.835586,104.277147 32.90244,104.288234 32.942628,104.345516 32.940117,104.378161 32.953174,104.383704 32.994343,104.426204 33.010906,104.391711 33.035493,104.337509 33.038002,104.378161 33.109214,104.351059 33.158828,104.32827 33.223934,104.323958 33.26898,104.303632 33.304499,104.333813 33.315502,104.386168 33.298497,104.420045 33.327004,104.373849 33.345004,104.292545 33.336505,104.272219 33.391486,104.22048 33.404477,104.213089 33.446932,104.180444 33.472895,104.155191 33.542755,104.176749 33.5996,104.103452 33.663381,104.046169 33.686291,103.980264 33.670852,103.861388 33.682307,103.778236 33.658898,103.690772 33.69376,103.667983 33.685793,103.645809 33.708697,103.593454 33.716164,103.563889 33.699735,103.552186 33.671351,103.520157 33.678323,103.545411 33.719649,103.518309 33.807213,103.464723 33.80224,103.434542 33.752993,103.35447 33.743539,103.278709 33.774387,103.284868 33.80224,103.24976 33.814175,103.228202 33.79478,103.165376 33.805721,103.153673 33.819147,103.181391 33.900649,103.16476 33.929454,103.1315 33.931937,103.120413 33.953286,103.157369 33.998944,103.147514 34.036644,103.119797 34.03466,103.129652 34.065899,103.178927 34.079779,103.121644 34.112487,103.124108 34.162022,103.100087 34.181828,103.052043 34.195194,103.005848 34.184798,102.973203 34.205588,102.977515 34.252595,102.949181 34.292159,102.911609 34.312923,102.85987 34.301058,102.856791 34.270895,102.798276 34.272874,102.779798 34.236764,102.728675 34.235774,102.694799 34.198659,102.664002 34.192719,102.651067 34.165983,102.598712 34.14766,102.655994 34.113478,102.649219 34.080275,102.615958 34.099604,102.511865 34.086222,102.471213 34.072839,102.437336 34.087214,102.406539 34.033172,102.392372 33.971651,102.345561 33.969666,102.315996 33.993983,102.287047 33.977607,102.248858 33.98654,102.226069 33.963214,102.16817 33.983066,102.136142 33.965199,102.25317 33.861399,102.261177 33.821136,102.243315 33.786823,102.296286 33.783838,102.324619 33.754486,102.284583 33.719151,102.342481 33.725622,102.31538 33.665374,102.346793 33.605582,102.440416 33.574673,102.477988 33.543254,102.446575 33.53228,102.461358 33.501345,102.462589 33.449429,102.447807 33.454922,102.392988 33.404477,102.368967 33.41247,102.310452 33.397982,102.296286 33.413969,102.258098 33.409472,102.218062 33.349503,102.192192 33.337005,102.217446 33.247961,102.200815 33.223434,102.160163 33.242956,102.144765 33.273983,102.117047 33.288492,102.08933 33.227439,102.08933 33.204908,102.054838 33.189884,101.99386 33.1999,101.935345 33.186879,101.921795 33.153817,101.887302 33.135778,101.865744 33.103198,101.825708 33.119239,101.841723 33.184876,101.83002 33.213921,101.770274 33.248962,101.769658 33.26898,101.877447 33.314502,101.887302 33.383991,101.915635 33.425957,101.946432 33.442937,101.906396 33.48188,101.907012 33.539264,101.884222 33.578163,101.844186 33.602591,101.831252 33.554726,101.783208 33.556721,101.769042 33.538765,101.777665 33.533776,101.769042 33.45592,101.695745 33.433948,101.663716 33.383991,101.64955 33.323004,101.677883 33.297497,101.735781 33.279987,101.709912 33.21292,101.653861 33.162835,101.661252 33.135778,101.633535 33.101193,101.557775 33.167344,101.515275 33.192889,101.487557 33.226938,101.403174 33.225436,101.386543 33.207412,101.393935 33.157826,101.381616 33.153316,101.297232 33.262475,101.217776 33.256469,101.182668 33.26948,101.156798 33.236449,101.124769 33.221431,101.11553 33.194893,101.169733 33.10019,101.143863 33.086151,101.146327 33.056563,101.184515 33.041514,101.171581 33.009902,101.183899 32.984304,101.129081 32.989324,101.134624 32.95217,101.124153 32.909976,101.178356 32.892892,101.223935 32.855698,101.237486 32.825026,101.22332 32.725898,101.157414 32.661431,101.124769 32.658408,101.077342 32.68259,101.030531 32.660424,100.99727 32.627668,100.956618 32.621116,100.93198 32.600447,100.887633 32.632708,100.834046 32.648835,100.77122 32.643795,100.690532 32.678056,100.71209 32.645307,100.710242 32.610026,100.673286 32.628172,100.661583 32.616075,100.657887 32.546484,100.645568 32.526303,100.603069 32.553547,100.54517 32.569687,100.516837 32.632204,100.470026 32.694678,100.450932 32.694678,100.420135 32.73194,100.378251 32.698707,100.399193 32.756101,100.339447 32.719353,100.258759 32.742511,100.231041 32.696189,100.229809 32.650346,100.208252 32.606497,100.189773 32.630692,100.109701 32.640268,100.088143 32.668988,100.139266 32.724388,100.117093 32.802392,100.123252 32.837095,100.064738 32.895907,100.029629 32.895907,100.038252 32.929066,99.956332 32.948152,99.947709 32.986814,99.877492 33.045527,99.877492 32.993339,99.851007 32.941623,99.805427 32.940619,99.788181 32.956689,99.764159 32.924545,99.791877 32.883344,99.766623 32.826032,99.760464 32.769689,99.717964 32.732443,99.700718 32.76667,99.646515 32.774721,99.640355 32.790822,99.589233 32.789312,99.558436 32.839106,99.45311 32.862233,99.376118 32.899927,99.353944 32.885354,99.268944 32.878318,99.24677 32.924043,99.235067 32.982296,99.214741 32.991332,99.196263 33.035493,99.124814 33.046028,99.090322 33.079131,99.024416 33.094675,99.014561 33.081137,98.971445 33.098185,98.967134 33.115229,98.92217 33.118738,98.858728 33.150811,98.804526 33.219428,98.802062 33.270481,98.759562 33.276985,98.779888 33.370497,98.736157 33.406975,98.742316 33.477887,98.725686 33.503341,98.678258 33.522801,98.648077 33.548741,98.652389 33.595114,98.622824 33.610067,98.61728 33.637476,98.6567 33.64744,98.610505 33.682805,98.582788 33.731595,98.539672 33.746525,98.51873 33.77389,98.494092 33.768915,98.492861 33.796272,98.463295 33.848477,98.434962 33.843009,98.407245 33.867362,98.425723 33.913066,98.415252 33.956761,98.440506 33.981577,98.428187 34.029204,98.396774 34.053008,98.399854 34.085231,98.344419 34.094648,98.258188 34.083249,98.206449 34.08424,98.158405 34.107037,98.098043 34.122892,98.028442 34.122892,97.95453 34.190739,97.898479 34.209548,97.8104 34.207568,97.796849 34.199154,97.796849 34.199154,97.789458 34.182818,97.789458 34.182818,97.766668 34.158555,97.665654 34.126855,97.70261 34.036644,97.652719 33.998448,97.660111 33.956264,97.629314 33.919523,97.601596 33.929951,97.52214 33.903133,97.503662 33.912073,97.460546 33.887236,97.395257 33.889224,97.398336 33.848477,97.371851 33.842015,97.373083 33.817655,97.406344 33.795278,97.422974 33.754984,97.418046 33.728608,97.435293 33.682307,97.415583 33.605582,97.450075 33.582152,97.523372 33.577166,97.511669 33.520805,97.552321 33.465906,97.625618 33.461412,97.674893 33.432949,97.754349 33.409972,97.676125 33.341004,97.622538 33.337005,97.607756 33.263976,97.548626 33.203907,97.487648 33.168346,97.498119 33.137783,97.487032 33.107209,97.517213 33.097683,97.542466 33.035995,97.499966 33.011408,97.523988 32.988822,97.438372 32.976271,97.375547 32.956689,97.347829 32.895907,97.376163 32.886359,97.392793 32.828546,97.386018 32.77925,97.429133 32.714318,97.42359 32.70475,97.48272 32.654377,97.535075 32.638252,97.543698 32.62162,97.607756 32.614059,97.616995 32.586329,97.700763 32.53488,97.730944 32.527312,97.795617 32.521257,97.80732 32.50006,97.863986 32.499051,97.880001 32.486431,97.940363 32.482393,98.079565 32.415224,98.107283 32.391476,98.125145 32.401077,98.218768 32.342444,98.208913 32.318171,98.23047 32.262521,98.218768 32.234683,98.260035 32.208862,98.303151 32.121726,98.357354 32.087253,98.404781 32.045159,98.402933 32.026896,98.434962 32.007613,98.432498 31.922825,98.399238 31.895899,98.426339 31.856767,98.414636 31.832365,98.461448 31.800327,98.508875 31.751995,98.516882 31.717383,98.545831 31.717383,98.553839 31.660349,98.619128 31.591555,98.651157 31.57881,98.696736 31.538523,98.714599 31.508935,98.844562 31.429817,98.84333 31.416028,98.887062 31.37465,98.810685 31.306668,98.805758 31.279052,98.773113 31.249382,98.691809 31.333253,98.643766 31.338876,98.616048 31.3036,98.60373 31.257568,98.62344 31.221238,98.602498 31.192062,98.675179 31.15417,98.710287 31.1178,98.712135 31.082954,98.736772 31.049121,98.774961 31.031174,98.806374 30.995783,98.797135 30.948575,98.774345 30.908019,98.797135 30.87926,98.850105 30.849465,98.904924 30.782649,98.957895 30.765166,98.963438 30.728134,98.907388 30.698292,98.92217 30.609225,98.939417 30.598923,98.926482 30.569556,98.932025 30.521623,98.965286 30.449937,98.967134 30.33482,98.986844 30.280569,98.970829 30.260928,98.993003 30.215429,98.9813 30.182843,98.989308 30.151799,99.044742 30.079842,99.036735 30.053945,99.055213 29.958587,99.068148 29.931621,99.0238 29.846009,99.018873 29.792009,98.992387 29.677163,99.014561 29.607464,99.052133 29.563748,99.044742 29.520013,99.066916 29.421018,99.058909 29.417368,99.075539 29.316186,99.114343 29.243628,99.113727 29.221171,99.105104 29.162656,99.118039 29.100971,99.113727 29.07273,99.132206 28.94869,99.123582 28.890021,99.103872 28.841803,99.114343 28.765763,99.134053 28.734806,99.126662 28.698066,99.147604 28.640831,99.183944 28.58882,99.170394 28.566221,99.191952 28.494714,99.187024 28.44,99.16485 28.425264,99.200575 28.365774,99.229524 28.350502,99.237531 28.317842,99.28927 28.286227,99.306516 28.227714,99.374886 28.18183,99.412458 28.295186,99.392748 28.318369,99.437095 28.398419,99.404451 28.44421,99.426625 28.454207,99.396444 28.491032,99.403219 28.546246,99.463581 28.549401,99.466045 28.579886,99.504233 28.619294,99.540573 28.623497,99.53195 28.677591,99.553508 28.710664,99.614486 28.740054,99.609559 28.784122,99.625573 28.81454,99.676696 28.810345,99.717964 28.846521,99.722275 28.757369,99.755536 28.701216,99.79434 28.699116,99.834992 28.660788,99.834376 28.628225,99.873181 28.631902,99.875644 28.611939,99.91876 28.599329,99.985281 28.529422,99.990209 28.47683,100.073977 28.426317,100.057346 28.368934,100.136803 28.349975,100.176223 28.325218,100.147274 28.288862,100.188541 28.252493,100.153433 28.208202,100.102926 28.201873,100.091223 28.181302,100.062274 28.193962,100.033325 28.184467,100.021006 28.147008,100.05673 28.097922,100.088759 28.029269,100.120788 28.018703,100.196549 27.936254,100.170063 27.907699,100.210715 27.87702,100.30865 27.861149,100.30865 27.830457,100.28586 27.80611,100.304954 27.788639,100.311729 27.724028,100.327744 27.72032,100.350534 27.755809,100.412127 27.816167,100.442924 27.86644,100.504518 27.852154,100.511294 27.827811,100.54517 27.809286,100.609228 27.859033,100.634482 27.915631,100.681293 27.923035,100.719481 27.858503,100.707162 27.800816,100.757053 27.770107,100.775532 27.743098,100.782307 27.691708,100.848212 27.672099,100.827886 27.615904,100.854988 27.623858,100.91227 27.521473,100.901183 27.453517,100.936908 27.469448,100.95169 27.426961,101.021907 27.332899,101.026219 27.270679,101.042233 27.22173,101.071798 27.194585,101.119226 27.208957,101.167885 27.198311,101.167885 27.198311)),((106.264167 30.20974,106.260471 30.207672,106.260471 30.204051,106.260471 30.19681,106.264167 30.20974)),((106.976809 30.088127,106.975577 30.088127,106.976193 30.083467,106.981736 30.08502,106.980504 30.087609,106.979888 30.088127,106.978656 30.087609,106.977425 30.087609,106.976809 30.088127)),((105.558916 30.18543,105.556453 30.187499,105.550909 30.179222,105.56138 30.183878,105.558916 30.18543)))')
#     # muldata = b.multiworker()
#     # mulend_time = time.time()
#     # print(muldata)
#     print('本次单线程计算耗时：{}s'.format(str(end_time - start_time)))
#     # with open('e:/data.xml', 'w', encoding='utf-8') as f:
#     #     f.write(data.to_xml())
#     # f.close()

#     # print('本次多线程计算耗时：{}s'.format(str(mulend_time - mulstart_time)))

# def start(argsa):
#     b = GeeImageDownload(argsa[0], argsa[1], argsa[2], argsa[3], argsa[4], argsa[5], argsa[6], argsa[7], argsa[8], argsa[9],)
#     b.multiworker()
#
#
# if __name__ == '__main__':
#     multiprocessing.freeze_support()
#     start_time = time.time()
#     manager = multiprocessing.Manager()
#     info = manager.dict()
#     info['signal'] = manager.Event()
#     lock = multiprocessing.Lock()
#     args = ('Changdou_01', 'd:\\test\\Changdou_01\\Changdou_01.nev', 'Global Multi-resolution Terrain', None, None,
#             {'qee76cb60898a4d8a97c849c62c5bb82e': {'http': 'http://127.0.0.1:10809', 'https': 'http://127.0.0.1:10809'}},
#             ('ee-test@ee-test-objective.iam.gserviceaccount.com', 'C:/Users/B_Snowflake/AppData/Local/Nevasa_gee/setting/ee-test-objective-b17955d7634c.json',
#              'ee-test-objective'), 100, info, lock)
#     print(args)
#     new_process = multiprocessing.Process(target=start, args=(args,))
#     new_process.start()
#     new_process.join()
#     end_time = time.time()
#     print('本次下载耗时：{}s'.format(str(end_time - start_time)))
