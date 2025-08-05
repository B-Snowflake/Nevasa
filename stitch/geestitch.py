#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2024/7/16

import math
import queue as q
import signal
import sqlite3
import threading
import time
import traceback
import zipfile
import cv2
import os
import rasterio
import numpy as np
import geopandas as gpd
import concurrent.futures
import pycuda.driver as cudadrv
import pycuda.gpuarray as gpuarray
from dbutils.pooled_db import PooledDB
from shapely.wkt import loads
from threading import Lock
from multiprocessing import Event, Queue
from shapely import Polygon, MultiPolygon
from rasterio.features import geometry_mask


class GeeImageStitch:
    def __init__(self, taskname: str, path: str, sqlitename: str, ee_object: str, polygon: str, scale: int, region: str, start_date: str, end_date: str,   # NOQA
                 datainfo_url: str, is_export_shp: bool, progress_info: dict, process_done, signal, queue, enable_gpu: bool, kernel_func):
        self.exception = None
        self.signal = signal
        self.is_stitch_complete = False
        self.polygon = loads(polygon)
        self.ee_object = ee_object
        self.update_stitch_info = q.Queue()
        self.task_list = []
        self.is_export_shp = is_export_shp
        self.taskname = taskname
        self.database_session = None
        self.tilepath = os.path.join(path, sqlitename)
        self.tifsavepath = path
        self.queue = queue
        self.kernel_func = kernel_func
        self.process_done = process_done
        self.thread_lock = Lock()
        self.database_conn_pool()
        self.progress_info = progress_info
        self.stitch_info()
        self.savefile = os.path.join(self.tifsavepath, 'GeoTif')
        self.scale = scale
        self.region = region
        self.start_date = start_date
        self.end_date = end_date
        self.data_info_url = datainfo_url
        self.is_empty_image = False
        self.get_channels_dtype()
        if enable_gpu and kernel_func is not None:
            self.enable_gpu = self.check_cuda()
        else:
            self.enable_gpu = False
        self.crop_threading_lock = threading.Lock()
        os.environ['PROJ_LIB'] = os.path.join(os.getcwd(), 'rasterio', 'proj_data')

    def write_to_queue(self):
        data = (self.progress_info, self.process_done, self.taskname)
        self.queue.put(data)

    def check_cuda(self):
        try:
            cudadrv.init()
            num_devices = cudadrv.Device.count()
            if num_devices > 0:
                return True
            else:
                return False
        except:
            return False

    def database_conn_pool(self):
        engine = PooledDB(
            creator=sqlite3,  # 使用sqlite3作为数据库连接
            maxconnections=30,  # 最多5个连接
            database=self.tilepath,
            check_same_thread=False
        )
        self.database_session = engine
        return self.database_session

    def get_channels_dtype(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        channels_rs = cur.execute('select channels,dtype from task_info')
        self.channels, dtype = channels_rs.fetchone()
        self.dtype = np.dtype(dtype)
        conn.close()

    def export_shp(self):
        shp_file = os.path.join(self.savefile, self.taskname)
        zip_file = os.path.join(self.savefile, f'{self.taskname}_shp.zip')
        gdf = gpd.GeoDataFrame(geometry=[self.polygon])
        gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)
        gdf.to_file(shp_file + '.shp')
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Shapefile 会生成多个文件: .shp, .shx, .dbf 以及可能的 .prj 等
            for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
                zipf.write(f"{shp_file}{ext}", os.path.basename(f"{shp_file}{ext}"))
        # 删除生成的 Shapefile 文件，保留压缩包
        for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
            os.remove(f"{shp_file}{ext}")

    def stitch_info(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        cur.execute('create table if not exists stitch_info (tablename text primary key, total int, success int, fail int, start_time float, end_time float)')
        try:
            cur.execute(
                "insert into stitch_info SELECT replace(name,'_rs',''),0,0,0,null,null FROM sqlite_master WHERE type='table' and (name like 'tiles_%_rs' or name like 'tiles_%_part_%_rs')")
            conn.commit()
        except:
            pass
        res = cur.execute("SELECT name FROM sqlite_master WHERE type='table' and (name like 'tiles_%' or name like 'tiles_%_part_%') and name not like '%rs%'")
        for table in res.fetchall():
            cur.execute(f'update stitch_info set total = (select count(1) from {table[0]}) where tablename = "{table[0]}"')
        conn.commit()
        conn.close()

    def task_create(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        try:
            res = cur.execute("SELECT replace(name,'tiles_','') FROM sqlite_master WHERE type='table' and "
                              "(name like 'tiles_%_rs' or name like 'tiles_%_part_%_rs')")
            table_list = res.fetchall()
            for table in table_list:
                res = cur.execute(f'select distinct \'tiles_{table[0]}\',z,width,height,bands,count(*) from "tiles_{table[0]}" group by z,height,width,bands')
                task = res.fetchall()
                self.task_list.extend(task)
            stitch_total = 0
            for task in self.task_list:
                table_name, zoom, tile_size_width, tile_size_height, bands, count = task
                stitch_total = stitch_total + count
            self.progress_info['stitch_total'] = stitch_total
            self.write_to_queue()
        except Exception as e:
            print(e)
        conn.close()

    def add_text_watermark(self, numpy_image, watermark_text):
        try:
            # 获取图像的宽度和高度
            height, width = numpy_image.shape[:2]
            # 选择字体、大小和颜色（注意 OpenCV 的字体需要使用 cv2.FONT_HERSHEY_SIMPLEX 等）
            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = 10
            font_thickness = 20
            text_color = (0, 0, 0)  # 黑色
            alpha = 0.5  # 设置水印的透明度
            # 计算文本的大小和位置
            text_size = cv2.getTextSize(watermark_text, font, font_scale, font_thickness)[0]
            text_x = width - text_size[0] - 10
            text_y = height - 10
            # 创建透明图层用于添加水印
            overlay = numpy_image.copy()
            # 在 overlay 上添加文本水印
            cv2.putText(overlay, watermark_text, (text_x, text_y), font, font_scale, text_color, font_thickness, cv2.LINE_AA)
            # 使用透明度合成水印和原图
            cv2.addWeighted(overlay, alpha, numpy_image, 1 - alpha, 0, numpy_image)
            return numpy_image
        except Exception as e:
            print(e)

    def read_crop_info_from_db(self, tablename, bands):
        conn = self.database_session.connection()
        cur = conn.cursor()
        try:
            if bands is None:
                rs = cur.execute('select tablename, bands, x, y, x_end, y_end from crop_info where cropped <> 1 and tablename = ? and bands is null',
                                 (tablename,))
                block_list = rs.fetchall()
                count_rs = cur.execute('select count(1) from crop_info where cropped = 1 and tablename = ? and bands is null',
                                       (tablename,))
                croped_block = count_rs.fetchone()[0]
                bounds_rs = cur.execute('select ymin, ymax, xmin, xmax from crop_bounds_info where tablename = ? and bands is null',
                                        (tablename,))
                ymin, ymax, xmin, xmax = bounds_rs.fetchone()
            else:
                rs = cur.execute('select tablename, bands, x, y, x_end, y_end from crop_info where cropped <> 1 and tablename = ? and bands = ?',
                                 (tablename, bands))
                block_list = rs.fetchall()
                count_rs = cur.execute('select count(1) from crop_info where cropped = 1 and tablename = ? and bands = ?', (tablename, bands))
                croped_block = count_rs.fetchone()[0]
                bounds_rs = cur.execute('select ymin, ymax, xmin, xmax from crop_bounds_info where tablename = ? and bands = ?',
                                        (tablename, bands))
                ymin, ymax, xmin, xmax = bounds_rs.fetchone()
            if ymin is None:
                ymin = float('inf')
            if xmin is None:
                xmin = float('inf')
        except Exception as e:
            croped_block, block_list, ymin, ymax, xmin, xmax = 0, [], float('inf'), 0, float('inf'), 0
            print(e)
        finally:
            conn.close()
        return croped_block, block_list, ymin, ymax, xmin, xmax

    def create_crop_info(self, table_name, bands, map_image, map_height, map_width, block_size):
        block_list = []
        if not self.stitch_mode:
            for y in range(0, map_image.shape[0], block_size):
                for x in range(0, map_image.shape[1], block_size):
                    y_end = min(y + block_size, map_height)
                    x_end = min(x + block_size, map_width)
                    block_list.append((table_name, bands, x, y, x_end, y_end))
            self.progress_info['is_restart'] = True
            self.write_to_queue()
        else:
            croped_block, block_list, ymin, ymax, xmin, xmax = self.read_crop_info_from_db(table_name, bands)
            if not block_list:
                for y in range(0, map_image.shape[0], block_size):
                    for x in range(0, map_image.shape[1], block_size):
                        y_end = min(y + block_size, map_height)
                        x_end = min(x + block_size, map_width)
                        block_list.append((table_name, bands, x, y, x_end, y_end))
                self.progress_info['is_restart'] = True
                self.write_to_queue()
            else:
                with self.crop_threading_lock:
                    self.progress_info['croped_blocks'] = self.progress_info['croped_blocks'] + croped_block
        conn = self.database_session.connection()
        cur = conn.cursor()
        cur.execute('create table if not exists crop_info(tablename text, bands text, x int, y int, x_end int, y_end int, cropped int,'
                    'primary key (tablename, bands, x, y, x_end, y_end))')
        if bands is not None:
            cur.execute('delete from crop_info where tablename=? and bands=?', (table_name, bands))
        else:
            cur.execute('delete from crop_info where tablename=? and bands is null', (table_name,))
        cur.executemany('INSERT INTO crop_info (tablename, bands, x, y, x_end, y_end, cropped) VALUES (?, ?, ?, ?, ?, ?, 0)', block_list)
        cur.execute('create table if not exists crop_bounds_info(tablename text, bands text, ymin int, ymax int, xmin int, xmax int,'
                    'primary key (tablename, bands))')
        if bands is not None:
            cur.execute('delete from crop_bounds_info where tablename=? and bands=?', (table_name, bands))
        else:
            cur.execute('delete from crop_bounds_info where tablename=? and bands is null', (table_name,))
        cur.execute('INSERT INTO crop_bounds_info (tablename, bands, ymin, ymax, xmin, xmax) VALUES (?, ?, ?, ?, ?, ?)',
                    (table_name, bands, None, 0, None, 0))
        conn.commit()
        conn.close()
        return block_list

    def write_memo(self, top_left_geo, bottom_right_geo):
        path = os.path.join(self.savefile, 'readme.txt')
        with open(path, 'w', encoding='utf-8') as f:
            if not self.is_empty_image:
                f.write(f"数据源： {self.ee_object}\n")
                f.write(f"分辨率： {self.scale}\n")
                f.write(f"区域： {self.region}\n")
                f.write(f"起止日期： {self.start_date}_{self.end_date}\n")
                f.write(f"数据详细信息： {self.data_info_url}\n")
                f.write(f"地理坐标范围： {str(top_left_geo[1])}, {str(top_left_geo[0])}  {str(bottom_right_geo[1])}, {str(bottom_right_geo[0])}\n")
            else:
                f.write(f"错误：图像与裁剪多边形无任何重叠部分\n")

    def prepare_crop_param(self, table_name, bands, map_image, map_height, map_width, block_size):
        block_count = math.ceil(map_height / block_size) * math.ceil(map_width / block_size)
        ymin, ymax, xmin, xmax = float('inf'), 0, float('inf'), 0
        with self.thread_lock:
            self.progress_info['crop_total'] = block_count if self.progress_info['crop_total'] == 1 else self.progress_info['crop_total'] + block_count
        block_list = []
        if not self.stitch_mode:
            for y in range(0, map_image.shape[0], block_size):
                for x in range(0, map_image.shape[1], block_size):
                    y_end = min(y + block_size, map_height)
                    x_end = min(x + block_size, map_width)
                    block_list.append((table_name, bands, x, y, x_end, y_end))
            self.progress_info['is_restart'] = True
            self.write_to_queue()
            self.create_crop_info(table_name, bands, map_image, map_height, map_width, block_size)
        else:
            croped_block, block_list, ymin, ymax, xmin, xmax = self.read_crop_info_from_db(table_name, bands)
            if not block_list:
                for y in range(0, map_image.shape[0], block_size):
                    for x in range(0, map_image.shape[1], block_size):
                        y_end = min(y + block_size, map_height)
                        x_end = min(x + block_size, map_width)
                        block_list.append((table_name, bands, x, y, x_end, y_end))
                self.progress_info['is_restart'] = True
                self.write_to_queue()
                self.create_crop_info(table_name, bands, map_image, map_height, map_width, block_size)
            else:
                with self.crop_threading_lock:
                    self.progress_info['croped_blocks'] = self.progress_info['croped_blocks'] + croped_block
        return block_list, ymin, ymax, xmin, xmax

    def apply_mask_and_crop_gpu(self, table_name, bands, map_image, transform_parameters, block_size):
        west, south, east, north, map_width, map_height = transform_parameters
        block_shape = (block_size, block_size, self.channels)
        mmap_dtype = map_image.dtype
        block_list, ymin, ymax, xmin, xmax = self.prepare_crop_param(table_name, bands, map_image, map_height, map_width, block_size)
        image_crop = CudaImageCrop(block_list, self.kernel_func, self.database_session, self.crop_threading_lock, self.progress_info, self.write_to_queue,
                                   block_shape, block_size, mmap_dtype, map_image, transform_parameters, self.polygon, self.signal,
                                   (ymin, ymax, xmin, xmax))  # NOQA
        cropped_image, top_left_geo, bottom_right_geo = image_crop.worker()
        return cropped_image, top_left_geo, bottom_right_geo

    def apply_mask_and_crop_cpu(self, table_name, bands, map_image, transform_parameters, block_size):
        conn = self.database_session.connection()
        cur = conn.cursor()
        # Initialize boundaries
        # mask = geometry_mask([self.polygon], out_shape=map_image.shape[:2], transform=transform, invert=True)
        west, south, east, north, map_width, map_height = transform_parameters
        transform = rasterio.transform.from_bounds(west, south, east, north, map_width, map_height)
        pixel_width = (east - west) / map_width  # 每个像素的经度
        pixel_height = (north - south) / map_height  # 每个像素的纬度
        # Apply mask in blocks and find the bounding box of the mask
        block_list, ymin, ymax, xmin, xmax = self.prepare_crop_param(table_name, bands, map_image, map_height, map_width, block_size)
        for table_name, bands, x, y, x_end, y_end in block_list:
            if self.signal.is_set():
                return None, None, None
            else:
                block_west = west + x * pixel_width
                block_south = north - y_end * pixel_height
                block_east = west + x_end * pixel_width
                block_north = north - y * pixel_height
                block_width = x_end - x
                block_height = y_end - y
                block_transform = rasterio.transform.from_bounds(block_west, block_south, block_east, block_north, block_width, block_height)
                # Generate the mask for the current block
                block_mask = geometry_mask([self.polygon], out_shape=(y_end - y, x_end - x), transform=block_transform, invert=True)
                # Apply the block mask directly to the map_image (without storing the full mask)
                map_image[y:y_end, x:x_end][~block_mask] = 0
                # Update boundaries if there are any masked areas in the current block
                if np.any(block_mask):
                    y_coords, x_coords = np.where(block_mask)
                    ymin = min(ymin, y + y_coords.min())
                    ymax = max(ymax, y + y_coords.max())
                    xmin = min(xmin, x + x_coords.min())
                    xmax = max(xmax, x + x_coords.max())
                with self.crop_threading_lock:
                    self.progress_info['croped_blocks'] = self.progress_info['croped_blocks'] + 1
                if bands is not None:
                    cur.execute('UPDATE crop_info SET cropped = 1 WHERE tablename = ? AND bands = ? AND x = ? AND y = ? AND x_end = ? AND y_end = ?',
                                (table_name, bands, x, y, x_end, y_end))
                    cur.execute('update crop_bounds_info set ymin = ?, ymax = ?, xmin = ?, xmax = ? where tablename = ? and bands = ?',
                                (int(ymin) if ymin != float('inf') else None, int(ymax), int(xmin) if xmin != float('inf') else None, int(xmax), table_name,
                                 bands))
                else:
                    cur.execute('UPDATE crop_info SET cropped = 1 WHERE tablename = ? AND bands is null AND x = ? AND y = ? AND x_end = ? AND y_end = ?',
                                (table_name, x, y, x_end, y_end))
                    cur.execute('update crop_bounds_info set ymin = ?, ymax = ?, xmin = ?, xmax = ? where tablename = ? and bands is null',
                                (int(ymin) if ymin != float('inf') else None, int(ymax), int(xmin) if xmin != float('inf') else None, int(xmax), table_name))
                conn.commit()
                self.write_to_queue()
        # Ensure valid boundaries
        if ymin == float('inf'):
            ymin, xmin, ymax, xmax = 0, 0, 0, 0
        # Crop the image to the determined bounding box
        if ymin < ymax and xmin < xmax:
            cropped_image = map_image[ymin:ymax, xmin:xmax]
        else:
            cropped_image = np.zeros((512, 512, 3), dtype=map_image.dtype)
            self.is_empty_image = True
        # Calculate the geographic coordinates of the top-left and bottom-right corners
        top_left_geo = (ymin * transform.e + transform.f, xmin * transform.a + transform.c)
        bottom_right_geo = (ymax * transform.e + transform.f, xmax * transform.a + transform.c)
        conn.close()
        print(ymin, ymax, xmin, xmax)
        return cropped_image, top_left_geo, bottom_right_geo

    def tiles_stitch(self, task: tuple, block_size=2048):
        table_name, zoom, tile_size_width, tile_size_height, bands, count = task
        ee_object = self.ee_object.replace(' ', '')
        if 'part' in table_name and bands is not None:
            temp_file = os.path.join(self.savefile, f'temp_{table_name.replace(f'tiles_{zoom}_part_', '').replace('_rs', '')}_{bands}')
            geo_file = os.path.join(self.savefile,
                                    f'{self.taskname}_{ee_object}_{table_name.replace(f'tiles_{zoom}_part_', '').replace('_rs', '')}_{bands}.tif')
        elif 'part' in table_name and bands is None:
            temp_file = os.path.join(self.savefile, f'temp_{table_name.replace(f'tiles_{zoom}_part_', '').replace('_rs', '')}')
            geo_file = os.path.join(self.savefile, f'{self.taskname}_{ee_object}_{table_name.replace(f'tiles_{zoom}_part_', '').replace('_rs', '')}.tif')
        elif 'part' not in table_name and bands is not None:
            temp_file = os.path.join(self.savefile, f'temp_{bands}')
            geo_file = os.path.join(self.savefile, f'{self.taskname}_{ee_object}_{bands}.tif')
        else:
            temp_file = os.path.join(self.savefile, f'temp')
            geo_file = os.path.join(self.savefile, f'{self.taskname}_{ee_object}.tif')
        conn = self.database_session.connection()
        cur = conn.cursor()
        try:
            res = cur.execute(f'select max(x),max(y),min(x),min(y) from (SELECT x,y FROM "{table_name}" where z = {zoom})')
            max_x, max_y, min_x, min_y = res.fetchone()
            if os.path.exists(temp_file) and os.path.getsize(temp_file) != 0:
                mode = 'r+'
                self.stitch_mode = True
                stitched_res = cur.execute(f'select success from stitch_info where tablename = "{table_name.replace('_rs', '')}"')
                stitched = stitched_res.fetchone()[0]
                if bands is None:
                    res = cur.execute(
                        f'SELECT image,x,y,z,shape,dtype FROM "{table_name}" WHERE z = ? AND status = 1 and stitch_status is null AND bands IS NULL',
                        (zoom,)
                    )
                else:
                    res = cur.execute(
                        f'SELECT image,x,y,z,shape,dtype FROM "{table_name}" WHERE z = ? AND status = 1 and stitch_status is null AND bands = ?',
                        (zoom, bands)
                    )
                with self.thread_lock:
                    self.progress_info['stitched_tiles'] = stitched
                    self.progress_info[f'{table_name}_stitched_tiles'] = stitched
                    self.write_to_queue()
            else:
                mode = 'w+'
                self.stitch_mode = False
                if bands is None:
                    res = cur.execute(
                        f'SELECT image,x,y,z,shape,dtype FROM "{table_name}" WHERE z = ? AND status = 1 AND bands IS NULL',
                        (zoom,)
                    )
                else:
                    res = cur.execute(
                        f'SELECT image,x,y,z,shape,dtype FROM "{table_name}" WHERE z = ? AND status = 1 AND bands = ?',
                        (zoom, bands)
                    )
                stitched = 0
            # 假设有 num_tiles 个瓦片，根据实际情况修改
            num_tiles_x = max_x - min_x + 1
            num_tiles_y = max_y - min_y + 1
            # 计算空白图像的大小
            map_width = num_tiles_x * tile_size_width
            map_height = num_tiles_y * tile_size_height
            try:
                os.mkdir(self.savefile)
            except:
                pass
            map_image = np.memmap(filename=temp_file, dtype=self.dtype, mode=mode, shape=(map_height, map_width, self.channels))
            # 从数据库遍历读取的每个瓦片的数据和位置信息，并将其放置到空白图像上
            while True:
                row = res.fetchone()
                if not row or self.signal.is_set():  # 监听来自主进程的终止信号，如果有终止信号，安全关闭拼接线程组， 如果拼接完成，结束拼接循环，开始写入磁盘
                    break
                tile_data, x_position, y_position, z, shape, dtype = row
                image_shape = tuple(map(int, shape.strip('()').split(',')))
                image_dtype = np.dtype(dtype)  # 将 dtype 字符串转换回 np.dtype
                tile_image = np.frombuffer(tile_data, dtype=image_dtype).reshape(image_shape)
                if self.ee_object == 'Dynamic World':
                    tile_image = np.flipud(tile_image)
                # 将瓦片放置到空白图像的对应位置
                start_x = (x_position - min_x) * tile_size_width
                start_y = (y_position - min_y) * tile_size_height
                # 确保放置位置在图像范围内
                tile_width = min(tile_image.shape[1], map_width - start_x)
                tile_height = min(tile_image.shape[0], map_height - start_y)
                map_image[start_y:start_y + tile_height, start_x:start_x + tile_width] = tile_image[:tile_height, :tile_width]
                stitched += 1
                with self.thread_lock:
                    self.progress_info['stitched_tiles'] = self.progress_info['stitched_tiles'] + 1
                    self.progress_info[f'{table_name}_stitched_tiles'] = stitched
                    self.write_to_queue()
                self.update_stitch_info.put((x_position, y_position, z, table_name))
                if stitched % 200 == 0:
                    map_image.flush()
            if not self.signal.is_set():
                # 将 WKT 字符串转换为掩膜
                north, west = self.tile_to_latlon_top_left(min_x, min_y, zoom)
                south, east = self.tile_to_latlon_top_left(max_x + 1, max_y + 1, zoom)
                # 生成变换矩阵
                transform_parameters = (west, south, east, north, map_width, map_height)
                # 应用掩膜并裁剪图像
                if not self.enable_gpu:
                    cropped_image, top_left_geo, bottom_right_geo = self.apply_mask_and_crop_cpu(table_name, bands, map_image, transform_parameters, block_size)
                else:
                    try:
                        cropped_image, top_left_geo, bottom_right_geo = self.apply_mask_and_crop_gpu(table_name, bands, map_image, transform_parameters, block_size)
                    except:
                        traceback.print_exc()
                        raise GPUUnavailableError("Error when trying to use GPU")
                if cropped_image is not None:
                    self.progress_info['is_cropping_complete'] = True
                    self.write_to_queue()
                    print(f'已完成任务：{'GPU' if self.enable_gpu else 'CPU'}裁剪{self.taskname}')
                    rs = cv2.imwrite(geo_file, cropped_image)
                    print(f'tif写入结果：{'成功' if rs else '失败'}')
                    self.to_geotiff(max_x=max_x, max_y=max_y, min_x=min_x, min_y=min_y, zoom=zoom, top_left_geo=top_left_geo, bottom_right_geo=bottom_right_geo,
                                    map_image=cropped_image, table_name=table_name, bands=bands)
                    # self.write_memo(top_left_geo, bottom_right_geo)
                    map_image._mmap.close()
                    os.remove(temp_file)
                    return str(zoom) + ': success'
                else:
                    return
        except Exception as e:
            print(e)
            self.exception = e
        conn.close()

    @staticmethod
    def tile_to_latlon_top_left(x, y, zoom):
        n = 2.0 ** zoom
        lon_deg = x / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        lat_deg = math.degrees(lat_rad)
        return lat_deg, lon_deg

    def to_geotiff(self, max_x, max_y, min_x, min_y, zoom, map_image, table_name, bands, top_left_geo=None, bottom_right_geo=None):
        ee_object = self.ee_object.replace(' ', '')
        if top_left_geo is None:
            top_left_lat, top_left_lon = self.tile_to_latlon_top_left(min_x, min_y, zoom)
        else:
            top_left_lat, top_left_lon = top_left_geo[0], top_left_geo[1]
        if bottom_right_geo is None:
            bottom_right_lat, bottom_right_lon = self.tile_to_latlon_top_left(max_x + 1, max_y + 1, zoom)
        else:
            bottom_right_lat, bottom_right_lon = bottom_right_geo[0], bottom_right_geo[1]
        if 'part' in table_name and bands is not None:
            tfw_file = os.path.join(self.savefile,
                                    f'{self.taskname}_{ee_object}_{table_name.replace(f'tiles_{zoom}_part_', '').replace('_rs', '')}_{bands}.tfw')
            prj_file = os.path.join(self.savefile,
                                    f'{self.taskname}_{ee_object}_{table_name.replace(f'tiles_{zoom}_part_', '').replace('_rs', '')}_{bands}.prj')
        elif 'part' in table_name and bands is None:
            tfw_file = os.path.join(self.savefile, f'{self.taskname}_{ee_object}_{table_name.replace(f'tiles_{zoom}_part_', '').replace('_rs', '')}.tfw')
            prj_file = os.path.join(self.savefile + f'{self.taskname}_{ee_object}_{table_name.replace(f'tiles_{zoom}_part_', '').replace('_rs', '')}.prj')
        elif 'part' not in table_name and bands is not None:
            tfw_file = os.path.join(self.savefile, f'{self.taskname}_{ee_object}_{bands}.tfw')
            prj_file = os.path.join(self.savefile, f'{self.taskname}_{ee_object}_{bands}.prj')
        else:
            tfw_file = os.path.join(self.savefile, f'{self.taskname}_{ee_object}.tfw')
            prj_file = os.path.join(self.savefile, f'{self.taskname}_{ee_object}.prj')
        # crs = CRS('EPSG:4326')
        crs = 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS84",SPHEROID["WGS84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'
        height = map_image.shape[0]
        width = map_image.shape[1]
        pixel_width = (bottom_right_lon - top_left_lon) / width
        pixel_height = -(top_left_lat - bottom_right_lat) / height
        row_rotation = 0
        column_skew = 0
        with open(tfw_file, 'w') as file:
            # Write the seven lines of the TFW file
            file.write(f"{pixel_width}\n")
            file.write(f"{row_rotation}\n")
            file.write(f"{column_skew}\n")
            file.write(f"{pixel_height}\n")
            file.write(f"{top_left_lon}\n")
            file.write(f"{top_left_lat}\n")
        with open(prj_file, 'w') as file:
            file.write(crs)

    def update_sqlite_stitch_info(self):
        conn = self.database_session.connection()
        cur = conn.cursor()
        while not (self.is_stitch_complete and self.update_stitch_info.empty()):
            try:
                stitched = self.progress_info['stitched_tiles']
                x, y, z, table_name = self.update_stitch_info.get(block=False, timeout=1)
                stitch_status_sql = f'update "{table_name}" set stitch_status = 1 where (x,y,z) = ({x},{y},{z})'
                stitch_info_sql = f'update stitch_info set success = {stitched} where tablename = "{table_name.replace('_rs', '')}"'
                cur.execute(stitch_status_sql)
                cur.execute(stitch_info_sql)
                conn.commit()
            except Exception as e:
                pass
        self.progress_info['is_update_sqlite_complete'] = True
        self.write_to_queue()
        conn.close()

    def multiworker(self):
        self.task_create()
        to_sqlite_results = []
        to_sqlite_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        to_sqlite_result = to_sqlite_thread.submit(self.update_sqlite_stitch_info)
        to_sqlite_results.append(to_sqlite_result)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # 提交多个任务给线程池执行拼接任务
            self.futures = [executor.submit(self.tiles_stitch, stitch_task, ) for stitch_task in self.task_list]
        if self.exception is not None:
            raise self.exception
        self.is_stitch_complete = True
        if self.is_export_shp:
            self.export_shp()
        concurrent.futures.wait(to_sqlite_results)
        self.progress_info['is_stitch_complete'] = True
        self.write_to_queue()


class CudaImageCrop:
    def __init__(self, block_list, kernel_func, database_conn, crop_threading_lock, progress_info, write_to_queue, block_shape, block_size,
                 mmap_dtype, map_image, transform_parameters, polygon, signal, extremum, num_buffers=4):
        self._producer_context = cudadrv.Device(0).make_context()
        self._producer_context.push()
        self.producer_stream = cudadrv.Stream()
        self.block_list = block_list
        self.signal = signal
        self.database_conn = database_conn.connection()
        self.extremum = extremum
        self.crop_threading_lock = crop_threading_lock
        self.progress_info = progress_info
        self.write_to_queue = write_to_queue
        self.polygon = polygon
        self.num_buffers = num_buffers
        self.mmap_dtype = mmap_dtype
        self.map_image = map_image
        self.block_shape = block_shape
        self.kernel_func = kernel_func
        self.block_size = block_size
        buffer_shape = np.empty(block_shape, dtype=mmap_dtype).flatten().shape
        self.host_image_buffers = [cudadrv.pagelocked_empty(buffer_shape, dtype=mmap_dtype) for _ in range(num_buffers)]  # NOQA
        self.device_image_buffers = [gpuarray.empty(buffer_shape, dtype=mmap_dtype) for _ in range(num_buffers)]
        self.host_params_buffers = [cudadrv.pagelocked_empty(12, dtype=np.float64) for _ in range(num_buffers)]  # NOQA
        self.device_params_buffers = [gpuarray.empty(12, dtype=np.float64) for _ in range(num_buffers)]
        self.host_extremum_buffers = [cudadrv.pagelocked_empty(4, dtype=np.int32) for _ in range(num_buffers)]  # NOQA
        self.device_extremum_buffers = [gpuarray.empty(4, dtype=np.int32) for _ in range(num_buffers)]
        self.buffer_ready = [threading.Event() for _ in range(num_buffers)]
        self.cuda_event = [cudadrv.Event() for _ in range(num_buffers)]
        for event in self.buffer_ready:
            event.set()  # 初始时所有缓冲区可用
        self.transform_parameters = transform_parameters
        self.streams = [cudadrv.Stream() for _ in range(num_buffers)]  # NOQA
        self._consumer_thread = threading.Thread(target=self._collect_result)
        self.task_queue = q.Queue()
        self.all_complete = False

    @staticmethod
    def extract_coords(geom):
        all_coords = []  # 存储所有多边形的顶点坐标
        offsets = []  # 存储每个多边形的起始索引
        vertex_counts = []  # 存储每个多边形的顶点数
        current_offset = 0  # 当前已处理顶点数（用于计算偏移量）
        if isinstance(geom, Polygon):
            coords = np.array(geom.exterior.coords, dtype=np.float64)
            all_coords.append(coords)
            offsets.append(current_offset)
            vertex_counts.append(len(coords))
        elif isinstance(geom, MultiPolygon):
            for poly in geom.geoms:
                coords = np.array(poly.exterior.coords, dtype=np.float64)
                all_coords.append(coords)
                offsets.append(current_offset)
                vertex_counts.append(len(coords))
                current_offset += len(coords)  # 更新偏移量为下一个多边形的起始位置
        # 将所有多边形的坐标连接成一个二维数组
        poly_coords = np.vstack(all_coords) if all_coords else np.empty((0, 2), dtype=np.float64)
        poly_offsets = np.array(offsets, dtype=np.int32)
        poly_vertex_counts = np.array(vertex_counts, dtype=np.int32)
        return poly_coords.flatten(order='C'), poly_offsets, poly_vertex_counts

    def _start_gpu_kernel(self):
        module = cudadrv.module_from_buffer(self.kernel_func)
        # 获取内核函数
        process_image_kernel = module.get_function('process_image_kernel')
        west, south, east, north, map_width, map_height = self.transform_parameters
        self.transform = rasterio.transform.from_bounds(west, south, east, north, map_width, map_height)
        pixel_width = (east - west) / map_width  # 每个像素的经度
        pixel_height = (north - south) / map_height  # 每个像素的纬度
        poly_coords, poly_offsets, poly_vertex_counts = self.extract_coords(self.polygon)
        n_polygon = len(poly_offsets)
        rows, cols = np.indices((self.block_size, self.block_size))
        d_poly_coords = cudadrv.to_device(poly_coords)
        d_poly_offsets = cudadrv.to_device(poly_offsets)
        d_poly_vertex_counts = cudadrv.to_device(poly_vertex_counts)
        threads_per_block = (32, 32, 1)
        blocks_per_grid = (
            (cols.shape[1] + threads_per_block[0] - 1) // threads_per_block[0],
            (rows.shape[0] + threads_per_block[1] - 1) // threads_per_block[1],
            1)
        if self.map_image.ndim == 2:
            n_bands = 1
        else:
            n_bands = self.map_image.shape[2]
        for i, value in enumerate(self.block_list):
            if self.signal.is_set():
                return
            else:
                table_name, bands, x, y, x_end, y_end = value
                block_west = west + x * pixel_width
                block_south = north - y_end * pixel_height
                block_east = west + x_end * pixel_width
                block_north = north - y * pixel_height
                block_width = x_end - x
                block_height = y_end - y
                block_transform = rasterio.transform.from_bounds(block_west, block_south, block_east, block_north, block_width, block_height)
                c, a, b, f, d, e = block_transform.c, block_transform.a, block_transform.b, block_transform.f, block_transform.d, block_transform.e
                buf_idx = i % self.num_buffers
                self.buffer_ready[buf_idx].wait()
                self.buffer_ready[buf_idx].clear()
                stream = self.streams[buf_idx]
                host_image_buf = self.host_image_buffers[buf_idx]
                device_image_buf = self.device_image_buffers[buf_idx]
                host_params_buf = self.host_params_buffers[buf_idx]
                device_params_buf = self.device_params_buffers[buf_idx]
                host_extremum_buf = self.host_extremum_buffers[buf_idx]
                device_extremum_buf = self.device_extremum_buffers[buf_idx]
                buffer_ready = self.buffer_ready[buf_idx]
                cuda_event = self.cuda_event[buf_idx]
                image = self.map_image[y:y_end, x:x_end]
                block_shape = image.shape
                # 拷贝数据 → pinned
                try:
                    host_image_buf[:] = image.flatten(order='C')
                except:
                    image = self._pad_tile(image)
                    np.save('test_dc.npy', image)
                    host_image_buf[:] = image.flatten(order='C')
                cudadrv.memcpy_htod_async(device_image_buf.gpudata, host_image_buf, stream=stream)
                host_params_buf[:] = np.array([c, a, b, f, d, e, float(n_polygon), float(y), float(x),
                                               float(image.shape[0]), float(image.shape[1]), float(n_bands)], dtype=np.float64)
                cudadrv.memcpy_htod_async(device_params_buf.gpudata, host_params_buf, stream=stream)
                host_extremum_buf[:] = np.array([(int32_max := np.iinfo(np.int32).max), 0, int32_max, 0], dtype=np.int32)
                cudadrv.memcpy_htod_async(device_extremum_buf.gpudata, host_extremum_buf, stream=stream)
                # 上传 → 执行 → 下载（异步）
                # 启动核函数
                process_image_kernel(device_image_buf.gpudata, device_params_buf.gpudata, device_extremum_buf.gpudata, d_poly_coords,
                                     d_poly_offsets, d_poly_vertex_counts, block=threads_per_block, grid=blocks_per_grid, stream=stream)
                cuda_event.record(stream)
                self.task_queue.put((cuda_event, block_shape, host_image_buf, device_image_buf, host_extremum_buf, device_extremum_buf,
                                     buffer_ready, table_name, bands, x, y, x_end, y_end))
        self.all_complete = True

    def worker(self):
        self._consumer_thread.start()
        self._start_gpu_kernel()
        self._consumer_thread.join()
        self._producer_context.pop()
        self._producer_context.detach()
        if self.signal.is_set():
            return None, None, None
        else:
            return self.cropped_image, self.top_left_geo, self.bottom_right_geo

    def _record_process(self, table_name, bands, x, y, x_end, y_end, ymin, ymax, xmin, xmax):
        conn = self.database_conn
        cur = conn.cursor()
        with self.crop_threading_lock:
            self.progress_info['croped_blocks'] = self.progress_info['croped_blocks'] + 1
        if bands is not None:
            cur.execute('UPDATE crop_info SET cropped = 1 WHERE tablename = ? AND bands = ? AND x = ? AND y = ? AND x_end = ? AND y_end = ?',
                        (table_name, bands, x, y, x_end, y_end))
            cur.execute('update crop_bounds_info set ymin = ?, ymax = ?, xmin = ?, xmax = ? where tablename = ? and bands = ?',
                        (int(ymin) if ymin != float('inf') else None, int(ymax), int(xmin) if xmin != float('inf') else None, int(xmax), table_name,
                         bands))
        else:
            cur.execute('UPDATE crop_info SET cropped = 1 WHERE tablename = ? AND bands is null AND x = ? AND y = ? AND x_end = ? AND y_end = ?',
                        (table_name, x, y, x_end, y_end))
            cur.execute('update crop_bounds_info set ymin = ?, ymax = ?, xmin = ?, xmax = ? where tablename = ? and bands is null',
                        (int(ymin) if ymin != float('inf') else None, int(ymax), int(xmin) if xmin != float('inf') else None, int(xmax), table_name))
        conn.commit()
        self.write_to_queue()

    def _collect_result(self):
        self._consumer_context = cudadrv.Device(0).make_context()
        self._consumer_context.push()
        consumer_stream = cudadrv.Stream()
        ymin, ymax, xmin, xmax = self.extremum
        processed_count, total_tasks = 0, len(self.block_list)
        # shape = self.map_image.shape
        while processed_count < total_tasks:
            if not self.signal.is_set():
                try:
                    task = self.task_queue.get(timeout=1)
                    (cuda_event, block_shape, host_image_buf, device_image_buf, host_extremum_buf, device_extremum_buf,
                     buffer_ready, table_name, bands, x, y, x_end, y_end) = task
                    cuda_event.synchronize()
                    cudadrv.memcpy_dtoh_async(host_image_buf, device_image_buf.gpudata, stream=consumer_stream)  # NOQA
                    cudadrv.memcpy_dtoh_async(host_extremum_buf, device_extremum_buf.gpudata, stream=consumer_stream)  # NOQA
                    consumer_stream.synchronize()
                    try:
                        self.map_image[y:y_end, x:x_end] = host_image_buf.reshape(block_shape, order='C')
                    except:
                        cropped_image = host_image_buf.reshape(self.block_shape, order='C')
                        self.map_image[y:y_end, x:x_end] = cropped_image[:(y_end-y), :(x_end-x)]
                    ymin = min(ymin, host_extremum_buf[0])
                    ymax = max(ymax, host_extremum_buf[1])
                    xmin = min(xmin, host_extremum_buf[2])
                    xmax = max(xmax, host_extremum_buf[3])
                    self.map_image.flush()
                    self._record_process(table_name, bands, x, y, x_end, y_end, ymin, ymax, xmin, xmax)
                    buffer_ready.set()
                    processed_count += 1
                except q.Empty:
                    pass
                except Exception as e:
                    print(e)
            else:
                self._consumer_context.pop()
                self._consumer_context.detach()
                return
        self.top_left_geo = (ymin * self.transform.e + self.transform.f, xmin * self.transform.a + self.transform.c)
        self.bottom_right_geo = (ymax * self.transform.e + self.transform.f, xmax * self.transform.a + self.transform.c)
        self.cropped_image = self.map_image[ymin:ymax, xmin:xmax]
        # self.cropped_image = self.map_image
        self._consumer_context.pop()
        self._consumer_context.detach()

    def _pad_tile(self, block_image, fill_value=0):
        padded = np.full(self.block_shape, fill_value, dtype=block_image.dtype)
        h, w = block_image.shape[:2]
        padded[:h, :w, ...] = block_image
        return padded


class GPUUnavailableError(Exception):
    """在尝试使用 GPU 时发生错误"""
    pass


def start(argsa):
    b = GeeImageStitch(*argsa)
    b.multiworker()


if __name__ == '__main__':
    connection = sqlite3.connect(database='../resources/data/data.nev', check_same_thread=False)
    cur = connection.cursor()
    kernel_func = cur.execute('select binary from ptx_modules where name=?', ('process_image_kernel',)).fetchone()[0]
    cur.close()
    polygon = 'POLYGON ((116.4436400000000020 39.8728460000000027, 116.4441180000000031 39.8753620000000026, 116.4444629999999989 39.8761569999999992, 116.4453730000000036 39.8777940000000015, 116.4456169999999986 39.8785760000000025, 116.4456420000000065 39.8792789999999968, 116.4453390000000041 39.8810020000000023, 116.4447080000000057 39.8848580000000013, 116.4441790000000054 39.8890170000000026, 116.4440680000000015 39.8900369999999995, 116.4459640000000036 39.8901469999999989, 116.4459030000000013 39.8915190000000024, 116.4469339999999988 39.8915160000000029, 116.4469859999999954 39.8912040000000019, 116.4473590000000058 39.8911530000000027, 116.4473930000000053 39.8908050000000003, 116.4481990000000025 39.8907110000000031, 116.4483110000000039 39.8905040000000000, 116.4500170000000026 39.8904489999999967, 116.4503820000000047 39.8902440000000027, 116.4509350000000012 39.8902429999999981, 116.4510130000000032 39.8919619999999995, 116.4523729999999944 39.8919799999999967, 116.4523910000000058 39.8928659999999979, 116.4508660000000049 39.8928659999999979, 116.4508749999999964 39.8940880000000035, 116.4471599999999967 39.8941850000000002, 116.4470040000000068 39.8970589999999987, 116.4468310000000031 39.8974600000000024, 116.4468569999999943 39.8983419999999995, 116.4468240000000065 39.9000419999999991, 116.4459650000000011 39.9000600000000034, 116.4458790000000050 39.9010340000000028, 116.4480259999999987 39.9012340000000023, 116.4490660000000020 39.9012490000000000, 116.4487199999999945 39.9032419999999988, 116.4470230000000015 39.9031599999999997, 116.4460870000000057 39.9031510000000011, 116.4438090000000017 39.9032249999999991, 116.4419639999999987 39.9030959999999979, 116.4401960000000003 39.9026610000000019, 116.4386970000000048 39.9025930000000031, 116.4376670000000047 39.9025779999999983, 116.4371799999999979 39.9023309999999967, 116.4367900000000020 39.9020149999999987, 116.4364779999999939 39.9020459999999986, 116.4364530000000002 39.9026190000000014, 116.4363220000000041 39.9036299999999997, 116.4359589999999969 39.9055359999999979, 116.4358390000000014 39.9068389999999980, 116.4357509999999962 39.9082269999999966, 116.4356719999999967 39.9105149999999966, 116.4355690000000010 39.9113249999999979, 116.4350840000000034 39.9133580000000023, 116.4349880000000041 39.9139580000000009, 116.4348080000000039 39.9172250000000020, 116.4346519999999998 39.9211689999999990, 116.4344880000000018 39.9245179999999991, 116.4343089999999989 39.9286859999999990, 116.4359409999999997 39.9286519999999996, 116.4359450000000038 39.9284490000000005, 116.4370559999999983 39.9284460000000010, 116.4369310000000013 39.9294050000000027, 116.4373099999999965 39.9294009999999986, 116.4373099999999965 39.9289660000000026, 116.4375809999999944 39.9289610000000010, 116.4375900000000001 39.9287009999999967, 116.4387279999999976 39.9287040000000033, 116.4387300000000067 39.9285250000000005, 116.4390300000000025 39.9285319999999970, 116.4390590000000003 39.9295370000000034, 116.4395719999999983 39.9297040000000010, 116.4403060000000067 39.9297150000000016, 116.4404169999999965 39.9286779999999979, 116.4417239999999936 39.9286290000000008, 116.4436809999999980 39.9286689999999993, 116.4437530000000010 39.9335260000000005, 116.4437099999999958 39.9366669999999999, 116.4437530000000010 39.9369230000000002, 116.4444550000000049 39.9384209999999982, 116.4445410000000010 39.9391529999999975, 116.4445410000000010 39.9398830000000018, 116.4446109999999948 39.9407890000000023, 116.4447839999999985 39.9416220000000024, 116.4449479999999966 39.9418280000000010, 116.4461790000000008 39.9424810000000008, 116.4465509999999995 39.9429700000000025, 116.4472719999999981 39.9451559999999972, 116.4467089999999985 39.9461110000000019, 116.4463439999999963 39.9461999999999975, 116.4446719999999971 39.9462460000000021, 116.4436409999999995 39.9462090000000032, 116.4426449999999988 39.9457049999999967, 116.4421159999999986 39.9451430000000016, 116.4417700000000053 39.9450349999999972, 116.4410599999999931 39.9451490000000007, 116.4405560000000008 39.9453000000000031, 116.4407510000000059 39.9457719999999981, 116.4423259999999942 39.9471589999999992, 116.4422440000000023 39.9497009999999975, 116.4402800000000013 39.9496889999999993, 116.4399509999999935 39.9521349999999984, 116.4390729999999934 39.9521659999999983, 116.4354189999999960 39.9521179999999987, 116.4360170000000068 39.9508470000000031, 116.4357389999999981 39.9507990000000035, 116.4362160000000017 39.9498170000000030, 116.4367969999999985 39.9496910000000014, 116.4367009999999993 39.9492479999999972, 116.4350979999999964 39.9492329999999995, 116.4348209999999995 39.9490380000000016, 116.4343609999999956 39.9490679999999969, 116.4342050000000057 39.9493069999999975, 116.4339990000000000 39.9499380000000031, 116.4334860000000020 39.9501410000000021, 116.4330530000000010 39.9501769999999965, 116.4294799999999981 39.9501630000000034, 116.4294639999999958 39.9504310000000018, 116.4290560000000028 39.9505059999999972, 116.4290490000000062 39.9510709999999989, 116.4298900000000003 39.9510130000000032, 116.4298979999999943 39.9512920000000022, 116.4302179999999964 39.9512920000000022, 116.4301999999999992 39.9517859999999985, 116.4294900000000013 39.9518500000000003, 116.4295520000000010 39.9535339999999977, 116.4294210000000049 39.9535440000000008, 116.4293870000000055 39.9542980000000014, 116.4290220000000033 39.9542780000000022, 116.4289959999999979 39.9556230000000028, 116.4292039999999986 39.9556490000000011, 116.4291609999999935 39.9567750000000004, 116.4290040000000062 39.9572689999999966, 116.4298040000000043 39.9572560000000010, 116.4298289999999980 39.9565350000000024, 116.4306179999999955 39.9565600000000032, 116.4305140000000023 39.9592469999999977, 116.4254329999999982 39.9591640000000012, 116.4253379999999964 39.9620859999999993, 116.4248699999999985 39.9622839999999968, 116.4162369999999953 39.9621989999999983, 116.4153830000000056 39.9621520000000032, 116.4141999999999939 39.9621780000000015, 116.4141400000000033 39.9638089999999977, 116.4121870000000030 39.9637799999999999, 116.4121609999999976 39.9649429999999981, 116.4114239999999967 39.9649300000000025, 116.4110960000000006 39.9714540000000014, 116.4087860000000063 39.9714630000000000, 116.4088210000000032 39.9724959999999996, 116.4093409999999977 39.9725299999999990, 116.4093589999999949 39.9740359999999981, 116.4075100000000020 39.9740019999999987, 116.4075789999999984 39.9716530000000034, 116.4075449999999989 39.9710850000000022, 116.4073609999999945 39.9702040000000025, 116.4074829999999992 39.9676299999999998, 116.4077180000000027 39.9665620000000033, 116.4078719999999976 39.9621780000000015, 116.4054849999999988 39.9622549999999990, 116.4054780000000022 39.9618690000000001, 116.4041919999999948 39.9618910000000014, 116.4036710000000028 39.9618480000000034, 116.4026829999999961 39.9624329999999972, 116.4014480000000020 39.9624239999999986, 116.4013010000000037 39.9620820000000023, 116.4004500000000064 39.9620510000000024, 116.3997020000000049 39.9623879999999971, 116.3988520000000051 39.9623800000000031, 116.3987400000000036 39.9625619999999984, 116.3982270000000057 39.9625309999999985, 116.3969589999999954 39.9632039999999975, 116.3895059999999972 39.9631469999999993, 116.3894800000000060 39.9610380000000021, 116.3876640000000009 39.9609230000000011, 116.3873669999999976 39.9594859999999983, 116.3867759999999976 39.9570160000000030, 116.3933420000000041 39.9573490000000007, 116.3934430000000049 39.9504639999999966, 116.3939689999999985 39.9504799999999989, 116.3942030000000045 39.9442650000000015, 116.3942729999999983 39.9406320000000008, 116.3951490000000035 39.9405599999999978, 116.3961659999999938 39.9400600000000026, 116.3967030000000022 39.9283110000000008, 116.3988209999999981 39.9283590000000004, 116.3992310000000003 39.9281289999999984, 116.3994639999999947 39.9235680000000031, 116.3968229999999977 39.9234670000000023, 116.3968229999999977 39.9225289999999973, 116.3921840000000003 39.9224269999999990, 116.3925480000000050 39.9152079999999998, 116.3926349999999985 39.9129220000000018, 116.3920619999999957 39.9128919999999994, 116.3920959999999951 39.9124260000000035, 116.3919559999999933 39.9123999999999981, 116.3918790000000030 39.9120520000000027, 116.3918359999999979 39.9112319999999983, 116.3926780000000036 39.9112190000000027, 116.3926950000000033 39.9086679999999987, 116.3923730000000063 39.9082939999999979, 116.3922510000000017 39.9078820000000007, 116.3955629999999957 39.9079920000000001, 116.3958130000000040 39.9029049999999970, 116.3960890000000035 39.8994459999999975, 116.3963940000000008 39.8990510000000000, 116.3976020000000062 39.8986739999999998, 116.3976980000000054 39.8985789999999980, 116.3978709999999950 39.8960399999999993, 116.3981299999999948 39.8909840000000031, 116.3984339999999946 39.8863660000000024, 116.3985809999999930 39.8825369999999992, 116.3985980000000069 39.8810780000000022, 116.3984420000000028 39.8810969999999969, 116.3984679999999940 39.8802149999999997, 116.3987199999999973 39.8802149999999997, 116.3991000000000042 39.8722009999999969, 116.3925430000000034 39.8719120000000018, 116.3864350000000059 39.8715819999999965, 116.3848360000000071 39.8714810000000028, 116.3805870000000056 39.8711460000000031, 116.3806900000000013 39.8696139999999986, 116.3805160000000001 39.8693359999999970, 116.3805590000000052 39.8679419999999993, 116.3796909999999940 39.8679649999999981, 116.3796040000000005 39.8677899999999994, 116.3786199999999980 39.8676880000000011, 116.3787339999999944 39.8670249999999982, 116.3778559999999942 39.8669280000000015, 116.3771339999999981 39.8666940000000025, 116.3776639999999958 39.8666800000000023, 116.3785510000000016 39.8664339999999982, 116.3806809999999956 39.8660539999999983, 116.3816020000000009 39.8660169999999994, 116.3825669999999946 39.8660649999999990, 116.3833829999999949 39.8662309999999991, 116.3841910000000013 39.8665310000000019, 116.3852700000000056 39.8670660000000012, 116.3853399999999993 39.8667769999999990, 116.3878940000000028 39.8673760000000001, 116.3879110000000026 39.8665129999999976, 116.3878679999999974 39.8660429999999977, 116.3876790000000057 39.8651979999999995, 116.3910539999999969 39.8652670000000029, 116.3912030000000044 39.8652179999999987, 116.3912349999999947 39.8637400000000000, 116.3929729999999978 39.8637739999999994, 116.3929930000000041 39.8623070000000013, 116.3942789999999974 39.8622859999999974, 116.3942789999999974 39.8627329999999986, 116.3949559999999934 39.8627329999999986, 116.3949819999999988 39.8621210000000019, 116.3953640000000007 39.8614359999999976, 116.3954509999999942 39.8611059999999995, 116.3955040000000025 39.8586790000000022, 116.3995149999999938 39.8586250000000035, 116.3994720000000029 39.8597230000000025, 116.4036449999999974 39.8596930000000000, 116.4036859999999933 39.8596640000000022, 116.4068620000000038 39.8599650000000025, 116.4082240000000041 39.8589130000000011, 116.4106189999999970 39.8588740000000001, 116.4124510000000043 39.8589339999999979, 116.4124319999999955 39.8601069999999993, 116.4141439999999932 39.8601409999999987, 116.4139610000000005 39.8640350000000012, 116.4143689999999935 39.8640339999999966, 116.4144040000000047 39.8637090000000001, 116.4150810000000007 39.8636780000000002, 116.4151070000000061 39.8634670000000000, 116.4158189999999991 39.8635000000000019, 116.4158960000000036 39.8636440000000007, 116.4158269999999931 39.8657660000000007, 116.4166770000000071 39.8657400000000024, 116.4167549999999949 39.8668850000000035, 116.4156890000000004 39.8668509999999969, 116.4156799999999947 39.8670770000000019, 116.4152289999999965 39.8670630000000017, 116.4151930000000021 39.8686429999999987, 116.4150460000000038 39.8688349999999971, 116.4150460000000038 39.8693340000000020, 116.4158269999999931 39.8693600000000004, 116.4158280000000047 39.8698249999999987, 116.4136670000000038 39.8698260000000033, 116.4136579999999981 39.8711399999999969, 116.4169040000000024 39.8711869999999990, 116.4168690000000055 39.8714730000000017, 116.4192970000000003 39.8717499999999987, 116.4204699999999946 39.8719490000000008, 116.4214139999999986 39.8721749999999986, 116.4232099999999974 39.8728290000000030, 116.4251079999999945 39.8728260000000034, 116.4269469999999984 39.8726959999999977, 116.4352090000000004 39.8721760000000032, 116.4425729999999959 39.8718789999999998, 116.4429889999999972 39.8719170000000034, 116.4433630000000051 39.8721040000000002, 116.4435090000000059 39.8723120000000009, 116.4436400000000020 39.8728460000000027))'
    """
    taskname: str, path: str, sqlitename: str, ee_object: str, polygon: str, scale: int, region: str, start_date: str, end_date: str,
    datainfo_url: str, is_export_shp: bool, progress_info: dict, process_done, signal, queue, gpu_setting: bool
    """
    progress_info_dict = {'stitched_tiles': 0, 'target': 'stitch', 'st_time': 0, 'ed_time': 0, 'this_st_time': 0, 'stitch_list': [], 'crop_total': 1,
                          'croped_blocks': 0, 'is_stitch_complete': False, 'is_cropping_complete': False, 'update_sqlite_complete': False}
    gpu_setting = True
    args = ('Dongcheng_01', 'd:\\datadownload\\Dongcheng_01', 'Dongcheng_01.nev', 'Dynamic World', polygon, 10, '北京市', '2023-11-01', '2024-11-01',
            'https://127.0.0.1', True, progress_info_dict, {}, Event(), Queue(), gpu_setting, kernel_func)
    start_time = time.time()
    start(args)
    end_time = time.time()
    if gpu_setting:
        print('本次gpu拼接耗时：{}s'.format(str(end_time - start_time)))
    else:
        print('本次cpu拼接耗时：{}s'.format(str(end_time - start_time)))
    os.kill(os.getpid(), signal.SIGTERM)


# Beijing_01 tif裁剪完成
# tif写入结果：  True
# 本次gpu拼接耗时：47.08265161514282s


# Beijing_01 tif裁剪完成
# tif写入结果：  True
# 本次cpu拼接耗时：69.20752000808716s
