#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2024/9/23

import math
from pyproj import Transformer
from shapely import Point, LineString, Polygon, MultiPolygon
from shapely.wkt import dumps, loads


class CoordTransform:
    def __init__(self):
        self.wgs84_to_cgcs2000_transformer = Transformer.from_crs("EPSG:4326", "EPSG:4490", always_xy=True)
        self.cgcs2000_to_wgs84_transformer = Transformer.from_crs("EPSG:4490", "EPSG:4326", always_xy=True)
        self.x_pi = 3.14159265358979324 * 3000.0 / 180.0
        self.pi = 3.1415926535897932384626
        self.a = 6378245.0
        self.ee = 0.00669342162296594323

    def gcj02_to_bd09(self, lon, lat):
        z = math.sqrt(lon * lon + lat * lat) + 0.00002 * math.sin(lat * self.x_pi)
        theta = math.atan2(lat, lon) + 0.000003 * math.cos(lon * self.x_pi)
        bd_lon = z * math.cos(theta) + 0.0065
        bd_lat = z * math.sin(theta) + 0.006
        return [bd_lon, bd_lat]

    def bd09_to_gcj02(self, lon, lat):
        x = lon - 0.0065
        y = lat - 0.006
        z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * self.x_pi)
        theta = math.atan2(y, x) - 0.000003 * math.cos(x * self.x_pi)
        gcj_lon = z * math.cos(theta)
        gcj_lat = z * math.sin(theta)
        return [gcj_lon, gcj_lat]

    def wgs84_to_gcj02(self, lon, lat):
        if self.out_of_china(lon, lat):
            return [lon, lat]
        dlat = self._transformlat(lon - 105.0, lat - 35.0)
        dlng = self._transformlng(lon - 105.0, lat - 35.0)
        radlat = lat / 180.0 * self.pi
        magic = math.sin(radlat)
        magic = 1 - self.ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((self.a * (1 - self.ee)) / (magic * sqrtmagic) * self.pi)
        dlng = (dlng * 180.0) / (self.a / sqrtmagic * math.cos(radlat) * self.pi)
        mg_lat = lat + dlat
        mg_lon = lon + dlng
        return [mg_lon, mg_lat]

    def gcj02_to_wgs84(self, lng, lat):
        if self.out_of_china(lng, lat):
            return [lng, lat]
        dlat = self._transformlat(lng - 105.0, lat - 35.0)
        dlng = self._transformlng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * self.pi
        magic = math.sin(radlat)
        magic = 1 - self.ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((self.a * (1 - self.ee)) / (magic * sqrtmagic) * self.pi)
        dlng = (dlng * 180.0) / (self.a / sqrtmagic * math.cos(radlat) * self.pi)
        mglat = lat + dlat
        mglng = lng + dlng
        return [lng * 2 - mglng, lat * 2 - mglat]

    def bd09_to_wgs84(self, bd_lon, bd_lat):
        lon, lat = self.bd09_to_gcj02(bd_lon, bd_lat)
        return self.gcj02_to_wgs84(lon, lat)

    def wgs84_to_bd09(self, lon, lat):
        lon, lat = self.wgs84_to_gcj02(lon, lat)
        return self.gcj02_to_bd09(lon, lat)

    def _transformlat(self, lng, lat):
        ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
              0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * self.pi) + 20.0 *
                math.sin(2.0 * lng * self.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * self.pi) + 40.0 *
                math.sin(lat / 3.0 * self.pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * self.pi) + 320 *
                math.sin(lat * self.pi / 30.0)) * 2.0 / 3.0
        return ret

    def _transformlng(self, lng, lat):
        ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
              0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * self.pi) + 20.0 *
                math.sin(2.0 * lng * self.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * self.pi) + 40.0 *
                math.sin(lng / 3.0 * self.pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * self.pi) + 300.0 *
                math.sin(lng / 30.0 * self.pi)) * 2.0 / 3.0
        return ret

    def wgs84_to_cgcs2000(self, lon, lat):
        return self.wgs84_to_cgcs2000_transformer.transform(lon, lat)

    def cgcs2000_to_wgs84(self, lon, lat):
        return self.cgcs2000_to_wgs84_transformer.transform(lon, lat)

    def out_of_china(self, lng, lat):
        return not (73.66 < lng < 135.05 and 3.86 < lat < 53.55)

    def st_transform(self, geom: str, crs_from, crs_to):
        if geom is None:
            return None
        if crs_from not in ['bd09', 'wgs84', 'gcj02', 'cgcs2000'] or crs_to not in ['bd09', 'wgs84', 'gcj02', 'cgcs2000']:
            raise Exception('Unsupported coordinate system')
        if crs_from == crs_to:
            return geom
        geometry = loads(geom)
        if crs_to == 'cgcs2000' and crs_from != 'wgs84':
            wgs84_geom = self.st_transform(geom, crs_from, 'wgs84')
            return self.st_transform(wgs84_geom, 'wgs84', 'cgcs2000')
        elif crs_from == 'cgcs2000' and crs_to != 'wgs84':
            wgs84_geom = self.st_transform(geom, crs_from, 'wgs84')
            return self.st_transform(wgs84_geom, 'wgs84', crs_to)
        else:
            function_name = f'{crs_from}_to_{crs_to}'
        # function = globals().get(function_name)
        function = getattr(self, function_name)
        if isinstance(geometry, Point):
            # 点
            tranformed_geometry = Point(function(geometry.x, geometry.y))
        elif isinstance(geometry, LineString):
            # 线
            tranformed_geometry = LineString([function(x, y) for x, y in geometry.coords])
        elif isinstance(geometry, Polygon):
            # 面
            # 转换外部边界
            outer_coords = [function(x, y) for x, y in geometry.exterior.coords]
            # 转换内部孔
            inner_coords = [[function(x, y) for x, y in interior.coords] for interior in geometry.interiors]
            tranformed_geometry = Polygon(outer_coords, inner_coords)
        elif isinstance(geometry, MultiPolygon):
            # 多面
            tranformed_polygons = [
                Polygon(
                    [function(x, y) for x, y in polygon.exterior.coords],
                    [[function(x, y) for x, y in interior.coords] for interior in polygon.interiors]
                ) for polygon in geometry.geoms
            ]
            tranformed_geometry = MultiPolygon(tranformed_polygons)
        else:
            raise ValueError("Unsupported geometry type")
        return dumps(tranformed_geometry)


