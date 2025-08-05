#!/usr/bin/python3
# Author: B_Snowflake
# Date: 2024/11/19

import os
import traceback
from typing import Optional
import folium
import geemap.maplibregl as geemaplibregl
import geemap.foliumap as geemapfolium
import xyzservices
from geemap import arc_add_layer
from shapely import MultiPolygon, Polygon
from shapely.wkt import loads
import ee


class GeeMapHtml:
    def __init__(self):
        self.ee_geom = None
        self.customlandcover = CustomLandcover()
        self.Dynamic_World_Legend_Dict = {
            "0 水体": "419BDF",
            "1 林地": "397D49",
            "2 草地": "88B053",
            "3 果蔬": "7A87C6",
            "4 庄稼": "E49635",
            "5 灌木和灌丛": "DFC35A",
            "6 建筑物": "C4281B",
            "7 裸地": "A59B8F",
            "8 冰雪": "B39FE1",
        }
        self.JRC_Monthly_Water_History_Legend_Dict = {}
        os.environ["MAPTILER_KEY"] = 'd6Pq7m9y8IWXNK0271ZC'

    def export_html(self, ee_object, savepath, datasetname, basemap, region, start_date, end_date, scale, opacity, legend=True, map_style='2d', min_zoom=10,
                    bands=None, save_html=True):
        try:
            self.ee_object = ee_object
            self.savepath = savepath
            self.region = region
            self.geometry = loads(region)
            self.centroid = self.geometry.centroid
            self.start_date = start_date
            self.basemap = basemap
            self.end_date = end_date
            self.legend = legend
            self.scale = scale
            self.get_region_bounds()
            self.wkt_to_eegeometry()
            if map_style == '2d':
                m = RewriteAddBaseMapGeeMap(plugin_Fullscreen=True, plugin_Draw=True, search_control=True, zoom_control=True)
                m.fit_bounds(bounds=self.bounds)
            else:
                m = geemaplibregl.Map(center=(self.centroid.x, self.centroid.y), zoom=8, pitch=60, bearing=30, style="3d-terrain",
                                      controls={"scale": "bottom-left"})
            if self.basemap == '影像底图' and map_style == '2d':
                google_satellite_provider = xyzservices.TileProvider({
                    'url': 'https://www.google.com/maps/vt?lyrs=y@815&hl=zh-CN&x={x}&y={y}&z={z}',
                    'attribution': '(C) xyzservices',
                    'name': 'Google Satellite Provider',
                })
                m.add_basemap(google_satellite_provider)
            elif self.basemap == '矢量底图':
                google_vector_provider = xyzservices.TileProvider({
                    'url': 'https://www.google.com/maps/vt?lyrm=s@815&hl=zh-CN&x={x}&y={y}&z={z}',
                    'attribution': '(C) xyzservices',
                    'name': 'Google Vector Provider',
                })
                m.add_basemap(google_vector_provider)
            elif self.basemap == '地形底图':
                google_vector_provider = xyzservices.TileProvider({
                    'url': 'https://www.google.com/maps/vt?lyrs=p@815&hl=zh-CN&x={x}&y={y}&z={z}',
                    'attribution': '(C) xyzservices',
                    'name': 'Google Vector Provider',
                })
                m.add_basemap(google_vector_provider)
            if self.ee_object == 'Dynamic World':
                layer = self.customlandcover.dynamic_world(region=self.ee_geom, start_date=self.start_date, end_date=self.end_date, scale=self.scale,
                                                           clip=True)
                if self.legend:
                    m.add_legend(title='土地覆盖', legend_dict=self.Dynamic_World_Legend_Dict)
            elif self.ee_object == 'JRC Monthly Water History':
                layer = self.customlandcover.jrc_monthly_water_history(region=self.ee_geom, start_date=self.start_date, end_date=self.end_date,
                                                                       scale=self.scale, clip=True)
            elif self.ee_object == 'Global Multi-resolution Terrain':
                layer = self.customlandcover.global_multi_resolution_terrain(region=self.ee_geom)
            elif self.ee_object == 'CFSV2':
                if not isinstance(bands, tuple):
                    layer = self.customlandcover.CFSV2(region=self.ee_geom, start_date=self.start_date, end_date=self.end_date, band=bands, scale=self.scale,
                                                       clip=True)
                else:
                    layer = self.customlandcover.CFSV2(region=self.ee_geom, start_date=self.start_date, end_date=self.end_date, band=bands[0], scale=self.scale,
                                                       clip=True)
            if map_style == '2d':
                m.add_layer(ee_object=layer, vis_params={}, name='2D layer', opacity=1)
            elif map_style == '3d':
                m.add_ee_layer(ee_object=layer, vis_params={}, name='3D layer', opacity=opacity)
                m.add_3d_buildings(min_zoom=min_zoom)
            if save_html and map_style == '2d':
                m.save(os.path.join(self.savepath, f"{datasetname}_2d.html"))
            elif save_html and map_style == '3d':
                m.to_html(output=os.path.join(self.savepath, f"{datasetname}_3d.html"), replace_key=True, overwrite=True)
            return m
        except Exception as e:
            traceback.print_exc()
            raise Exception(e)

    def get_region_bounds(self):
        shapely_bounds = self.geometry.bounds
        self.bounds = [[shapely_bounds[1], shapely_bounds[0]], [shapely_bounds[3], shapely_bounds[2]]]

    def wkt_to_eegeometry(self):
        if isinstance(self.geometry, Polygon):
            coords = [list(self.geometry.exterior.coords)]
            self.ee_geom = ee.Geometry.Polygon(coords)
        elif isinstance(self.geometry, MultiPolygon):
            coords = [list(poly.exterior.coords) for poly in self.geometry.geoms]
            self.ee_geom = ee.Geometry.MultiPolygon(coords)
        else:
            raise ValueError("Geometry type not supported")


class CustomLandcover:
    def __init__(self):
        pass

    @staticmethod
    def CFSV2(region=None, start_date='2021-01-01', end_date='2022-01-01', band='Temperature_height_above_ground', scale=10, clip=True, reducer=None,
              projection="EPSG:3857"):
        visparams_dict = {
            'Downward_Long-Wave_Radp_Flux_surface_6_Hour_Average': {  # 地表向下长波辐射通量，6 小时平均值
                'min': 41.76,
                'max': 532.67,
                'palette': ['#000080', '#4169E1', '#00BFFF', '#7CFC00', '#FFD700', '#FF4500', '#8B0000']
            },
            'Downward_Short-Wave_Radiation_Flux_surface_6_Hour_Average': {  # 地表向下短波辐射通量，6 小时平均值
                'min': 0,
                'max': 1125.23,
                'palette': ['#000080', '#4169E1', '#00BFFF', '#7CFC00', '#FFD700', '#FF4500', '#8B0000']
            },
            'Latent_heat_net_flux_surface_6_Hour_Average': {  # 地表潜热净通量，6 小时平均值
                'min': -628,
                'max': 2357,
                'palette': ['#000080', '#4169E1', '#00BFFF', '#7CFC00', '#FFD700', '#FF4500', '#8B0000']
            },
            'Potential_Evaporation_Rate_surface_6_Hour_Average': {  # 地表潜在蒸发速率（平均 6 小时）
                'min': -202,
                'max': 6277,
                'palette': ['#000080', '#4169E1', '#00BFFF', '#7CFC00', '#FFD700', '#FF4500', '#8B0000']
            },
            'Sensible_heat_net_flux_surface_6_Hour_Average': {  # 地表的感热净通量，6 小时平均值
                'min': -2295,
                'max': 3112,
                'palette': ['#000080', '#4169E1', '#00BFFF', '#7CFC00', '#FFD700', '#FF4500', '#8B0000']
            },
            'Upward_Long-Wave_Radp_Flux_surface_6_Hour_Average': {  # 地表向上的长波辐射通量，6 小时平均值
                'min': 59,
                'max': 757,
                'palette': ['#000080', '#4169E1', '#00BFFF', '#7CFC00', '#FFD700', '#FF4500', '#8B0000']
            },
            'Geopotential_height_surface': {  # 地表处的地表位势高度
                'min': -292,
                'max': 5938.65,
                'palette': ['#1a0b73', '#0057b8', '#00b4d8', '#76c893', '#f4d35e', '#ff6b35', '#d90429', '#ffffff']
            },
            'Upward_Short-Wave_Radiation_Flux_surface_6_Hour_Average': {  # 地表向上的短波辐射通量，6 小时平均值
                'min': 0,
                'max': 812,
                'palette': ['#000080', '#4169E1', '#00BFFF', '#7CFC00', '#FFD700', '#FF4500', '#8B0000']
            },
            'Pressure_surface': {  # 表面压力
                'min': 47200,
                'max': 109180,
                'palette': ['#0d0887', '#3f00c8', '#6a00f5', '#8f29ff', '#b14fff', '#d171ff', '#ffb0d9', '#ff8080', '#ff4d4d', '#ff0000']
            },
            'Maximum_specific_humidity_at_2m_height_above_ground_6_Hour_Interval': {  # 地面 2 米处的最大比湿度，间隔 6 小时
                'min': 0,
                'max': 0.1,
                'palette': ['#ffffff', '#e0f3f8', '#abd9e9', '#74add1', '#4575b4', '#fef0d9', '#fdae61', '#f46d43', '#d73027', '#940000']
            },
            'Minimum_specific_humidity_at_2m_height_above_ground_6_Hour_Interval': {  # 地面 2 米处的最小比湿度，间隔 6 小时
                'min': 0,
                'max': 0.02,
                'palette': ['#ffffff', '#e0f3f8', '#abd9e9', '#74add1', '#4575b4', '#fef0d9', '#fdae61', '#f46d43', '#d73027', '#940000']
            },
            'Specific_humidity_height_above_ground': {  # 离地面 2 米处的特定湿度
                'min': 0,
                'max': 0.06,
                'palette': ['#ffffff', '#e0f3f8', '#abd9e9', '#74add1', '#4575b4', '#fef0d9', '#fdae61', '#f46d43', '#d73027', '#940000']
            },
            'Precipitation_rate_surface_6_Hour_Average': {  # 地表降水率（6 小时平均值）
                'min': 0,
                'max': 0.03,
                'palette': ['#f0f9ff', '#c6e3f7', '#81c2ff', '#3d8bfd', '#0d6efd', '#6f42c1', '#d63384', '#dc3545', '#ff8c00', '#fd7e14']
            },
            'u-component_of_wind_height_above_ground': {  # 离地面 10 米处的风速 U 分量
                'min': -57.2,
                'max': 57.99,
                'palette': ['#0d47a1', '#1976d2', '#42a5f5', '#90caf9', '#e3f2fd', '#ffffff', '#ffebee', '#ff8a80', '#ff5252', '#d50000', '#8b0000']
            },
            'v-component_of_wind_height_above_ground': {  # 离地面 10 米处的风速 V 分量
                'min': -53.09,
                'max': 57.11,
                'palette': ['#0d47a1', '#1976d2', '#42a5f5', '#90caf9', '#e3f2fd', '#ffffff', '#ffebee', '#ff8a80', '#ff5252', '#d50000', '#8b0000']
            },
            'Volumetric_Soil_Moisture_Content_depth_below_surface_layer_5_cm': {  # 地表下 5 厘米处的土壤体积含水量
                'min': 0.02,
                'max': 1,
                'palette': ['#8b0000', '#ff4500', '#ffd700', '#adff2f', '#32cd32', '#006400', '#87ceeb', '#4169e1', '#00008b']
            },
            'Volumetric_Soil_Moisture_Content_depth_below_surface_layer_25_cm': {  # 地表下 25 厘米处的土壤体积含水量
                'min': 0.02,
                'max': 1,
                'palette': ['#8b0000', '#ff4500', '#ffd700', '#adff2f', '#32cd32', '#006400', '#87ceeb', '#4169e1', '#00008b']
            },
            'Volumetric_Soil_Moisture_Content_depth_below_surface_layer_70_cm': {  # 地表下 70 厘米处的土壤体积含水量
                'min': 0.02,
                'max': 1,
                'palette': ['#8b0000', '#ff4500', '#ffd700', '#adff2f', '#32cd32', '#006400', '#87ceeb', '#4169e1', '#00008b']
            },
            'Volumetric_Soil_Moisture_Content_depth_below_surface_layer_150_cm': {  # 地表下 150 厘米处的土壤体积含水量
                'min': 0.02,
                'max': 1,
                'palette': ['#8b0000', '#ff4500', '#ffd700', '#adff2f', '#32cd32', '#006400', '#87ceeb', '#4169e1', '#00008b']
            },
            'Maximum_temperature_height_above_ground_6_Hour_Interval': {  # 地面 2 米处的最高温度，间隔 6 小时
                'min': 189.8,
                'max': 334.89,
                'palette': ['blue', 'purple', 'cyan', 'green', 'yellow', 'red']
            },
            'Minimum_temperature_height_above_ground_6_Hour_Interval': {  # 地面 2 米处的最低温度，间隔 6 小时
                'min': 188.39,
                'max': 324.39,
                'palette': ['blue', 'purple', 'cyan', 'green', 'yellow', 'red']
            },
            'Temperature_height_above_ground': {  # 离地面 2 米处的温度
                'min': 188.96,
                'max': 328.68,
                'palette': ['blue', 'purple', 'cyan', 'green', 'yellow', 'red']
            }
        }
        dataset = ee.ImageCollection("NOAA/CFSV2/FOR6H").filter(ee.Filter.date(start_date, end_date)).select(band)
        if band is None:
            return
        visparams = visparams_dict.get(band)
        if reducer is None:
            reducer = ee.Reducer.mode()
        dataset = dataset.reduce(reducer)
        if clip and (region is not None):
            if isinstance(region, ee.Geometry):
                dataset = dataset.clip(region)
            elif isinstance(region, ee.FeatureCollection):
                dataset = dataset.clipToCollection(region)
            elif isinstance(region, ee.Feature):
                dataset = dataset.clip(region.geometry())
        proj = ee.Projection(projection).atScale(scale)
        dataset = dataset.setDefaultProjection(proj)
        return dataset.visualize(**visparams)

    @staticmethod
    def global_multi_resolution_terrain(region=None, reducer=None, clip=True, projection="EPSG:3857"):
        myterrain = ee.Image("USGS/GMTED2010_FULL").select('mea')
        if reducer is None:
            reducer = ee.Reducer.mode()
        myterrain = myterrain.reduce(reducer)
        if clip and (region is not None):
            if isinstance(region, ee.Geometry):
                myterrain = myterrain.clip(region)
            elif isinstance(region, ee.FeatureCollection):
                myterrain = myterrain.clipToCollection(region)
            elif isinstance(region, ee.Feature):
                myterrain = myterrain.clip(region.geometry())
        exaggeration = 20
        myterrain = ee.Terrain.hillshade(myterrain.multiply(exaggeration), 270, 45)
        proj = ee.Projection(projection)
        myterrain = myterrain.setDefaultProjection(proj)
        return myterrain

    @staticmethod
    def jrc_monthly_water_history(region=None, start_date='2021-01-01', end_date='2022-01-01', scale=10, clip=True, reducer=None, projection="EPSG:3857"):
        if reducer is None:
            reducer = ee.Reducer.mode()
        myjrc = ee.ImageCollection("JRC/GSW1_4/MonthlyHistory").filter(
            ee.Filter.date(start_date, end_date)
        )
        if region is not None:
            if isinstance(region, ee.FeatureCollection) or isinstance(region, ee.Geometry):
                myjrc = myjrc.filterBounds(region)
            else:
                raise ValueError("region must be an ee.FeatureCollection or ee.Geometry.")

        def add_obs_band(img):
            obs = img.gt(0)
            return img.addBands(obs.rename('obs').set('system:time_start', img.get('system:time_start')))

        myjrc = myjrc.map(add_obs_band)

        def add_onlywater_band(img):
            water = img.select('water').eq(2)
            return img.addBands(water.rename('onlywater').set('system:time_start', img.get('system:time_start')))

        myjrc = myjrc.map(add_onlywater_band)
        # 计算每个像素点在一年12景影像中，有数据的次数
        totalObs = myjrc.select('obs').sum().toFloat()

        # 计算每个像素点在一年12景影像中，有水的次数
        totalWater = myjrc.select('onlywater').sum().toFloat()
        # 统计每个像素点在一年中有水的比例
        floodfreq = totalWater.divide(totalObs).multiply(100)
        # 删除没有值的像素
        myMask = floodfreq.eq(0).Not()
        floodfreq = floodfreq.updateMask(myMask)
        proj = ee.Projection(projection).atScale(scale)
        floodfreq = floodfreq.setDefaultProjection(proj)
        dwVisParams = {
            "min": 0,
            "max": 50,
            "palette": [
                'ffffff',
                'fffcb8',
                '0905ff'
            ],
        }
        # 裁剪到指定区域
        if clip and (region is not None):
            if isinstance(region, ee.Geometry):
                floodfreq = floodfreq.clip(region)
            elif isinstance(region, ee.FeatureCollection):
                floodfreq = floodfreq.clipToCollection(region)
            elif isinstance(region, ee.Feature):
                floodfreq = floodfreq.clip(region.geometry())

        return floodfreq.visualize(**dwVisParams)

    @staticmethod
    def dynamic_world(
            region=None,
            start_date="2020-01-01",
            end_date="2021-01-01",
            clip=False,
            reducer=None,
            projection="EPSG:3857",
            scale=10,
            return_type="hillshade",
    ):
        """Create 10-m land cover composite based on Dynamic World. The source code is adapted from the following tutorial by Spatial Thoughts:
        https://developers.google.com/earth-engine/tutorials/community/introduction-to-dynamic-world-pt-1

        Args:
            region (ee.Geometry | ee.FeatureCollection): The region of interest.
            start_date (str | ee.Date): The start date of the query. Default to "2020-01-01".
            end_date (str | ee.Date): The end date of the query. Default to "2021-01-01".
            clip (bool, optional): Whether to clip the image to the region. Default to False.
            reducer (ee.Reducer, optional): The reducer to be used. Default to None.
            projection (str, optional): The projection to be used for creating hillshade. Default to "EPSG:3857".
            scale (int, optional): The scale to be used for creating hillshade. Default to 10.
            return_type (str, optional): The type of image to be returned. Can be one of 'hillshade', 'visualize', 'class', or 'probability'. Default to "hillshade".

        Returns:
            ee.Image: The image with the specified return_type.
        """

        if return_type not in ["hillshade", "visualize", "class", "probability"]:
            raise ValueError(
                f"{return_type} must be one of 'hillshade', 'visualize', 'class', or 'probability'."
            )

        if reducer is None:
            reducer = ee.Reducer.mode()

        dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1").filter(
            ee.Filter.date(start_date, end_date)
        )

        if region is not None:
            if isinstance(region, ee.FeatureCollection) or isinstance(region, ee.Geometry):
                dw = dw.filterBounds(region)
            else:
                raise ValueError("region must be an ee.FeatureCollection or ee.Geometry.")

        # Create a Mode Composite
        classification = dw.select("label")
        dwComposite = classification.reduce(reducer)
        if clip and (region is not None):
            if isinstance(region, ee.Geometry):
                dwComposite = dwComposite.clip(region)
            elif isinstance(region, ee.FeatureCollection):
                dwComposite = dwComposite.clipToCollection(region)
            elif isinstance(region, ee.Feature):
                dwComposite = dwComposite.clip(region.geometry())

        dwVisParams = {
            "min": 0,
            "max": 8,
            "palette": [
                "#419BDF",
                "#397D49",
                "#88B053",
                "#7A87C6",
                "#E49635",
                "#DFC35A",
                "#C4281B",
                "#A59B8F",
                "#B39FE1",
            ],
        }

        if return_type == "class":
            return dwComposite
        elif return_type == "visualize":
            return dwComposite.visualize(**dwVisParams)
        else:
            # Create a Top-1 Probability Hillshade Visualization
            probabilityBands = [
                "water",
                "trees",
                "grass",
                "flooded_vegetation",
                "crops",
                "shrub_and_scrub",
                "built",
                "bare",
                "snow_and_ice",
            ]

            # Select probability bands
            probabilityCol = dw.select(probabilityBands)

            # Create a multi-band image with the average pixel-wise probability
            # for each band across the time-period
            meanProbability = probabilityCol.reduce(ee.Reducer.mean())

            # Composites have a default projection that is not suitable
            # for hillshade computation.
            # Set a EPSG:3857 projection with 10m scale
            proj = ee.Projection(projection).atScale(scale)
            meanProbability = meanProbability.setDefaultProjection(proj)

            # Create the Top1 Probability Hillshade
            top1Probability = meanProbability.reduce(ee.Reducer.max())

            if clip and (region is not None):
                if isinstance(region, ee.Geometry):
                    top1Probability = top1Probability.clip(region)
                elif isinstance(region, ee.FeatureCollection):
                    top1Probability = top1Probability.clipToCollection(region)
                elif isinstance(region, ee.Feature):
                    top1Probability = top1Probability.clip(region.geometry())

            if return_type == "probability":
                return top1Probability
            else:
                top1Confidence = top1Probability.multiply(100).int()
                hillshade = ee.Terrain.hillshade(top1Confidence).divide(255)
                rgbImage = dwComposite.visualize(**dwVisParams).divide(255)
                probabilityHillshade = rgbImage.multiply(hillshade)

                return probabilityHillshade


class RewriteAddBaseMapGeeMap(geemapfolium.Map):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_basemap(
            self, basemap: Optional[str | xyzservices.TileProvider] = "HYBRID", show: Optional[bool] = True, **kwargs
    ):
        try:
            map_dict = {
                "ROADMAP": "Esri.WorldStreetMap",
                "SATELLITE": "Esri.WorldImagery",
                "TERRAIN": "Esri.WorldTopoMap",
                "HYBRID": "Esri.WorldImagery",
            }
            if isinstance(basemap, str):
                if basemap.upper() in map_dict:
                    if basemap in os.environ:
                        if "name" in kwargs:
                            kwargs["name"] = basemap
                        basemap = os.environ[basemap]
                        self.add_tile_layer(tiles=basemap, **kwargs)
                    else:
                        basemap = basemap.upper()
                        geemapfolium.basemaps[basemap].add_to(self)
            elif isinstance(basemap, xyzservices.TileProvider):
                name = basemap.name
                url = basemap.build_url()
                attribution = basemap.attribution
                if "max_zoom" in basemap.keys():
                    max_zoom = basemap["max_zoom"]
                else:
                    max_zoom = 22
                layer = folium.TileLayer(
                    tiles=url,
                    attr=attribution,
                    name=name,
                    max_zoom=max_zoom,
                    overlay=True,
                    control=True,
                    show=show,
                    **kwargs,
                )
                layer.add_to(self)
                arc_add_layer(url, name)
            elif basemap in geemapfolium.basemaps:
                bmap = geemapfolium.basemaps[basemap]
                bmap.show = show
                bmap.add_to(self)
                if isinstance(geemapfolium.basemaps[basemap], folium.TileLayer):
                    url = geemapfolium.basemaps[basemap].tiles
                elif isinstance(geemapfolium.basemaps[basemap], folium.WmsTileLayer):
                    url = geemapfolium.basemaps[basemap].url
                arc_add_layer(url, basemap)
            else:
                print(
                    "Basemap can only be one of the following: {}".format(
                        ", ".join(geemapfolium.basemaps.keys())
                    )
                )
        except Exception as e:
            raise Exception(
                "Basemap can only be one of the following: {}".format(
                    ", ".join(geemapfolium.basemaps.keys())
                )
            )


if __name__ == '__main__':
    os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:10809'
    ee.Initialize(project='ee-test-objective')
    # 定义区域和时间范围
    ts_region = ee.Geometry.Rectangle([100, 30, 105, 35])  # 示例区域
    ts_start_date = '2021-01-01'
    ts_end_date = '2022-01-01'
    a = CustomLandcover()
    # 获取结果影像
    result_image = a.global_multi_resolution_terrain(ts_region)
    # 打印结果影像
    print(result_image.getInfo())
