from __future__ import print_function

import os
from datetime import datetime

import numpy as np
from osgeo import gdal, gdalconst
from osgeo import osr

from util.calibrator.geos_utils import geos_area_from_pixel_area, pixel_area_from_geos_area

#IMPORTANT        8-bit  8-bit       16-bit         16-bit      8-bit
CHANNEL_NAMES = ["CMa", "CMa_DUST", "CMa_QUALITY", "CMa_TEST", "CMa_VOLCANIC"]


def get_claas_filename(date_time, channel=None, prefix=None):
    filename = prefix + "CFCin" + datetime.strftime(date_time, "%Y%m%d%H%M")
    if date_time < datetime(2007, 4, 12):
        filename += "002050016001MA.hdf"
    else:
        filename += "002050023201MA.hdf"

    if channel is not None:
        filename = "HDF5:\"" + filename + "\"://" + channel
    return filename

gdal.UseExceptions()


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class AreaError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expr -- input expression in which the error occurred
        msg  -- explanation of the error
    """

    def __init__(self, pixel_area, geos_area):
        self.pixel_area = pixel_area
        self.geos_area = geos_area


class GdalError(Error):
    def __init__(self, error):
        self.error = error


class GdalClaasLoader:
    def __init__(self, base_path='.', prefixes=[""]):
        self.base_path = base_path
        self.prefixes = prefixes

    def load_scene(self, date_time, channel_names=["CMa", "CMa_DUST", "CMa_VOLCANIC"], pixel_area=(0, 0, 3712, 3712), geos_area=None, data_type=np.int16):

        channel_name_list = []

        x_min, y_min, x_max, y_max = (None, None, None, None)
        if geos_area is None and pixel_area is not None:
            x_min, y_min, x_max, y_max = pixel_area
            geos_area = geos_area_from_pixel_area(pixel_area)

        if geos_area is not None:
            x_min, y_min, x_max, y_max = pixel_area_from_geos_area(geos_area)
            pixel_area = (x_min, y_min, x_max, y_max)

        if x_min is None:
            raise AreaError(pixel_area, geos_area)

        x_off = np.floor(x_min)
        y_off = np.floor(y_min)
        x_size = np.floor(x_max) - np.floor(x_min)
        y_size = np.floor(y_max) - np.floor(y_min)

        #print(x_off, y_off, x_size, y_size)

        srs = osr.SpatialReference()
        srs.ImportFromProj4("+proj=geos +a=6378169.0 +b=6356583.8 +lon_0=0.0 +h=35785831.0")  # NC_GLOBAL#PROJECTION=+proj=geos +a=6378169.0 +b=6356583.8 +lon_0=0.0 +h=35785831.0
        wkt = srs.ExportToWkt()
        geotransform = [-5570248.832537, 3000.403357, 0.000000, 5570248.832537, 0.000000,
                        -3000.403357]  # GEOTRANSFORM_GDAL_TABLE=-5570248.832537, 3000.403357, 0.000000, 5570248.832537,0.000000, -3000.403357

        top_left_x, we_pixel_resolution, _, top_left_y, _, ns_pixel_resolution = geotransform
        cropped_geotransform = (top_left_x + x_off * we_pixel_resolution, we_pixel_resolution, 0.0,
                                top_left_y + y_off * ns_pixel_resolution, 0.0, ns_pixel_resolution)

        path_with_prefix = None
        for pre in self.prefixes:
            new_path_with_prefix = self.base_path + "/" + datetime.strftime(date_time, pre)
            if os.path.isdir(new_path_with_prefix):
                path_with_prefix = new_path_with_prefix
                break

        if path_with_prefix is None:
            return None

        claas_filename = path_with_prefix + get_claas_filename(date_time, prefix=pre)

        if not os.path.exists(claas_filename):
            return None

        # load global metadata:
        #filename = path_with_prefix + "/" + get_claas_filename(date_time, channel=None, prefix=pre)
        #dataset = gdal.Open(filename, gdalconst.GA_ReadOnly)
        #proj4s_string = dataset.GetMetadata("PROJECTION", domain="NC_GLOBAL")

        for channel_number, channel_name in enumerate(channel_names):
            filename ='HDF5:"' + claas_filename + '"://' + channel_name

            try:
                dataset = gdal.Open(filename, gdalconst.GA_ReadOnly) #get the raster band. It's always the first one...
                band = dataset.GetRasterBand(1)
                metadata = band.GetMetadata()

                data = band.ReadAsArray(xoff= np.int32(x_off), yoff=np.int32(y_off), win_xsize=np.int32(x_size), win_ysize=np.int32(y_size)).astype(data_type)

                channel_name_list.append((channel_name, ClaasChannel(channel_name, data, cropped_geotransform, metadata)))
                del band
                del dataset

            except RuntimeError as e:
                print(e)
                #raise GdalError(e)

        #print(channel_name_list)
        if len(channel_name_list) <= 0:
            return None

        return ClaasScene(channel_name_list, geos_area, pixel_area, date_time, wkt, cropped_geotransform)


class ClaasScene:
    def __init__(self, name_channels, geos_area, pixel_area, date, wkt, geotransform, metadata=None):
        self.channels = dict(name_channels)
        self.geos_area = geos_area
        self.pixel_area = pixel_area
        self.date = date
        self.wkt = wkt
        self.geotransform = geotransform
        self.metadata = metadata

    def __str__(self):
        return "Scene: %s - %s > %s" % (self.geos_area, self.date, self.channels.keys())

    def __getitem__(self, index):
        return self.channels.get(index)

    def __setitem__(self, key, value):
        self.channels[key] = value


class ClaasChannel:
    def __init__(self, name, data, geotransform, metadata=None, no_data_value=None):
        self.name = name
        self.data = data
        self.geotransform = geotransform
        self.metadata = metadata
        self.no_data_value = no_data_value

    def __str__(self):
        return "%s: %s $%s$ $%s$ $%s$" % (self.name, self.data.shape, self.geotransform, self.metadata, self.satellite)

#loader = GdalClaasLoader('/media/agdbs/DATA_EXTERN/geodata/raster/claas_v2_cma/ORD19113_194/')
#scene = loader.load_scene(datetime(2012, 11, 8, 13, 30), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 13, 45), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 14, 00), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 14, 15), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 14, 30), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 14, 45), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 15, 00), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 15, 15), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 15, 30), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 15, 45), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 16, 00), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 16, 15), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 16, 30), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 16, 45), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 17, 00), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 17, 15), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 17, 30), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 17, 45), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 18, 00), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 18, 15), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#scene = loader.load_scene(datetime(2012, 11, 8, 18, 45), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#print(scene)
