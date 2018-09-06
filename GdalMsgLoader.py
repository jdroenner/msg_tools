from __future__ import print_function

import numpy as np
import os
from datetime import datetime

from osgeo import gdal, gdalconst

from geos_utils import geos_area_from_pixel_area, pixel_area_from_geos_area
from msg import CHANNEL_NAMES, MSG_SATELLITES, get_xrit_filename
from MsgScene import MsgScene, MsgChannel

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


class GdalMsgLoader:
    def __init__(self, base_path='.', prefixes=["%Y/%m/%d/%Y%m%d_%H%M/"], simpleProfiler = None):
        self.base_path = base_path
        self.prefixes = prefixes
        self.simpleProfiler = simpleProfiler

    def load_scene(self, date_time, channel_names=CHANNEL_NAMES, pixel_area=(0, 0, 3712, 3712), geos_area=None, overwrite_dataset_geos_area = False):

        if(self.simpleProfiler is not None):
            self.simpleProfiler.start("GdalMsgLoader_load_scene")

        channel_name_list = []

        x_min, y_min, x_max, y_max = (None, None, None, None)
        if pixel_area is not None:
            x_min, y_min, x_max, y_max = pixel_area

        if geos_area is None and pixel_area is not None:
            geos_area = geos_area_from_pixel_area(pixel_area)

        if geos_area is not None and pixel_area is None:
            x_min, y_min, x_max, y_max = pixel_area_from_geos_area(geos_area)
            pixel_area = (np.int(x_min), np.int(y_min), np.int(x_max), np.int(y_max))

        if x_min is None:
            raise AreaError(pixel_area, geos_area)

        x_off = np.int(np.floor(x_min))
        y_off = np.int(np.floor(y_min))
        x_size = np.int(np.absolute(np.floor(x_max) - np.floor(x_min)))
        y_size = np.int(np.absolute(np.floor(y_max) - np.floor(y_min)))

        #print(x_off, y_off, x_size, y_size)

        wkt = None
        satellite = None
        cropped_geotransform = None

        path_with_prefix = None
        for pre in self.prefixes:
            new_path_with_prefix = self.base_path + "/" + datetime.strftime(date_time, pre)
            if os.path.isdir(new_path_with_prefix):
                path_with_prefix = new_path_with_prefix
                break

        if path_with_prefix is None:
            return None


        for channel_number, channel_name in enumerate(channel_names):
            filename = path_with_prefix + "/" + get_xrit_filename(date_time, channel_name)

            try:
                dataset = gdal.Open(filename, gdalconst.GA_ReadOnly)

                # get dataset information from the first channel only
                if channel_number == 0:
                #    print('Driver: ', dataset.GetDriver().ShortName, '/', dataset.GetDriver().LongName)
                #    print('Size is ', dataset.RasterXSize, 'x', dataset.RasterYSize, 'x', dataset.RasterCount)
                #    print('Projection is ', dataset.GetProjection())
                #    print('GeoTransform', dataset.GetGeoTransform())
                    satellite_number = dataset.GetMetadata("msg")['satellite_number']
                    if satellite_number is not None:
                        satellite = MSG_SATELLITES[int(satellite_number)]


                    wkt = dataset.GetProjectionRef()

                    geotransform = dataset.GetGeoTransform()
                    cropped_geotransform = geotransform

                    if geotransform is not None:
                        top_left_x, we_pixel_resolution, _, top_left_y, _, ns_pixel_resolution = geotransform
                        cropped_geotransform = (top_left_x + (x_off * we_pixel_resolution), we_pixel_resolution, 0.0,
                                                top_left_y + (y_off * ns_pixel_resolution), 0.0, ns_pixel_resolution)

                    if overwrite_dataset_geos_area is True :
                        print ("Overwriting data geotransform", cropped_geotransform)
                        cropped_geotransform = (geos_area[0], we_pixel_resolution, 0.0,
                                                geos_area[1], 0.0, ns_pixel_resolution)
                        print("Overwriting with geotransform", cropped_geotransform)

                # get the raster band. It's always the first one... (MSG/HRIT)
                band = dataset.GetRasterBand(1)
                #print("rasterband")
                raw_metadata = band.GetMetadata("msg")
                metadata = {
                    'calibration_offset': float(raw_metadata['calibration_offset']),
                    'calibration_slope': float(raw_metadata['calibration_slope']),
                    'channel_number': int(raw_metadata['channel_number']),
                    'date_time': date_time
                }
                #print("metadata")

                data = band.ReadAsArray(xoff=x_off, yoff=y_off, win_xsize=x_size, win_ysize=y_size).astype(np.int16)  # TODO read only the needed area!
                #print("data")
                channel_name_list.append((channel_name, MsgChannel(channel_name, data, cropped_geotransform, metadata, satellite, no_data_value=0)))
                del band
                del dataset

            except RuntimeError as e:
                print(e)
                #raise GdalError(e)

        #print(channel_name_list)
        if len(channel_name_list) <= 0:
            return None

        if(self.simpleProfiler is not None):
            self.simpleProfiler.stop("GdalMsgLoader_load_scene")

        return MsgScene(channel_name_list, date_time, wkt, cropped_geotransform, geos_area, pixel_area)

#msg_loader = GdalMsgLoader('/media/agdbs/6TB_#1')
#msg_scene = msg_loader.load_scene(datetime(2012, 1, 1, 1, 0), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#print(msg_scene["VIS006"])

#print(msg_scene["VIS006"].data)
