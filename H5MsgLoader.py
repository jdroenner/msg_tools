from __future__ import print_function
import numpy as np
import os
from datetime import datetime
import h5py

#from osgeo import gdal, gdalconst

from geos_utils import geos_area_from_pixel_area, pixel_area_from_geos_area
from msg import CHANNEL_NAMES, MSG_SATELLITES, get_channel_number_for_channel_name, get_geos_wkt
from MsgScene import MsgScene, MsgChannel
from file_utils import find_date_filename, unbz2_file, _MSG_HDF5_FILENAME_REGEX_PATTERN, _DEFAULT_FILE_PREFIX

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


class H5MsgLoader:
    def __init__(self, base_path='.', prefixes=[_DEFAULT_FILE_PREFIX], filename_regex_pattern=_MSG_HDF5_FILENAME_REGEX_PATTERN):
        self.base_path = base_path
        self.prefixes = prefixes
        self.filename_regex_pattern = filename_regex_pattern
        self.opt_temp_file = None

    def find_filename(self, date_time, minutes_range = 5):
        return find_date_filename(date_time, self.base_path, self.filename_regex_pattern, self.prefixes, minutes_range)

    def load_scene(self, date_time, channel_names=CHANNEL_NAMES, pixel_area=(0, 0, 3712, 3712), geos_area=None):

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

        path_with_prefix = None
        for pre in self.prefixes:
            new_path_with_prefix = self.base_path + "/" + datetime.strftime(date_time, pre)
            if os.path.isdir(new_path_with_prefix):
                path_with_prefix = new_path_with_prefix
                break

        if path_with_prefix is None:
            return None

        filename, metadata_filename = self.find_filename(date_time)

        if filename is None :
            return None

        full_path = path_with_prefix + '/' + filename

        if metadata_filename['compression'] == ".bz2":
            self.opt_temp_file = unbz2_file(full_path)
            full_path = self.opt_temp_file.name
        
        print("loading h5 data from this file: ", full_path)

        h5file = h5py.File(full_path, "r")

        metadata_image_description = dict(h5file["/U-MARF/MSG/Level1.5/METADATA/HEADER/ImageDescription/ImageDescription_DESCR"][:])
        metadata_calibration_slope_offset = h5file['/U-MARF/MSG/Level1.5/METADATA/HEADER/RadiometricProcessing/Level15ImageCalibration_ARRAY'][:]
        # print(metadata_calibration_slope_offset)

        we_pixel_resolution = np.float(metadata_image_description['ReferenceGridVIS_IR-LineDirGridStep'])  * 1000.0 # TODO: negative?
        ns_pixel_resolution = np.float(metadata_image_description['ReferenceGridVIS_IR-ColumnDirGridStep']) * -1000.0
        sub_satellite_point_lon = np.float(metadata_image_description['ProjectionDescription-LongitudeOfSSP'])
        satellite_number = np.int(metadata_filename['msg_id'])
        # print('we_pixel_resolution, ns_pixel_resolution, sub_sat_lon, satellite_number', we_pixel_resolution, ns_pixel_resolution, sub_sat_lon, satellite_number)


        satellite = MSG_SATELLITES[int(satellite_number)]
        wkt = get_geos_wkt(str(sub_satellite_point_lon))

        top_left_x, top_left_y, _, _ = geos_area
        cropped_geotransform = (top_left_x, we_pixel_resolution, 0.0, top_left_y, 0.0, ns_pixel_resolution)
        print('cropped_geotransform', cropped_geotransform)

        for channel_name in channel_names:
            channel_number = get_channel_number_for_channel_name(channel_name)

            h5_inner_path = '/U-MARF/MSG/Level1.5/DATA/Channel ' + '{i:02d}'.format(i=channel_number) + '/IMAGE_DATA'
            slope, offset = metadata_calibration_slope_offset[channel_number]

            try:
                metadata = {
                    'calibration_offset': offset,
                    'calibration_slope': slope,
                    'channel_number': get_channel_number_for_channel_name(channel_name),
                    'date_time': date_time
                }

                print("metadata", metadata)

                # NOW WE HAVE TO FLIP THE Y-AXIS (MSG DATA IS FLIPPED WHEN ORIGIN = 2)
                flipped_y_min = 3712 - y_max
                flipped_y_max = 3712 - y_min
                data = h5file[h5_inner_path][np.int(y_min):np.int(y_max), np.int(x_min):np.int(x_max)]
                data = np.asarray(data, dtype=np.int16)

                channel_name_list.append((channel_name, MsgChannel(channel_name, data, cropped_geotransform, metadata, satellite, no_data_value=0.0)))

            except KeyError as e:
                print(e)

        h5file = None
        if self.opt_temp_file is not None:
            print("closing temp file:", self.opt_temp_file.name)
            self.opt_temp_file.close() # TODO: remove needed?

        if len(channel_name_list) <= 0:
            return None
        
        print("date_time", date_time)

        #return MsgScene(channel_name_list, date_time, wkt, cropped_geotransform, geos_area, pixel_area)

        return MsgScene(channel_name_list, date_time, wkt, cropped_geotransform, geos_area, pixel_area, sub_satellite_point_lon = sub_satellite_point_lon)


if __name__ == '__main__':
    d= datetime(2017, 1, 1, 00, 15)
    folder = '/mnt/c/msg_data/'
    msg_loader = H5MsgLoader(folder, prefixes=[""])
    filename, match = msg_loader.find_filename(d)
    print(filename, match)
    opt_temp_file = None

    full_path = folder + '/' + filename
    print(full_path)


    if match['compression'] == ".bz2":
        opt_temp_file = unbz2_file(full_path)
        full_path = opt_temp_file.name
        print(full_path)

    h5file = h5py.File(full_path, "r")
    print(dict(h5file["/U-MARF/MSG/Level1.5/METADATA/HEADER/ImageDescription/ImageDescription_DESCR"]))

    print(zip(*h5file["/U-MARF/MSG/Level1.5/METADATA/HEADER/RadiometricProcessing/Level15ImageCalibration_ARRAY"][:]))
    
    print("now close temp file", opt_temp_file)
    opt_temp_file.close()
    
    # use load_scene:

    scene = msg_loader.load_scene(d, pixel_area=(1856, 1856, 3712, 3712))
    print(scene.wkt)




# print(msg_scene["VIS006"])

#msg_loader = GdalMsgLoader('/media/agdbs/6TB_#1')
#msg_scene = msg_loader.load_scene(datetime(2012, 1, 1, 1, 0), geos_area=(-804098.1746833668, 5106867.373326323,  1500160.1123622048, 3576662.2515481785))
#print(msg_scene["VIS006"])

#print(msg_scene["VIS006"].data)
