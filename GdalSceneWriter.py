import os
from datetime import datetime
import numpy as np

from osgeo import gdal


class GdalSceneWriter:
    def __init__(self, path_pattern="./msg_gdal_writer_out", file_pattern="%Y%m%d_%H%M.tif", gdal_format="GTiff", gdal_driver_options=["COMPRESS=DEFLATE", "NUM_THREADS=ALL_CPUS"], gdal_copy_format=None):
        self.path_pattern=path_pattern
        self.file_pattern=file_pattern
        self.gdal_driver=gdal.GetDriverByName( gdal_format )
        self.gdal_driver_options=gdal_driver_options
        self.gdal_copy_driver = None
        if gdal_copy_format is not None:
            self.gdal_copy_driver = gdal.GetDriverByName(gdal_copy_format)
        else:
            self.gdal_copy_driver = None

    def write_scene(self, scene, channel_list=None, channel_scales=None, gdal_type=gdal.GDT_Int16):

        if channel_list is None:
            channel_list = scene.channels

        if channel_scales is None:
            channel_scales = [1.0] * len(channel_list)

        dst_folder = datetime.strftime(scene.date, self.path_pattern)

        if not os.path.exists(dst_folder):
            os.makedirs(dst_folder)

        dst_filename = dst_folder + '/' + datetime.strftime(scene.date, self.file_pattern)

        x_min, y_min, x_max, y_max = scene.pixel_area
        x_size = np.absolute(np.int(x_max - x_min))
        y_size = np.absolute(np.int(y_max - y_min))

        dst_number_of_bands = len(channel_list)

        if self.gdal_copy_driver is not None:
            dst_ds = self.gdal_copy_driver.Create('', x_size, y_size, dst_number_of_bands, gdal_type)
        else:
            dst_ds = self.gdal_driver.Create(dst_filename, x_size, y_size, dst_number_of_bands, gdal_type, options=self.gdal_driver_options)

        dst_ds.SetGeoTransform(scene.geotransform)
        print("scene adf", scene.geotransform)
        dst_ds.SetProjection(scene.wkt)

        if scene.metadata is not None:
            string_metadata = {}
            for key in scene.metadata:
                string_metadata[key] = str(scene.metadata[key])
            dst_ds.SetMetadata( string_metadata )

        for i, (msg_channel_name, channel_scale) in enumerate(zip(channel_list, channel_scales), start=1):
            channel = scene[msg_channel_name]
            band = dst_ds.GetRasterBand(i)

            if channel.no_data_value is not None:
                band.SetNoDataValue(channel.no_data_value)

            data = channel.data
            if channel_scale is not 1.0:
                data = channel.data * channel_scale

            band.WriteArray(data)

            if channel.metadata is not None:
                metadata = channel.metadata.copy()
                for key in metadata:
                    metadata[key] = str(metadata[key])
                metadata['channel_name'] = msg_channel_name
                band.SetMetadata( metadata )

            band.FlushCache()
            band = None

        if self.gdal_copy_driver is not None:
            self.gdal_driver.CreateCopy(dst_filename, dst_ds, options=self.gdal_driver_options)

        dst_ds = None

