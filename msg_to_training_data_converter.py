from date_generator import date_generator
from MsgCalibrator import OpenClMsgCalibrator
from GdalMsgLoader import GdalMsgLoader
from H5MsgLoader import H5MsgLoader
from GdalSceneWriter import GdalSceneWriter
from osgeo import gdalconst
import logging
from datetime import datetime

# Defines a buffer added to the pixel area (default = 0)
domainBuffer = 0

# Logging to file + log level
log_file = 'msg_to_training_data_converter.log'
log_level = logging.INFO

# Start and end date (end is exclusive)
date_generator_start_date = datetime(2006, 1, 18, 10, 00)
date_generator_end_date = datetime(2006, 1, 18, 12, 00)

# loader configuration:
# base path should point to the data dir
loader_base_path='/mnt/c/msg_data'
# if data is splitted into sub dirs by date, you can use the date format as part of the prefix
loader_prefixes=[''] # ['%Y_%m_%d-%H_%M'] #["2004_2005_ntfs/%Y/%m/%d/%H%M/", "2004_2005_ntfs/%Y/%m/%d/%Y%m%d_%H%M/"]
# the loader can use pixel or GEOS areas
loader_pixel_area = (0, 0, 3712, 3712) #(1589-domainBuffer, 154-domainBuffer, 2356+domainBuffer, 664+domainBuffer)
# the loader class can be GDAL (for XRIT) or H5 for HDF/netCDF
MSG_LOADER_CLASS = H5MsgLoader # GdalMsgLoader

# the path to the opencl file containing the calibration implementations
calibrator_opencl_file = './opencl/calibrate.cl'

## writer options
# the path where the generated rasters are stored
writer_path_pattern = "/mnt/c/msg_out/%Y/%m"
# the file pattern of the generated files
writer_file_pattern ="%Y%m%d_%H%M.tif"
# the data type of the results
writer_gdal_type = gdalconst.GDT_Float32 #gdalconst.GDT_UInt16

writer_gdal_driver_options = []#['COMPRESS=LZMA', 'NUM_THREADS=ALL_CPUS'] #["QUALITY=100", "REVERSIBLE=YES", "YCBCR420=NO"]
writer_gdal_format = "GTiff" #"JP2OpenJPEG"
writer_channel_list = ["VIS006_RAD_REFL", "VIS008_RAD_REFL", "IR_016_RAD_REFL", "IR_039_RAD_TEMP_CO2CORR", "WV_062_RAD_TEMP", "WV_073_RAD_TEMP", "IR_087_RAD_TEMP", "IR_097_RAD_TEMP", "IR_108_RAD_TEMP", "IR_120_RAD_TEMP", "IR_134_RAD_TEMP", 'azimuth', 'zenith'] #["VIS006_RAD_REFL", "VIS008_RAD_REFL", "IR_016_RAD_REFL", "IR_039_RAD_TEMP_CO2CORR", "WV_062_RAD_TEMP", "WV_073_RAD_TEMP", "IR_087_RAD_TEMP", "IR_097_RAD_TEMP", "IR_108_RAD_TEMP", "IR_120_RAD_TEMP", "IR_134_RAD_TEMP", "azimuth", "zenith"]
writer_channel_scls = None #[ 100*100,       100*100,       100*100,       100,                   100,           100,           100,           100,           100,           100,           100,           100]

out_list_existing = "./msg_existing.txt"
out_list_missing = "./msg_missing.txt"

cl_platform_id = 0
writer_gdal_copy_format = None#'MEM'

if __name__ == '__main__':
    print('Converter running!')

    logging.basicConfig(filename=log_file, level=log_level)
    logging.info('Started.')

    missing_dates=[]
    existing_dates=[]

    dates = date_generator(date_generator_start_date, date_generator_end_date)
    logging.info('Generator created')

    loader = MSG_LOADER_CLASS(base_path=loader_base_path, prefixes=loader_prefixes)
    logging.info('loader created. base_path= {}. prefix='.format(loader.base_path, loader.prefixes))

    calibrator = OpenClMsgCalibrator(calibrator_opencl_file, cl_platform_id=cl_platform_id)
    logging.info('calibrator created. Using class: ' + str(calibrator))

    writer = GdalSceneWriter(gdal_driver_options=writer_gdal_driver_options, path_pattern=writer_path_pattern, file_pattern=writer_file_pattern, gdal_format=writer_gdal_format, gdal_copy_format=writer_gdal_copy_format)
    logging.info('writer created. path_pattern='+ writer.path_pattern + 'gdal_driver_options=' + str(writer.gdal_driver_options))

    for d in dates:

        logging.info('${}$ loading scene. date: {} area: {}'.format(d, d, loader_pixel_area))
        scene = loader.load_scene(d, pixel_area=loader_pixel_area)
        if scene is None:
            logging.warning('${}$ MISSING scene. date: {}'.format(d, d))
            missing_dates.append(d)
            continue
        logging.info('${}$ scene loaded. Channels: {}'.format(d, scene.channels))

        logging.info('${}$ calibrating scene.')
        calibrated_scene = calibrator.calibrate_scene(scene, extend_input_scene=True)
        if calibrated_scene is None:
            logging.warning('${}$ ERROR calibration. date: {}'.format(d, d))
            missing_dates.append(d)
            continue
        logging.info('${}$ scene calibrated. Channels: {}'.format(d, calibrated_scene.channels))

        logging.info('${}$ writing scene.'.format(d))
        writer.write_scene(scene, channel_list=writer_channel_list, channel_scales=writer_channel_scls, gdal_type=writer_gdal_type)
        logging.info('${}$ scene written.'.format(d))

        existing_dates.append(d)

    print('existing_dates', str(existing_dates))
    print('missing_dates', str(missing_dates))

    logging.info('Writing lists')
    file_out_list_existing = open(out_list_existing, 'w')
    for item in existing_dates:
        file_out_list_existing.write("{}\n".format(item))
    file_out_list_missing = open(out_list_missing, 'w')
    for item in missing_dates:
        file_out_list_missing.write("{}\n".format(item))

    logging.info('Finished')