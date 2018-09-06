from date_generator import date_generator
from GdalClaasLoader import GdalClaasLoader
from GdalSceneWriter import GdalSceneWriter
import logging
from datetime import datetime
from osgeo import gdalconst


log_file = 'claas_to_training_data_converter.log'
log_level = logging.INFO

date_generator_start_date = datetime(2006, 11, 1)
date_generator_end_date = datetime(2006, 12, 1)

loader_base_path='/media/droenner/DATA_EXTERN/geodata/raster/claas_v2_cma/untar'
loader_prefixes=[""]
loader_pixel_area=(1589, 154, 2356, 664)

writer_gdal_driver_options = ['COMPRESS=DEFLATE', 'NUM_THREADS=ALL_CPUS', 'ZLEVEL=9']
writer_path_pattern = "/media/droenner/DATA_EXTERN/geodata/raster/training/eu_cma_5/%Y/%m"
writer_file_pattern ="%Y%m%d_%H%M.tif"
writer_channel_names=["CMa", "CMa_DUST", "CMa_VOLCANIC"]

out_list_existing = "./claas_existing.txt"
out_list_missing = "./claas_missing.txt"

if __name__ == '__main__':
    print('Meow, thats right!')

    logging.basicConfig(filename=log_file, level=log_level)
    logging.info('Started.')

    missing_dates=[]
    existing_dates=[]

    dates = date_generator(date_generator_start_date, date_generator_end_date)
    logging.info('Generator created')

    loader = GdalClaasLoader(base_path=loader_base_path, prefixes=loader_prefixes)
    logging.info('loader created. base_path= {}. prefix='.format(loader.base_path, loader.prefixes))

    writer = GdalSceneWriter(gdal_driver_options=writer_gdal_driver_options, path_pattern=writer_path_pattern, file_pattern=writer_file_pattern)
    logging.info('writer created. path_pattern='+ writer.path_pattern + 'gdal_driver_options=' + str(writer.gdal_driver_options))

    for d in dates:

        logging.info('${}$ loading scene. date: {} area: {}'.format(d, d, loader_pixel_area))
        scene = loader.load_scene(d, pixel_area=loader_pixel_area)
        if scene is None:
            logging.warning('${}$ MISSING scene. date: {}'.format(d, d))
            missing_dates.append(d)
            continue
        logging.info('${}$ scene loaded. Channels: {}'.format(d, scene.channels))

        logging.info('${}$ writing scene.'.format(d))
        writer.write_scene(scene, channel_list=writer_channel_names, gdal_type=gdalconst.GDT_Byte)
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