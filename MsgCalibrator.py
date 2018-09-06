import numpy as np
import pyopencl as cl
from MsgScene import MsgScene, MsgChannel
import math
#from astropy import time as astrotime
#from astropy import coordinates as astrocoordinates
from sunpos_intermediate import sunposIntermediate, sun_earth_distance

# constants
_c = 299792458  # ms-1 Speed of light in vacuum
_h = 6.62606957 * math.pow(10, -34)  # Js Planck
_k = 1.3806488 * math.pow(10, -23)  # JK-1 Bolzmann
_radiation_constant_1 = 2 * _h * _c * _c
_radiation_constant_2 = _h * _c / _k
_to_view_angle_fac = 65536.0 / (-13642337.0 * 3000.403165817)
_FLOAT_NO_DATA_VALUE = 0.0
_INT_NO_DATA_VALUE = 0


# print("_radiation_constant_1", _radiation_constant_1)
# print("_radiation_constant_2", _radiation_constant_2)


# Method to load and build an openCL-Program from a given file
def loadOpenCLProgram(filename, ctx):
    # read in the OpenCL source file as a string
    f = open(filename, 'r')
    fstr = "".join(f.readlines())
    # create the program
    return cl.Program(ctx, fstr).build()


def radiance_to_temperature(wavenumber, alpha, beta, radiance):
    """
    convert radiance values into black body temperatures

    :param wavenumber: the wavenumber
    :param alpha: the alpha value
    :param beta: the beta value
    :param radiance: the radiance value
    :return: the bbt temperature
    """
    np.seterr(divide='ignore', invalid='ignore')
    temp = (_radiation_constant_1 * 1.0e6 * (wavenumber * wavenumber * wavenumber)) / (1.0e-5 * radiance)
    return ((_radiation_constant_2 * 100. * wavenumber / np.log(temp + 1.0)) - beta) / alpha


def raw_to_radiance(slope, offset, raw):
    """
    Transform raw counts into radiance values
    :param slope: the calibration slope
    :param offset: the calibration offset
    :param raw: the raw count value
    :return: derived radiance value
    """
    return raw * slope + offset


def raw_to_temperature(wavenumber, alpha, beta, slope, offset, raw):
    """

    :param wavenumber:
    :param alpha:
    :param beta:
    :param slope:
    :param offset:
    :param raw:
    :return:
    """
    np.seterr(divide='ignore', invalid='ignore')
    radiance = raw * slope + offset
    if radiance == 0:
        temp = 0
    else:
        temp = (_radiation_constant_1 * 1.0e6 * (wavenumber * wavenumber * wavenumber)) / (1.0e-5 * radiance)
    return ((_radiation_constant_2 * 100. * wavenumber / np.log(temp + 1.0)) - beta) / alpha


def channel_raw_to_radiance(msg_channel, name_suffix='_RAD'):
    calibration_slope = msg_channel.metadata['calibration_slope']
    calibration_offset = msg_channel.metadata['calibration_offset']

    if calibration_slope is None or calibration_offset is None:
        return None

    co = calibration_offset
    cs = calibration_slope
    calibrated_data = raw_to_radiance(cs, co, msg_channel.data)

    return MsgChannel(data=calibrated_data, metadata=msg_channel.metadata.copy(), geotransform=msg_channel.geotransform,
                      name=msg_channel.name + name_suffix)


def channel_raw_to_temperature_lut(msg_channel):
    calibration_slope = msg_channel.metadata['calibration_slope']
    calibration_offset = msg_channel.metadata['calibration_offset']

    if calibration_slope is None or calibration_offset is None:
        return None

    co = calibration_offset
    cs = calibration_slope

    if msg_channel.metadata is None:
        return None

    channel_number = msg_channel.metadata['channel_number']
    wavenumber = msg_channel.satellite.vc[channel_number - 1]
    alpha = msg_channel.satellite.alpha[channel_number - 1]
    beta = msg_channel.satellite.beta[channel_number - 1]
    a = np.arange(0, 1024, dtype=np.float32)
    g = np.vectorize(lambda l: raw_to_temperature(wavenumber, alpha, beta, cs, co, l))

    lookup = g(a)
    return lookup


def channel_raw_to_temperature_optimized(msg_channel, name_suffix='_RAD_TEMP'):
    lookup = channel_raw_to_temperature_lut(msg_channel)
    lookup_fn = np.vectorize(lambda r: lookup[r])
    temperature_data = lookup_fn(msg_channel.data)
    return MsgChannel(data=temperature_data, metadata=msg_channel.metadata.copy(),
                      geotransform=msg_channel.geotransform, name=msg_channel.name + name_suffix)


def channel_radiance_to_temperature(msg_channel, name_suffix='_TEMP'):
    if msg_channel.metadata is None:
        return None

    channel_number = msg_channel.metadata['channel_number']
    wavenumber = msg_channel.satellite.vc[channel_number - 1]
    alpha = msg_channel.satellite.alpha[channel_number - 1]
    beta = msg_channel.satellite.beta[channel_number - 1]

    temperature_data = radiance_to_temperature(wavenumber, alpha, beta, msg_channel.data)

    return MsgChannel(data=temperature_data, metadata=msg_channel.metadata.copy(),
                      geotransform=msg_channel.geotransform, name=msg_channel.name + name_suffix)


def channel_raw_to_temperature(msg_channel, name_suffix='_RAD_TEMP'):
    calibration_slope = msg_channel.metadata['calibration_slope']
    calibration_offset = msg_channel.metadata['calibration_offset']

    if calibration_slope is None or calibration_offset is None:
        return None

    co = calibration_offset
    cs = calibration_slope
    calibrated_data = raw_to_radiance(cs, co, msg_channel.data)

    if msg_channel.metadata is None:
        return None

    channel_number = msg_channel.metadata['channel_number']
    wavenumber = msg_channel.satellite.vc[channel_number -1]
    alpha = msg_channel.satellite.alpha[channel_number - 1]
    beta = msg_channel.satellite.beta[channel_number - 1]

    temperature_data = radiance_to_temperature(wavenumber, alpha, beta, calibrated_data)

    return MsgChannel(data=temperature_data, metadata=msg_channel.metadata.copy(),
                      geotransform=msg_channel.geotransform, name=msg_channel.name + name_suffix)


class MsgCalibrator:
    def __init__(self):
        pass

    def calibrate_scene(self, msg_scene,
                        rad_bands=['IR_016', 'IR_039', 'IR_087', 'IR_097', 'IR_108', 'IR_120', 'IR_134', 'VIS006',
                                   'VIS008', 'WV_062', 'WV_073'],
                        temp_bands=['IR_039', 'IR_087', 'IR_097', 'IR_108', 'IR_120', 'IR_134', 'WV_062', 'WV_073'],
                        refl_bands=['VIS006', 'VIS008', 'IR_016'],
                        rad_suffix = '_RAD',
                        refl_suffix = '_RAD_REFL',
                        temp_suffix = '_RAD_TEMP'):
        for band_name in rad_bands:
            if msg_scene[band_name] is not None:
                calibrated_channel = channel_raw_to_radiance(msg_scene[band_name], rad_suffix)
                msg_scene[calibrated_channel.name] = calibrated_channel

        for band_name in temp_bands:
            if msg_scene[band_name] is not None:
                calibrated_channel = channel_raw_to_temperature_optimized(msg_scene[band_name], temp_suffix)
                msg_scene[calibrated_channel.name] = calibrated_channel

        # TODO: REFLS
        for band_name in refl_bands:
            if msg_scene[band_name] is not None:
                pass

        return msg_scene


class OpenClMsgCalibrator:
    cl_ctx = None
    cl_program = None
    cl_queue = None

    def __init__(self, program_file, cl_platform_id = 0, cl_device_type = cl.device_type.CPU, cl_ctx = None, cl_queue = None, simpleProfiler = None):

        if cl_ctx is None:
            platforms = cl.get_platforms()
            print("cl platforms ", platforms)
            devices = platforms[cl_platform_id].get_devices(device_type=cl_device_type)
            cl_ctx = cl.Context(devices)
        self.cl_ctx = cl_ctx

        if cl_queue is None:
            cl_queue = cl.CommandQueue(self.cl_ctx)
        self.cl_queue = cl_queue
        self.cl_program = loadOpenCLProgram(ctx=self.cl_ctx, filename=program_file)
        self.simpleProfiler = simpleProfiler

    def cl_azimuth_zenith(self, msg_channel, gmst_degree, asc_radian, dec_radian, sat_sub_lon = 0.0):
        if(self.simpleProfiler is not None):
            self.simpleProfiler.start("cl_azimuth_zenith")

        scale_x = msg_channel.geotransform[1]
        scale_y = msg_channel.geotransform[5]
        offset_x = msg_channel.geotransform[0]
        offset_y = msg_channel.geotransform[3]


        band = msg_channel.data
        azimuth = np.zeros_like(band, dtype=np.float32)
        zenith = np.zeros_like(band, dtype=np.float32)

        kernel_shape = band.shape[::-1]  # this is needed as numpy uses z:y:x instead of x:y:z!!!!

        mf = cl.mem_flags
        azimuth_buffer = cl.Buffer(self.cl_ctx, mf.WRITE_ONLY | mf.USE_HOST_PTR, hostbuf=azimuth)
        zenith_buffer = cl.Buffer(self.cl_ctx, mf.WRITE_ONLY | mf.USE_HOST_PTR, hostbuf=zenith)

        self.cl_program.azimuthZenithKernel(self.cl_queue, kernel_shape, None, azimuth_buffer, zenith_buffer,
                                            np.float64(scale_x),
                                            np.float64(scale_y),
                                            np.float64(offset_x),
                                            np.float64(offset_y),
                                            np.float64(_to_view_angle_fac),
                                            np.float64(gmst_degree),
                                            np.float64(asc_radian),
                                            np.float64(dec_radian),
                                            np.float64(sat_sub_lon)
                                            )

        cl.enqueue_copy(self.cl_queue, azimuth, azimuth_buffer)
        cl.enqueue_copy(self.cl_queue, zenith, zenith_buffer)

        if(self.simpleProfiler is not None):
            self.simpleProfiler.stop("cl_azimuth_zenith")

        return MsgChannel(data=azimuth, metadata=msg_channel.metadata.copy(), geotransform=msg_channel.geotransform,
                          name='azimuth'), MsgChannel(data=zenith, metadata=msg_channel.metadata.copy(),
                                                      geotransform=msg_channel.geotransform, name='zenith')


    def cl_channel_raw_to_radiance(self, msg_channel, convert=False, no_data_value_out = _FLOAT_NO_DATA_VALUE, name_suffix = '_RAD'):

        calibration_slope = np.float32(msg_channel.metadata['calibration_slope'])
        calibration_offset = np.float32(msg_channel.metadata['calibration_offset'])
        conversion_factor = 1.0
        no_data_value_in = _INT_NO_DATA_VALUE

        if msg_channel.no_data_value is not None:
            no_data_value_in = msg_channel.no_data_value

        if calibration_slope is None or calibration_offset is None:
            return None

        if convert:
            channel_number = msg_channel.metadata['channel_number']
            cwl = msg_channel.satellite.cwl[channel_number - 1]
            conversion_factor = 10.0 / (cwl * cwl)
        conversion_factor = np.float32(conversion_factor)

        band = msg_channel.data
        calibrated_data = np.zeros_like(band, dtype=np.float32)
        kernel_shape = band.shape[::-1]  # this is needed as numpy uses z:y:x instead of x:y:z!!!!

        mf = cl.mem_flags
        band_buffer = cl.Buffer(self.cl_ctx, mf.READ_ONLY | mf.USE_HOST_PTR, hostbuf=band)
        cl.enqueue_copy(self.cl_queue, band_buffer, band) # TODO: Needed?

        calibrated_data_buffer = cl.Buffer(self.cl_ctx, mf.WRITE_ONLY | mf.USE_HOST_PTR, hostbuf=calibrated_data)

        self.cl_program.rawToRadianceKernel(self.cl_queue, kernel_shape, None, band_buffer, calibrated_data_buffer,
                                            calibration_offset,
                                            calibration_slope,
                                            conversion_factor,
                                            np.int16(no_data_value_in),
                                            np.float32(no_data_value_out),
                                            )

        cl.enqueue_copy(self.cl_queue, calibrated_data, calibrated_data_buffer)
        return MsgChannel(data=calibrated_data, metadata=msg_channel.metadata.copy(),
                          geotransform=msg_channel.geotransform,
                          name=msg_channel.name + name_suffix, no_data_value=no_data_value_out)

    def cl_channel_raw_to_reflectance(self, msg_channel, gmst_degree, asc_radian, dec_radian, esd, solar_correction=True, no_data_value_out = _FLOAT_NO_DATA_VALUE, sat_sub_lon=0.0, name_suffix = '_RAD_REFL'):

        calibration_slope = msg_channel.metadata['calibration_slope']
        calibration_offset = msg_channel.metadata['calibration_offset']
        channel_number = msg_channel.metadata['channel_number']

        if calibration_slope is None or calibration_offset is None or channel_number is None:
            return None

        no_data_value_in = _INT_NO_DATA_VALUE

        if msg_channel.no_data_value is not None:
            no_data_value_in = msg_channel.no_data_value

        scale_x = msg_channel.geotransform[1]
        scale_y = msg_channel.geotransform[5]
        offset_x = msg_channel.geotransform[0]
        offset_y = msg_channel.geotransform[3]

        etsr = msg_channel.satellite.etsr[channel_number - 1] / np.pi

        band = msg_channel.data
        calibrated_data = np.zeros_like(band, dtype=np.float32)
        kernel_shape = band.shape[::-1]  # this is needed as numpy uses z:y:x instead of x:y:z!!!!

        mf = cl.mem_flags
        band_buffer = cl.Buffer(self.cl_ctx, mf.READ_ONLY | mf.USE_HOST_PTR, hostbuf=band)
        calibrated_data_buffer = cl.Buffer(self.cl_ctx, mf.WRITE_ONLY | mf.USE_HOST_PTR, hostbuf=calibrated_data)
        cl.enqueue_copy(self.cl_queue, band_buffer, band) # TODO: Needed?


        if solar_correction:
            # __kernel void reflectanceWithSolarCorrectionKernel(__global const int *in_data, __global float *out_data, const float offset, const float slope, const double dETSRconst, const double dESD, const double scale_x, const double scale_y, const double origin_x, const double origin_y, const double projectionCooridnateToViewAngleFactor, const double dGreenwichMeanSiderealTime, const double dRightAscension, const double dDeclination) {
            self.cl_program.rawToReflectanceWithSolarCorrectionKernel(self.cl_queue, kernel_shape, None, band_buffer, calibrated_data_buffer,
                                                                 np.float32(calibration_offset),
                                                                 np.float32(calibration_slope),
                                                                 np.float64(etsr),
                                                                 np.float64(esd),
                                                                 np.float64(scale_x),
                                                                 np.float64(scale_y),
                                                                 np.float64(offset_x),
                                                                 np.float64(offset_y),
                                                                 np.float64(_to_view_angle_fac),
                                                                 np.float64(gmst_degree),
                                                                 np.float64(asc_radian),
                                                                 np.float64(dec_radian),
                                                                 np.int16(no_data_value_in),
                                                                 np.float32(no_data_value_out),
                                                                 np.float64(sat_sub_lon),
                                            )

        else:
            # __kernel void rawToReflectanceWithoutSolarCorrectionKernel(__global const int *in_data, __global float *out_data, const float offset, const float slope, const double dETSRconst, const double dESD) {
            self.cl_program.rawToReflectanceWithoutSolarCorrectionKernel(self.cl_queue, kernel_shape, None, band_buffer,
                                                                 calibrated_data_buffer,
                                                                 np.float32(calibration_offset),
                                                                 np.float32(calibration_slope),
                                                                 np.float64(etsr),
                                                                 np.float64(esd.value),
                                                                 np.int16(no_data_value_in),
                                                                 np.float32(no_data_value_out),
                                                                 )

        cl.enqueue_copy(self.cl_queue, calibrated_data, calibrated_data_buffer)
        return MsgChannel(data=calibrated_data, metadata=msg_channel.metadata.copy(),
                          geotransform=msg_channel.geotransform,
                          name=msg_channel.name + name_suffix,
                          no_data_value=no_data_value_out)

    def cl_channel_raw_to_bbt(self, msg_channel, no_data_value_out = _FLOAT_NO_DATA_VALUE, name_suffix='_RAD_TEMP'):
        lookup = channel_raw_to_temperature_lut(msg_channel)

        no_data_value_in = _INT_NO_DATA_VALUE
        if msg_channel.no_data_value is not None:
            no_data_value_in = msg_channel.no_data_value

        band = msg_channel.data
        calibrated_data = np.zeros_like(band, dtype=np.float32)
        kernel_shape = band.shape[::-1]  # this is needed as numpy uses z:y:x instead of x:y:z!!!!

        mf = cl.mem_flags
        band_buffer = cl.Buffer(self.cl_ctx, mf.READ_ONLY | mf.USE_HOST_PTR, hostbuf=band)
        lookup_buffer = cl.Buffer(self.cl_ctx, mf.READ_ONLY | mf.USE_HOST_PTR, hostbuf=lookup)

        calibrated_data_buffer = cl.Buffer(self.cl_ctx, mf.WRITE_ONLY | mf.USE_HOST_PTR, hostbuf=calibrated_data)
        cl.enqueue_copy(self.cl_queue, band_buffer, band)  # TODO: Needed?
        cl.enqueue_copy(self.cl_queue, lookup_buffer, lookup)  # TODO: Needed?

        self.cl_program.rawToBbtKernel(
            self.cl_queue,
            kernel_shape,
            None, band_buffer,
            calibrated_data_buffer,
            lookup_buffer,
            np.int16(no_data_value_in),
            np.float32(no_data_value_out)
        )

        cl.enqueue_copy(self.cl_queue, calibrated_data, calibrated_data_buffer)
        return MsgChannel(data=calibrated_data, metadata=msg_channel.metadata.copy(),
                          geotransform=msg_channel.geotransform,
                          name=msg_channel.name + name_suffix,
                          no_data_value=no_data_value_out)

    def co2_correction(self, bt039_channel, bt108_channel, bt134_channel, no_data_value_out = _FLOAT_NO_DATA_VALUE, name_suffix = '_CO2CORR'):

        no_data_value_in = _FLOAT_NO_DATA_VALUE
        if bt039_channel.no_data_value is not None:
            no_data_value_in = bt039_channel.no_data_value

        calibrated_data = np.zeros_like(bt039_channel.data, dtype=np.float32)
        kernel_shape = bt039_channel.data.shape[::-1]  # this is needed as numpy uses z:y:x instead of x:y:z!!!!

        mf = cl.mem_flags
        bt039_channel_buffer = cl.Buffer(self.cl_ctx, mf.READ_ONLY | mf.USE_HOST_PTR, hostbuf=bt039_channel.data)
        bt108_channel_buffer = cl.Buffer(self.cl_ctx, mf.READ_ONLY | mf.USE_HOST_PTR, hostbuf=bt108_channel.data)
        bt134_channel_buffer = cl.Buffer(self.cl_ctx, mf.READ_ONLY | mf.USE_HOST_PTR, hostbuf=bt134_channel.data)

        calibrated_data_buffer = cl.Buffer(self.cl_ctx, mf.WRITE_ONLY | mf.USE_HOST_PTR, hostbuf=calibrated_data)
        cl.enqueue_copy(self.cl_queue, bt039_channel_buffer, bt039_channel.data)  # TODO: Needed?
        cl.enqueue_copy(self.cl_queue, bt108_channel_buffer, bt108_channel.data)  # TODO: Needed?
        cl.enqueue_copy(self.cl_queue, bt134_channel_buffer, bt134_channel.data)  # TODO: Needed?

        self.cl_program.co2CorrectionKernel(self.cl_queue, kernel_shape, None, bt039_channel_buffer, bt108_channel_buffer,
                                       bt134_channel_buffer, calibrated_data_buffer, np.float32(no_data_value_in), np.float32(no_data_value_out))

        cl.enqueue_copy(self.cl_queue, calibrated_data, calibrated_data_buffer)
        return MsgChannel(data=calibrated_data, metadata=bt039_channel.metadata.copy(),
                          geotransform=bt039_channel.geotransform,
                          name=bt039_channel.name + name_suffix, no_data_value=no_data_value_out)



    def calibrate_scene(self, raw_scene,
                        rad_bands=['IR_016', 'IR_039', 'IR_087', 'IR_097', 'IR_108', 'IR_120', 'IR_134', 'VIS006', 'VIS008', 'WV_062', 'WV_073'],
                        temp_bands=['IR_039', 'IR_087', 'IR_097', 'IR_108', 'IR_120', 'IR_134', 'WV_062', 'WV_073'],
                        refl_bands=['VIS006', 'VIS008', 'IR_016'],
                        rad_suffix='_RAD',
                        refl_suffix='_RAD_REFL',
                        temp_suffix='_RAD_TEMP',
                        co2_correct_suffix = '_CO2CORR',
                        azimuth_zenith=True,
                        bt039_co2_correction=True,
                        extend_input_scene=False
                        ):
        print('Using OpenCL calibrator ctx: ', self.cl_ctx)

        if(self.simpleProfiler is not None):
            self.simpleProfiler.start("OpenClMsgCalibrator_calibrate_scene")

        msg_scene = raw_scene
        if extend_input_scene is False:
            msg_scene = MsgScene([], raw_scene.date, raw_scene.wkt, raw_scene.geotransform, raw_scene.geos_area, raw_scene.pixel_area)

        for band_name in rad_bands:
            if raw_scene[band_name] is not None:
                calibrated_channel = self.cl_channel_raw_to_radiance(raw_scene[band_name], name_suffix = rad_suffix)
                msg_scene[calibrated_channel.name] = calibrated_channel

        for band_name in temp_bands:
            if raw_scene[band_name] is not None:
                calibrated_channel = self.cl_channel_raw_to_bbt(raw_scene[band_name], name_suffix=temp_suffix)
                msg_scene[calibrated_channel.name] = calibrated_channel

        if bt039_co2_correction: #TODO: check if all channels are there
            calibrated_channel = self.co2_correction(msg_scene["IR_039"+temp_suffix], msg_scene["IR_108"+temp_suffix], msg_scene["IR_134"+temp_suffix], name_suffix=co2_correct_suffix)
            msg_scene[calibrated_channel.name] = calibrated_channel

        # TODO: check if there are refls at all :D
        # get the astro params only once for a scene... NOTE: use accessors like .radians .degree to get values from angles!
        #astro_time = astrotime.Time(msg_scene.date, scale='utc')
        #julian_day = astro_time.jd
        #gmst = astro_time.sidereal_time('mean', 'greenwich')
        #astro_sun = astrocoordinates.get_sun(astro_time)
        #gmst_degree = gmst.degree
        #sun_ra = astro_sun.ra.radian
        #sun_dec = astro_sun.dec.radian
        #sun_dist = astro_sun.distance.value

        #FIXME: USE OLD SUNPOS AS WORKAROUND
        print("msg_scene", msg_scene.date)

        sun_ra, sun_dec, gmst_degree = sunposIntermediate(msg_scene.date.year, msg_scene.date.month, msg_scene.date.day, msg_scene.date.hour, msg_scene.date.minute, msg_scene.date.second)
        sun_dist = sun_earth_distance(msg_scene.date)
        sat_sub_lon = msg_scene.sub_satellite_point_lon

        #print('astro:', julian_day, gmst, sun_ra, sun_dec, sun_dist, astro_sun)
        if (azimuth_zenith):
            azimuth, zenith = self.cl_azimuth_zenith(raw_scene[refl_bands[0]], gmst_degree, sun_ra, sun_dec, sat_sub_lon)
            msg_scene[azimuth.name] = azimuth
            msg_scene[zenith.name] = zenith

        for band_name in refl_bands:
            if raw_scene[band_name] is not None:
                calibrated_channel = self.cl_channel_raw_to_reflectance(raw_scene[band_name], gmst_degree=gmst_degree, asc_radian=sun_ra, dec_radian= sun_dec, esd=sun_dist, solar_correction=True, sat_sub_lon=sat_sub_lon, name_suffix = refl_suffix)
                msg_scene[calibrated_channel.name] = calibrated_channel

        if (self.simpleProfiler is not None):
            self.simpleProfiler.stop("OpenClMsgCalibrator_calibrate_scene")

        return msg_scene