from datetime import datetime

CHANNEL_NAMES = ['VIS006', 'VIS008', 'IR_016', 'IR_039', 'WV_062', 'WV_073', 'IR_087', 'IR_097', 'IR_108', 'IR_120', 'IR_134']
CENTRAL_WAVELENGTH = [0.639, 0.809, 1.635, 3.965, 6.337, 7.362, 8.718, 9.668, 10.763, 11.938, 13.355, 0.674]

# solar irradiance *PI             VIS006,  VIS008,  IR_016,  IR_039,  WV_062,  WV_073,  IR_087,  IR_097,  IR_108,  IR_120,  IR_134,     HRV
METEOSAT_08_ETSR = [65.2296, 73.0127, 62.3715, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 78.7599]
METEOSAT_09_ETSR = [65.2065, 73.1869, 61.9923, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 79.0113]
METEOSAT_10_ETSR = [65.5148, 73.1807, 62.0208, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 78.9416]
METEOSAT_11_ETSR = [65.2656, 73.1692, 61.9416, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 0.00000, 79.0035]

# VC constant to approximate BBT       VIS006,  VIS008,  IR_016,   IR_039,   WV_062,   WV_073,   IR_087,  IR_097,   IR_108,  IR_120,  IR_134,    HRV
METEOSAT_08_VC = [0.00000, 0.00000, 0.00000, 2567.330, 1598.103, 1362.081, 1149.069, 1034.343, 930.647, 839.660, 752.387, 0.00000]
METEOSAT_09_VC = [0.00000, 0.00000, 0.00000, 2568.832, 1600.548, 1360.330, 1148.620, 1035.289, 931.700, 836.445, 751.792, 0.00000]
METEOSAT_10_VC = [0.00000, 0.00000, 0.00000, 2547.771, 1595.621, 1360.337, 1148.130, 1034.715, 929.842, 838.659, 750.653, 0.00000]
METEOSAT_11_VC =   [0.00000, 0.00000, 0.00000, 2555.280, 1596.080, 1361.748, 1147.433, 1034.851, 931.122, 839.113, 748.585, 0.00000]

# ALPHA constant to approximate BBT     VIS006,  VIS008,  IR_016,  IR_039  WV_062, WV_073, IR_087, IR_097, IR_108, IR_120, IR_134,  HRV
METEOSAT_08_ALPHA  = [0.00000, 0.00000, 0.00000, 0.9956, 0.9962, 0.9991, 0.9996, 0.9999, 0.9983, 0.9988, 0.9981, 0.0]
METEOSAT_09_ALPHA  = [0.00000, 0.00000, 0.00000, 0.9954, 0.9963, 0.9991, 0.9996, 0.9999, 0.9983, 0.9988, 0.9981, 0.0]
METEOSAT_10_ALPHA  = [0.00000, 0.00000, 0.00000, 0.9915, 0.9960, 0.9991, 0.9996, 0.9999, 0.9983, 0.9988, 0.9982, 0.0]
METEOSAT_11_ALPHA  = [0.00000, 0.00000, 0.00000, 0.9916, 0.9959, 0.9990, 0.9996, 0.9998, 0.9983, 0.9988, 0.9981, 0.0]

# ALPHA constant to approximate BBT     VIS006,  VIS008,  IR_016, IR_039, WV_062, WV_073, IR_087, IR_097, IR_108, IR_120, IR_134, HRV
METEOSAT_08_BETA = [0.00000, 0.00000, 0.00000, 3.4100, 2.2180, 0.4780, 0.1790, 0.0600, 0.6250, 0.3970, 0.5780, 0.0]
METEOSAT_09_BETA = [0.00000, 0.00000, 0.00000, 3.4380, 2.1850, 0.4700, 0.1790, 0.0560, 0.6400, 0.4080, 0.5610, 0.0]
METEOSAT_10_BETA = [0.00000, 0.00000, 0.00000, 2.9002, 2.0337, 0.4340, 0.1714, 0.0527, 0.6084, 0.3882, 0.5390, 0.0]
METEOSAT_11_BETA = [0.00000, 0.00000, 0.00000, 2.9438, 2.0780, 0.4929, 0.1731, 0.0597, 0.6256, 0.4002, 0.5635, 0.0]

GEOS_WKT = """PROJCS["unnamed",
    GEOGCS["WGS 84",
        DATUM["WGS_1984",
            SPHEROID["WGS 84",6378137,298.257223563,
                AUTHORITY["EPSG","7030"]],
            AUTHORITY["EPSG","6326"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.0174532925199433,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4326"]],
    PROJECTION["Geostationary_Satellite"],
    PARAMETER["central_meridian",@central_meridian@],
    PARAMETER["satellite_height",35785831],
    PARAMETER["false_easting",0],
    PARAMETER["false_northing",0]]
"""

class MsgSatellite:
    def __init__(self, name, meteosat_id, msg_id, cwl, etsr, vc, alpha, beta, operation_date, spacecraft_id):
        self.name = name
        self.meteosat_id = meteosat_id
        self.msg_id = msg_id
        self.cwl = cwl
        self.etsr = etsr
        self.vc = vc
        self.alpha = alpha
        self.beta = beta
        self.operation_date = operation_date #FIXME!!!
        self.spacecraft_id = spacecraft_id

# dates from here: https://directory.eoportal.org/web/eoportal/satellite-missions/m/meteosat-second-generation ?

MSG_SATELLITES = {
    1: MsgSatellite("Meteosat-8", 8, 1, CENTRAL_WAVELENGTH, METEOSAT_08_ETSR, METEOSAT_08_VC, METEOSAT_08_ALPHA, METEOSAT_08_BETA, datetime(2004,1,1), 321), # 09.01.2004
    2: MsgSatellite("Meteosat-9", 9, 2, CENTRAL_WAVELENGTH, METEOSAT_09_ETSR, METEOSAT_09_VC, METEOSAT_09_ALPHA, METEOSAT_09_BETA, datetime(2008,1,1), 322),   # 11.04.2007
    3: MsgSatellite("Meteosat-10", 10, 3, CENTRAL_WAVELENGTH, METEOSAT_10_ETSR, METEOSAT_10_VC, METEOSAT_10_ALPHA, METEOSAT_10_BETA, datetime(2013,1,1), 323), # Dec. 18, 2012
    4: MsgSatellite("Meteosat-11", 11, 4, CENTRAL_WAVELENGTH, METEOSAT_11_ETSR, METEOSAT_11_VC, METEOSAT_11_ALPHA, METEOSAT_11_BETA, datetime(2013,1,1), 324),
}


def get_msg_name_for_date(date_time):
    msg_number = 0
    for msg in MSG_SATELLITES:
        satellite = MSG_SATELLITES[msg]
        if date_time >= satellite.operation_date:
            msg_number = satellite.msg_id

    return 'MSG'+str(msg_number)


def get_xrit_filename(date_time, channel, date_pattern="%Y%m%d%H%M"):
    msg_name = get_msg_name_for_date(date_time)
    return "H-000-" + msg_name + "__-" + msg_name + "________-" + channel + "___-000001___-" + datetime.strftime(
        date_time, date_pattern) + "-C_"

def get_channel_number_for_channel_name(channel_name):
    return CHANNEL_NAMES.index(channel_name) + 1

def get_geos_wkt(central_meridian = '0.0'):
    return GEOS_WKT.replace('@central_meridian@', central_meridian)