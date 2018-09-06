class MsgScene:

    def __init__(self, name_channels, date, wkt, geotransform, geos_area=None, pixel_area=None, metadata = None, sub_satellite_point_lon = 0.0):
        self.channels = dict(name_channels)
        self.date = date
        self.wkt = wkt
        self.geotransform = geotransform
        self.geos_area = geos_area
        self.pixel_area = pixel_area
        self.metadata = metadata
        self.sub_satellite_point_lon = sub_satellite_point_lon


    def __str__(self):
        return "Scene: %s - %s > %s" % (self.geos_area, self.date, self.channels.keys())

    def __getitem__(self, index):
        return self.channels.get(index)

    def __setitem__(self, key, value):
        self.channels[key] = value

    def loaded_channels(self): #TODO: we do this to imitate the pytroll scene api
        return self.channels


class MsgChannel:
    def __init__(self, name, data, geotransform, metadata=None, satellite=None, no_data_value=None):
        self.name = name
        self.data = data
        self.geotransform = geotransform
        self.metadata = metadata
        self.satellite = satellite
        self.no_data_value = no_data_value

    def __str__(self):
        return "%s: %s $%s$ $%s$ $%s$" % (self.name, self.data.shape, self.geotransform, self.metadata, self.satellite)

