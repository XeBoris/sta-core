import json
import os
#import sportstrackeranalyzer as
import sta_core as sta


class TypeMapper(object):
    """
    This class introduces a general way to describe sport types
    across different data sources such as runtastic, strava or
    general gps files.
    The general idea is to give users a way to describe sport
    types from this class with the help of a configuration file
    which is shipped out with the package.
    """
    def __init__(self):
        """
        Constructor
        """
        self._basic_path = sta.__path__[0]

        self._track_source = None
        self._track_sources = None #path
        self._source_type = None

        self._basic_mapper = None  # Hold the basic maps (JSON)
        self._track_source_mapper = None

    def set_track_source(self, track_source=None):
        self._track_source = track_source

    def set_source_type(self, source_type=None):
        self._source_type = source_type

    def _load_basic_mapper(self):
        if self._source_type is None:
            self.types = f"{self._basic_path}/configuration/basic_types.config"
        else:
            print("Specify basic mapper file manually.")
        if os.path.exists(self.types):
            with open(self.types) as json_file:
                self._basic_mapper = json.load(json_file)

    def _load_track_source(self):

        if self._track_sources is None:
            self._track_sources = f"{self._basic_path}/configuration/type_mapper.config"
        else:
            print("Specify Sports mapper")

        if os.path.exists(self._track_sources):
            with open(self._track_sources) as json_file:
                self._track_source_mapper = json.load(json_file)
        self._track_source_mapper = self._track_source_mapper.get(self._track_source)

    def loader(self):
        self._load_track_source()
        self._load_basic_mapper()

    def mapper(self, _id):
        try:
            m = [i for i in self._track_source_mapper if i["id"] == str(_id)][0]
            map_type = m.get("map_types")
        except IndexError as e:
            map_type = "0"

        s_type = self._basic_mapper.get("types").get(map_type)

        return s_type
