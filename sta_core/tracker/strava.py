import os
import json
import xml.etree.ElementTree as ET
import gpxpy
import gpxpy.gpx
import datetime
import hashlib
import pandas as pd
import numpy as np
import time
from stravalib import Client

# from .db_handler import DataBaseHandler
# from .shelve_handler import ShelveHandler

from .blueprint import Blueprint
from .type_mapper import TypeMapper
from ..handler.shelve_handler import ShelveHandler
from ..handler.db_handler import DataBaseHandler

class StravaTokenHandler(object):
    def __init__(self):
        self.db_dict = None
        self.db_temp = ShelveHandler()
        self.refresh_token = None
        self.client_id = None
        self.client_secret = None

    def load_token(self):
        self.db_dict = self.db_temp.read_shelve_by_keys(["db_name",
                                                         "db_type",
                                                         "db_path",
                                                         "db_user",
                                                         "db_hash",
                                                         'db_strava'])

        dbh = DataBaseHandler(db_type=self.db_dict["db_type"])
        dbh.set_db_path(db_path=self.db_dict["db_path"])
        dbh.set_db_name(db_name=self.db_dict["db_name"])

        fdict = dbh.list_user_by_hash(self.db_dict["db_hash"])

        self.client_id = fdict["strava"][-1]["client_id"]
        self.client_secret = fdict["strava"][-1]["client_secret"]
        print(fdict)

    def update_token(self):
        print(self.db_dict)
        client = Client(access_token=self.db_dict["db_strava"]["access_token"])

        if time.time() > self.db_dict["db_strava"]["expires_at"]:
            print(self.db_dict["db_strava"]["refresh_token"])
            refresh_response = client.refresh_access_token(client_id=self.client_id,
                                                           client_secret=self.client_secret,
                                                           refresh_token=self.db_dict["db_strava"]["refresh_token"])
            # print(refresh_response.json())
            self.db_dict["db_strava"]["refresh_token"] = refresh_response["refresh_token"]
            self.db_dict["db_strava"]["access_token"] = refresh_response["access_token"]
            self.db_dict["db_strava"]["expires_at"] = refresh_response["expires_at"]

            self.db_temp.write_shelve(self.db_dict)
            self.load_token()
        else:
            dt = self.db_dict["db_strava"]["expires_at"] - time.time()
            print(f"Token valid for another {dt} seconds.")


#        client = Client(access_token=self.db_dict["db_strava"]["access_token"])


class Strava():

    def __init__(self):
        # Init the important variables in the beginning
        self.gps_path = None
        self.df = None
        self.track_name = None

        self.activity_raw_dates = None
        self.activity_raw_date_beg = None
        self.activity_raw_date_end = None

        self.bp = Blueprint()

        self._init_database_handler()

    def _get_all_sport_sessions(self):
        """
        Read all your sport sessions from the available database dump.
        Assume that the <UUID>.json file structure holds for unique
        files. 1 UUID == 1 file == 1 sports activity
        """

        session_paths = []
        for (dirpath, dirnames, filenames) in os.walk(self.gps_path):
            session_paths.extend(filenames)
            break

        session_paths = [i for i in session_paths if i.find(".gpx") > 0]
        return session_paths

    def _init_database_handler(self):
        # init a database handler here:
        self.db_temp = ShelveHandler()
        self.db_dict = self.db_temp.read_shelve_by_keys(["db_name", "db_type", "db_path",
                                                         "db_user", "db_hash", "db_strava"])
        if self.db_dict.get("db_hash") is None:
            print("You have to choose as user first")
            return

        self.dbh = DataBaseHandler(db_type=self.db_dict["db_type"])
        self.dbh.set_db_path(db_path=self.db_dict["db_path"])
        self.dbh.set_db_name(db_name=self.db_dict["db_name"])

    def set_gps_file(self, gps_file):
        self.gps_file = gps_file

    def set_gps_path(self, gps_path):
        self.gps_path = gps_path

    def set_activity_dates(self, raw_dates):
        self.activity_raw_dates = raw_dates
        self._extract_dates()

    def _extract_dates(self):
        """
        This member function encodes the dates when they are handed over by e.g. CLI
        by in a certain string format. Alternative is to use the member functions to set up
        the date of the beginning and ending of the activity.
        :return:
        """
        if self.activity_raw_dates is not None:
            self.activity_raw_date_beg = self.activity_raw_dates.split("-")[0]
            self.activity_raw_date_end = self.activity_raw_dates.split("-")[1]

        if "T" in self.activity_raw_date_beg:
            self.activity_raw_date_beg = datetime.datetime.strptime(self.activity_raw_date_beg, "%Y%m%dT%H:%M")
        else:
            self.activity_raw_date_beg = datetime.datetime.strptime(self.activity_raw_date_beg, "%Y%m%d")

        if "T" in self.activity_raw_date_end:
            self.activity_raw_date_end = datetime.datetime.strptime(self.activity_raw_date_end, "%Y%m%dT%H:%M")
        else:
            self.activity_raw_date_end = datetime.datetime.strptime(self.activity_raw_date_end, "%Y%m%d")

    def set_activity_date_beg(self, beg_date):
        if isinstance(beg_date, str):
            self.activity_raw_date_beg = beg_date
        elif isinstance(beg_date, datetime):
            self.activity_raw_date_beg = beg_date
        else:
            print(f"Check the input {beg_date}")

    def set_activity_date_end(self, end_date):
        if isinstance(end_date, str):
            self.activity_raw_date_end = end_date
        elif isinstance(end_date, datetime):
            self.activity_raw_date_end = end_date
        else:
            print(f"Check the input {end_date}")

    def import_strava_gpx_from_path(self):
        print(self.gps_path)

        all_gpx = self._get_all_sport_sessions()
        for i_gpx in all_gpx:
            gps_file = os.path.join(self.gps_path, i_gpx)
            print(gps_file)
            self.set_gps_file(gps_file)
            self.import_strava_gpx()

    def import_strava_gpx(self):
        # Import a single gpx file here:
        self.load_gps()

        beg_branch = int(min(self.df["timestamp"]) * 1000)
        end_branch = int(max(self.df["timestamp"]) * 1000)

        # Load the blueprint:
        blueprint_session = self.bp.get_branch_blueprint("1")

        blueprint_session["start_time"] = beg_branch
        blueprint_session["end_time"] = end_branch
        blueprint_session["created_at"] = beg_branch
        blueprint_session["updated_at"] = end_branch
        blueprint_session["title"] = self.track_name
        blueprint_session["notes"] = self.track_name
        blueprint_session["start_time_timezone_offset"] = None
        blueprint_session["end_time_timezone_offset"] = None
        blueprint_session["sports_type"] = None
        blueprint_session["source"] = "StravaGPS"

        if blueprint_session["sports_type"] is None:
            activity = self.bp.manual_sport_mapper()
            blueprint_session["sports_type"] = activity

        if blueprint_session["start_time_timezone_offset"] is None:
            print("Add time offset between UTC and timezone when tour was made:")
            print("Allowed only in NANOSECONDs")
            offset = input("Offset [ns]: ")
            blueprint_session["start_time_timezone_offset"] = offset
            blueprint_session["end_time_timezone_offset"] = offset

        # We add a track_hash to each track to make it unique:
        hash_str = f"{blueprint_session.get('start_time')}{blueprint_session.get('end_time')}"
        hash_str = hashlib.md5(hash_str.encode("utf-8")).hexdigest()[0:8]
        blueprint_session["track_hash"] = hash_str

        # We add the user specific hash to the track/branch for identification:
        blueprint_session["user_hash"] = self.db_dict.get("db_hash")

        # dbh.write_branch(db_operation="new", track=rt_json)
        self.dbh.write_branch(db_operation="update",
                              track=blueprint_session,
                              track_hash=hash_str)

        # GPS LEAF:
        # We create a branch which holds only gps relevant information:
        # HINT: We do not load the blueprint here! We have already
        #       a DataFrame with the important information to build the
        #       the leaf.
        #       The Blueprint is used in the function load_gpx() here in
        #       the class.
        # gps relevant infomation:
        obj_gps_defintion = ["timestamp", "longitude", "latitude",
                             "altitude"]
        df_sel = self.df[self.df.columns & obj_gps_defintion]

        # Create leaf configuration:
        leaf_config = self.dbh.create_leaf_config(leaf_name="gps",
                                                  track_hash=hash_str,
                                                  columns=obj_gps_defintion)

        # Write the first leaf:
        r = self.dbh.write_leaf(track_hash=hash_str,
                                leaf_config=leaf_config,
                                leaf=df_sel,
                                leaf_type="DataFrame"
                                )
        if r is True:
            print("GPS leaf written")
            del df_sel
            del self.df

    def import_strava_api(self):
        # before we start to get activities we test if our token is still valid:
        sth = StravaTokenHandler()
        sth.load_token()
        sth.update_token()
        del sth

        client = Client(access_token=self.db_dict["db_strava"]["access_token"])
        athlete = client.get_athlete()
        athlete_id = athlete.id

        activity_stream_types = ["time", "latlng", "distance", "altitude", "velocity_smooth",
                                 "heartrate", "cadence", "watts", "temp", "moving", "grade_smooth"]

        for activity in client.get_activities(
                before=self.activity_raw_date_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                after=self.activity_raw_date_beg.strftime("%Y-%m-%dT%H:%M:%SZ"),
                limit=100):
            # The first order task is to form a common branch description
            # from the Strava API activity (aka. activity -> branch/track)
            write_success, hash_str = self._handle_activity_from__strava_api(activity=activity)

            if write_success is False:
                print("We do not need to continue when creating the branch/track creation fails.")
                break


            #Extract more details from the activity via the API
            activity_stream = client.get_activity_streams(activity_id=activity.id,
                                                          types=activity_stream_types,
                                                          resolution="high",
                                                          # series_type=None
                                                          )

            #continue
            # Create a GPS data leaf
            self._handle_activity_from_strava_api_gps(activity=activity,
                                                      activity_stream=activity_stream,
                                                      hash_str=hash_str)

            # Create a strava based distance leaf
            self._handle_activity_from_strava_api_distances(activity=activity,
                                                            activity_stream=activity_stream,
                                                            hash_str=hash_str)

            # Create strava based metadata leaf
            self._handle_activity_from_stravi_api_metadata(activity=activity,
                                                           activity_stream=activity_stream,
                                                           hash_str=hash_str)


    def _handle_activity_from_strava_api_distances(self, activity=None, activity_stream=None, hash_str=None):
        """

        :param activity:
        :param activity_stream:
        :param hash_str:
        :return:
        """

        blueprint_session = self.bp.get_leaf_blueprint(leaf_type="distance",
                                                       version="1")

        activity_start_time = int(datetime.datetime.timestamp(activity.start_date.replace(tzinfo=None)))

        p_time = activity_stream['time'].data
        p_timestamp = [activity_start_time + i for i in p_time]
        p_distance = activity_stream['distance'].data
        p_velocity_smooth = activity_stream['velocity_smooth'].data

        blueprint_session["timestamp"] = [activity_start_time + i for i in p_time]
        blueprint_session["distance"] = p_distance
        blueprint_session["duration"] = p_time
        blueprint_session["speed"] = p_velocity_smooth
        blueprint_session["version"] = [None for i in range(len(p_time))]

        df_sel = pd.DataFrame(blueprint_session)
        obj_gps_defintion = list(df_sel.columns)

        # Create leaf configuration:
        leaf_config = self.dbh.create_leaf_config(leaf_name="strava_distances",
                                                  track_hash=hash_str,
                                                  columns=obj_gps_defintion)

        # Write the first leaf:
        r = self.dbh.write_leaf(track_hash=hash_str,
                                leaf_config=leaf_config,
                                leaf=df_sel,
                                leaf_type="DataFrame"
                                )
        if r is True:
            print("Strava API distances leaf written")
            del df_sel

        return r

    def _handle_activity_from_stravi_api_metadata(self, activity=None, activity_stream=None, hash_str=None):

        try:
            max_speed = float(activity.max_speed)
        except ValueError:
            max_speed = None
        try:
            average_speed = float(activity.average_speed)
        except ValueError:
            average_speed = None
        try:
            average_watts = float(activity.average_watts)
            #if float is None, we need a  proper exception
        except TypeError:
             average_watts = None
        except:
            average_watts = None
        try:
            max_watts = float(activity.max_watts)
        except TypeError:
            max_watts = None
        except:
            max_watts = None

        blueprint_session = self.bp.get_leaf_blueprint(leaf_type="strava_metadata",
                                                       version="1")

        blueprint_session["longitude"] = [activity.start_latlng[1]]
        blueprint_session["latitude"] = [activity.start_latlng[0]]
        blueprint_session["calories"] = [activity.calories]
        blueprint_session["max_speed"] = [max_speed]
        blueprint_session["average_speed"] = [average_speed]
        blueprint_session["average_watts"] = [average_watts]
        blueprint_session["max_watts"] = [max_watts]
        blueprint_session["private"] = [activity.private]
        blueprint_session["commute"] = [activity.commute]
        blueprint_session["subjective_feeling_id"] = [activity.suffer_score]
        blueprint_session["pause_duration"] = [(activity.elapsed_time - activity.moving_time).total_seconds()]


        df_sel = pd.DataFrame(blueprint_session)
        obj_gps_defintion = list(df_sel.columns)

        # Create leaf configuration:
        leaf_config = self.dbh.create_leaf_config(leaf_name="strava_metadata",
                                                  track_hash=hash_str,
                                                  columns=obj_gps_defintion)

        # Write the first leaf:
        r = self.dbh.write_leaf(track_hash=hash_str,
                                leaf_config=leaf_config,
                                leaf=df_sel,
                                leaf_type="DataFrame"
                                )
        if r is True:
            print("Strava API metadata leaf written")
            del df_sel

        return r

    def _handle_activity_from_strava_api_gps(self, activity=None, activity_stream=None, hash_str=None):
        """
        This function handles the activity and the activity_stream object in order
        to create a GPS oriented leaf from it.

        :param activity:
        :param activity_stream:
        :param hash_str:
        :return:
        """

        blueprint_session = self.bp.get_leaf_blueprint(leaf_type="positions",
                                                       version="1")


        activity_start_time = int(datetime.datetime.timestamp(activity.start_date.replace(tzinfo=None)))

        # print(activity_stream)
        p_latlng = activity_stream['latlng'].data
        p_time = activity_stream['time'].data
        p_distance = activity_stream['distance'].data
        p_altitude = activity_stream['altitude'].data
        p_velocity_smooth = activity_stream['velocity_smooth'].data

        #Start to fill the known fields:
        blueprint_session["timestamp"] = [activity_start_time + i for i in p_time]
        blueprint_session["longitude"] = [i[1] for i in p_latlng]
        blueprint_session["latitude"] = [i[0] for i in p_latlng]
        blueprint_session["altitude"] = p_altitude
        blueprint_session["version"] = [None for i in range(len(p_time))]

        df_sel = pd.DataFrame(blueprint_session)
        obj_gps_defintion = list(df_sel.columns)

        # Create leaf configuration:
        leaf_config = self.dbh.create_leaf_config(leaf_name="gps",
                                                  track_hash=hash_str,
                                                  columns=obj_gps_defintion)

        #Write the first leaf:
        r = self.dbh.write_leaf(track_hash=hash_str,
                                leaf_config=leaf_config,
                                leaf=df_sel,
                                leaf_type="DataFrame"
                                )
        if r is True:
            print("GPS leaf written")
            del df_sel

        return r


    def _handle_activity_from__strava_api(self, activity=None):
        """
        This member function is strictly used to handle all necessary
        steps to reshape and enrich the activity object from the Strava API
        into a track/branch.
        This is a strict operation and needs to align with all other inputs
        in order to not violate the rules of the individual branches which
        describe a track.
        Leaves are allowed to vary! - But this is handled afterwards
        :param activity:
        :return:
        """

        # Load the blueprint:
        blueprint_session = self.bp.get_branch_blueprint("1")

        # Extract the descriptive parameters first:
        timezone_offset = (activity.start_date_local - activity.start_date.replace(tzinfo=None)).total_seconds()
        activity_start_time = activity.start_date.replace(tzinfo=None)
        activity_end_time = (activity_start_time + activity.elapsed_time).replace(tzinfo=None)

        tm = TypeMapper()
        tm.set_track_source(track_source="strava")
        tm.loader()


        blueprint_session["source"] = "StravaApi"
        blueprint_session["start_time"] = int(datetime.datetime.timestamp(activity_start_time) * 1000)
        blueprint_session["end_time"] = int(datetime.datetime.timestamp(activity_end_time) * 1000)
        blueprint_session["created_at"] = int(datetime.datetime.timestamp(activity_start_time) * 1000)
        blueprint_session["updated_at"] = int(datetime.datetime.timestamp(activity_end_time) * 1000)
        blueprint_session["title"] = activity.name
        blueprint_session["notes"] = activity.description
        blueprint_session["start_time_timezone_offset"] = int(timezone_offset) * 1000
        blueprint_session["end_time_timezone_offset"] = int(timezone_offset) * 1000
        blueprint_session["sports_type"] = tm.mapper(activity.type)  # preliminary

        # We add a track_hash to each track to make it unique:
        hash_str = f"{blueprint_session.get('start_time')}{blueprint_session.get('end_time')}"
        hash_str = hashlib.md5(hash_str.encode("utf-8")).hexdigest()[0:8]
        blueprint_session["track_hash"] = hash_str

        # We add the user specific hash to the track/branch for identification:
        blueprint_session["user_hash"] = self.db_dict.get("db_hash")

        ##WE INTRODUCE THIS LATER #ToDo
        # blueprint_ok = self.bp.check_blueprint(blueprint_session)
        # if blueprint_ok is False:
        #     print("Blueprint is not filled out correctly:")
        #     for k, v in blueprint_session.items():
        #         print(f"{k}  --  {v}")
        #     exit()

        write_success = self.dbh.write_branch(db_operation="update",
                                              track=blueprint_session,
                                              track_hash=hash_str)

        del tm
        return write_success, hash_str

    def load_gps(self):
        """
        This member function reads in a GPX file from Strava
        - Takes track name
        :return:
        """

        #We read the gps file first which was set ahead
        self._read_json(self.gps_file)

        #We read a blueprint to be able to version the gps coordidantes
        blueprint_session = self.bp.get_leaf_blueprint(leaf_type="positions",
                                                       version="1")
        blueprint_session["latitude"] = []
        blueprint_session["longitude"] = []
        blueprint_session["altitude"] = []
        blueprint_session["timestamp"] = []

        #Let's loop over the loaded GPX file and extract the infomation
        for track in self.gpx.tracks:
            self.track_name = track.name
            for segment in track.segments:
                for point in segment.points:
                    # print(point.latitude, point.longitude, point.elevation, point.time)
                    blueprint_session["latitude"].append(point.latitude)
                    blueprint_session["longitude"].append(point.longitude)
                    blueprint_session["altitude"].append(point.elevation)
                    blueprint_session["timestamp"].append(point.time)

        #Make a DataFrame for transform:
        self.df = pd.DataFrame(blueprint_session)
        self.df["timestamp"] = pd.to_datetime(self.df["timestamp"])  # Get rid of +Z
        self.df["timestamp"] = pd.DatetimeIndex(self.df["timestamp"]).astype(np.int64) / 1e9  # Convert it to seconds

    def _read_json(self, fjson):
        """
        A simple private member function for reading a json file
        :param fjson:
        :return:
        """
        gpx_file = open(self.gps_file, 'r')
        tree = ET.parse(self.gps_file)
        root = tree.getroot()
        self.gpx = gpxpy.parse(gpx_file)
