import os
import json
import datetime
import hashlib
import pandas as pd

from sta_core.handler.db_handler import DataBaseHandler
from sta_core.handler.shelve_handler import ShelveHandler

from .blueprint import Blueprint
from .type_mapper import TypeMapper


class Runtastic():

    def __init__(self):
        self.input_type = None
        self.path = None
        self.core_info = None

        self.path_photos = None
        self.path_purchases = None
        self.path_routes = None
        self.path_sessions = None
        self.path_user = None
        self.path_weight = None

        # Data base handlers
        self.dbh = None

        #Init second classes:
        self.bp = Blueprint() #Blueprints

        self.tm = TypeMapper()
        self.tm.set_track_source(track_source="runtastic")
        self.tm.loader()

    def __del__(self):
        del self.bp
        del self.tm

    def _init_database_handler(self):
        """

        :return:
        """
        if self.core_info.get("db_hash") is None:
            print("The core_info object does not contain an unique user hash")
            print("We can not write data to the database without knowing who")
            print("whom the data belong.")
            print("Check!")
            exit()

        self.dbh = DataBaseHandler(db_type=self.core_info["db_type"])
        self.dbh.set_db_path(db_path=self.core_info["db_path"])
        self.dbh.set_db_name(db_name=self.core_info["db_name"])

    def _close_database_handler(self):
        del self.dbh

    def _read_json(self, fjson):
        """
        A simple private member function for reading a json file
        :param fjson:
        :return:
        """
        with open(fjson) as f:
            data = json.load(f)
        f.close()
        return data

    def _get_all_sport_session_ids(self):
        """
        Read all your sport sessions from the available database dump.
        Assume that the <UUID>.json file structure holds for unique
        files. 1 UUID == 1 file == 1 sports activity
        """

        session_ids = []
        for (dirpath, dirnames, filenames) in os.walk(self.path_sessions):
            session_ids.extend(filenames)
            break

        session_ids = [i.replace(".json", "") for i in session_ids]
        return session_ids

    def configure_core(self, core_info):
        """
        Core configuration is meant to setup the database handler of the sta-core
        for the third parity application "Runtastic" in order to write the gained
        information to the right database setup.
        A core configuration consists of a dictionary with four major entries:
        - db_path -> The path to the database
        - db_name -> The name of database
        - db_type -> The type of the database
        - db_hash -> The unique hash of an existing user in that database.

        :param core_info: dict
        :return:
        """
        self.core_info = core_info

    def setup_path(self, type=None, path=None):
        self.input_type = type
        self.path = path
        if type == "database":
            self.path_photos = os.path.join(self.path, "Photos")
            self.path_purchases = os.path.join(self.path, "Purchases")
            self.path_routes = os.path.join(self.path, "Routes")
            self.path_sessions = os.path.join(self.path, "Sport-sessions")
            self.path_user = os.path.join(self.path, "User")
            self.path_weight = os.path.join(self.path, "Weight")

    def get_session_Ids(self):

        if self.input_type == "database":
            return self._get_all_sport_session_ids()

    def _get_rt_db_track_info(self, session_id):
        """
        Get Runtastic Database Track Information

        :param session_id:
        :return:
        """
        pass

    def _read_session_by_id_from_database(self, session_id):
        """
        We will read the Runtastic database dump in

        :param session_id:
        :return:
        """
        # create temporally session path:
        json_path_info = os.path.join(self.path_sessions, f"{session_id}.json")
        json_path_gps = os.path.join(self.path_sessions, "GPS-data", f"{session_id}.json")
        json_path_elv = os.path.join(self.path_sessions, "Elevation-data", f"{session_id}.json")

        # read session info:
        json_info = self._read_json(json_path_info)

        # We will receive meta data from RunTastic First:
        json_info_meta = self.bp.runtastic_metadata(json_info)

        # We extract timestamps and timestamp names
        dtime = datetime.datetime.utcfromtimestamp(json_info["start_time"] / 1000).strftime('%Y-%m-%dT%H:%M:%SZ')
        dtime_name = datetime.datetime.utcfromtimestamp(json_info["start_time"] / 1000).strftime('%Y-%m-%d-%H-%M')

        # We will receive the main Runtastic information about the track
        json_info["sports_type"] = self.tm.mapper(json_info.get("sport_type_id"))


        json_info = self.bp.runtastic_session(json_info)

        # We will receive the track (gpx) related information about the track
        # This needs two steps:
        # 1) Read and transform the json objects from database dump (if existing)
        # 2) Merge them into one object with all "raw" information what is available
        # - step 1)
        if os.path.exists(json_path_gps):
            json_gps = self._read_json(json_path_gps)
            data_gps = self.bp.runtastic_session_lonlat(json_gps)
        else:
            data_gps = {}

        if os.path.exists(json_path_elv):
            json_ele = self._read_json(json_path_elv)
            data_ele = self.bp.runtastic_session_elevation(json_ele)
        else:
            data_ele = {}
        # - step 2:
        data_gpx_final = []
        if len(data_gps) > 0 and len(data_ele) > 0:
            for key, val in data_gps.items():
                ele = data_ele.get(key)
                if ele is not None:
                    ret = {**val, **ele}
                    ret["timestamp"] = key
                else:
                    ret = val
                    ret["timestamp"] = key

                # adjust timestamp (Move to UTC):
                ret["timestamp"] = datetime.datetime.strptime(ret["timestamp"], '%Y-%m-%d %H:%M:%S %z').timestamp()

                data_gpx_final.append(ret)
        elif len(data_gps) > 0 and len(data_ele) == 0:
            for key, val in data_gps.items():
                ret = val
                ret["timestamp"] = key

                # adjust timestamp (Move to UTC):
                ret["timestamp"] = datetime.datetime.strptime(ret["timestamp"], '%Y-%m-%d %H:%M:%S %z').timestamp()

                data_gpx_final.append(ret)
        elif len(data_gps) == 0 and len(data_ele) == 0:
            pass

        # data_gpx_final #final gpx object
        return {"timestamp": dtime,
                "timestampName": dtime_name,
                "json_info": json_info,
                "json_info_meta": json_info_meta,
                "gpx": data_gpx_final}

    def import_runtastic_sessions(self, overwrite=False):
        """
        You can always import sessions based on the source of runtastic
        Nevertheless, the type is important of how import the sessions

        :return:
        """
        self._init_database_handler()

        #extract the user hash from the core_info dictionary
        user_hash = self.core_info.get("db_hash")


        if self.input_type == "database":
            # This if conditions handles the runtastic database
            # dump as input only:

            # Get all session IDs first:
            all_session_ids = self._get_all_sport_session_ids()

            for session_id in all_session_ids:
                # Extract runtastic relevant data from the database dump
                # and fetch the information from the return object, which is
                # a json object:
                rt_obj = self._read_session_by_id_from_database(session_id)

                # We will handle now several leafs:
                # ---------------------------------

                # We create the branch first from the database dump:
                rt_json = rt_obj.get("json_info")

                #use the branch to write print outputs for now:
                print(f"Write: {rt_json.get('title')} of sports type {rt_json.get('sports_type')} into DB")

                # We add a track_hash to each track to make it unique:
                hash_str = f"{rt_json.get('start_time')}{rt_json.get('end_time')}"
                hash_str = hashlib.md5(hash_str.encode("utf-8")).hexdigest()[0:8]
                rt_json["track_hash"] = hash_str

                # We add the user specific hash to the track/branch for identification:
                rt_json["user_hash"] = user_hash

                self.dbh.write_branch(db_operation="update",
                                 track=rt_json,
                                 track_hash=hash_str)

                # Prepare to fill leafs:
                if len(rt_obj.get("gpx")) == 0:
                    continue
                df = pd.DataFrame.from_dict(rt_obj.get("gpx"))

                # GPS LEAF:
                # We create a branch which holds only gps relevant information:
                # gps relevant infomation:
                # HINT:
                # - These are also defined in blueprint.py for the GPS leaf!
                # - Don't add/remove here something what is not in line with it.
                obj_gps_defintion = ["timestamp", "longitude", "latitude",
                                     "altitude", "accuracy_v", "accuracy_h",
                                     "version"]
                df_sel = df[df.columns & obj_gps_defintion]

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

                # SECOND LEAF:
                # We create a branch which holds only gps relevant information:
                # gps relevant information:
                # HINT:
                # - These are also defined in blueprint.py for the GPS leaf!
                # - Don't add/remove here something what is not in line with it.
                obj_gps_defintion = ["timestamp", "speed", "duration",
                                     "distance", "elevation_gain", "elevation_loss",
                                     "elevation", "version"]

                # Select from dataframe:
                df_sel = df[df.columns & obj_gps_defintion]

                # Create leaf configuration:
                leaf_config = self.dbh.create_leaf_config(leaf_name="runtastic_distances",
                                                     track_hash=hash_str,
                                                     columns=obj_gps_defintion)

                # Write the second leaf:
                r = self.dbh.write_leaf(track_hash=hash_str,
                                   leaf_config=leaf_config,
                                   leaf=df_sel,
                                   leaf_type="DataFrame"
                                   )
                if r is True:
                    print("Runtastic distance leaf written")
                    del df_sel

                # Third Leaf: Meta data information:
                # ----------------------------------
                # Extract the json object and put it into a list for the Pandas dataframe:
                rt_metadata = [rt_obj.get("json_info_meta")]
                df_metadata = pd.DataFrame.from_dict(rt_metadata)

                # We do not extract sub information, so take all columns for the object definition
                obj_definition = list(df_metadata.keys())

                # Create leaf configuration:
                leaf_config = self.dbh.create_leaf_config(leaf_name="runtastic_metadata",
                                                     track_hash=hash_str,
                                                     columns=obj_definition)

                # Write the second leaf:
                r = self.dbh.write_leaf(track_hash=hash_str,
                                   leaf_config=leaf_config,
                                   leaf=df_metadata,
                                   leaf_type="DataFrame"
                                   )
                if r is True:
                    print("Runtastic metadata leaf written")
                    del df_metadata

        #Close the database handler
        self._close_database_handler()
