import sys
import os
import datetime
from shutil import copyfile, move
import shelve
from stravalib import Client


from .handler.db_handler import DataBaseHandler
from .handler.shelve_handler import ShelveHandler

from .tracker.runtastic import Runtastic
from .tracker.strava import Strava

def create_db(db_type=None,
              db_name=None,
              db_path=None):
    """

    :param db_type:
    :param db_name:
    :param db_path:
    :return:
    """

    dbh = DataBaseHandler(db_type=db_type)
    dbh.set_db_path(db_path=db_path)
    dbh.set_db_name(db_name=db_name)

    dbh.create_db_user()
    dbh.create_db_tracks()

    del dbh

def load_db(db_type=None,
            db_name=None,
            db_path=None):
    """

    :param db_type:
    :param db_name:
    :param db_path:
    :return:
    """
    dbh = DataBaseHandler(db_type=db_type)
    dbh.set_db_path(db_path=db_path)
    dbh.set_db_name(db_name=db_name)

    db_file_exists = dbh.get_database_exists()
    db_tables_exists = dbh.get_database_tables_exists()

    if db_file_exists is True and db_tables_exists is True:
        db = {'db_name': db_name, 'db_type': db_type, 'db_path': db_path}
        print(db)

        db_temp = ShelveHandler()
        db_temp.write_shelve(db)

    del dbh

def set_user(db_user=None):
    """
    This function allows to overwrite/set a pre-defined user to which new records/tracks
    are added.
    :param db_user:
    :return:
    """

    db_temp = ShelveHandler()
    db_dict = db_temp.read_shelve_by_keys(["db_name", "db_type", "db_path"])


    dbh = DataBaseHandler(db_type=db_dict["db_type"])
    dbh.set_db_path(db_path=db_dict["db_path"])
    dbh.set_db_name(db_name=db_dict["db_name"])

    # test if requested user is part of the database
    search_result = dbh.search_user(db_user, by="username")
    nb_search_result = len(search_result)

    if len(search_result) == 1:
        # Update Shelve:
        db = {"db_user": search_result[0].get("user_username"),
              "db_hash": search_result[0].get("user_hash")}

        db_temp.write_shelve(db)
        print(f"Assume to add tracks for user: {db_user}")
    elif len(search_result) == 0:
        print(f"Username {db_user} is not found in current user database.")
        print("Add user first...")
    else:
        print("We have found multiple user ids. Please select the one you are referring to:")
        for k, db_entry in enumerate(search_result):
            p = "[{k}] | Name: {user_surname} {user_lastname} | Username: {user_username}".format(k=k,
                                                                                                  user_surname=db_entry.get(
                                                                                                      "user_surname"),
                                                                                                  user_lastname=db_entry.get(
                                                                                                      "user_lastname"),
                                                                                                  user_username=db_entry.get(
                                                                                                      "user_username")
                                                                                                  )
            print(p)
        print("Choose a number:")
        selected_hash = input(f"Select number 0 to {nb_search_result - 1}: ")
        selected_hash = search_result[int(selected_hash)]

        # Update Shelve:
        db = {"db_user": selected_hash.get("user_username"),
              "db_hash": selected_hash.get("user_hash")}

        db_temp.write_shelve(db)

    del db_temp

def list_shelve(shelve_key=None):
    """
    This simple actions allows handles entries in the shelve which
    is used by CLI users.

    :return:
    """
    db_temp = ShelveHandler()
    if shelve_key == "all-keys":
        all_keys = db_temp.get_all_shelve_keys()
        del db_temp
        return all_keys
    elif shelve_key=="key-values":
        key_values = db_temp.read_shelve_by_keys(db_temp.get_all_shelve_keys())
        del db_temp
        return key_values
    elif shelve_key == "shelve-path":
        path = db_temp.get_shelve_path()
        del db_temp
        return path
    else:
        pass

def list_user():
    db_temp = ShelveHandler()
    db_dict = db_temp.read_shelve_by_keys(["db_name",
                                           "db_type",
                                           "db_path",
                                           "db_user",
                                           "db_hash"])


    dbh = DataBaseHandler(db_type=db_dict["db_type"])
    dbh.set_db_path(db_path=db_dict["db_path"])
    dbh.set_db_name(db_name=db_dict["db_name"])

    db_entry = dbh.list_user_by_hash(db_dict["db_hash"])

    print(f"DB User Overview: {db_entry['user_surname']} {db_entry['user_lastname']}:")
    for i_key in db_entry.keys():
        print(f" [{i_key}] \t {db_entry[i_key]}")

    del db_temp

def mod_user(key, value, date):
    print("ToDo: OUTDATED: mod_user_by_hash()! NOthing will happen here.")
    db_temp = ShelveHandler()
    db_dict = db_temp.read_shelve_by_keys(["db_name",
                                           "db_type",
                                           "db_path",
                                           "db_user",
                                           "db_hash"])

    dbh = DataBaseHandler(db_type=db_dict["db_type"])
    dbh.set_db_path(db_path=db_dict["db_path"])
    dbh.set_db_name(db_name=db_dict["db_name"])

    #ToDo: mod_user_by_hash is outdated! Use modify_user
    #dbh.mod_user_by_hash(db_dict["db_hash"], key, value, date)

def add_tracks(core_information,
               track_source, source_type, input_path,
               overwrite, date_obj):


    if track_source is None or source_type is None:
        print("You did not specify source-type or track-source")
        exit()
    elif track_source == "runtastic" and source_type == "database":
        # This if/else condition is supposed to import a runtastic
        # database dump into the database

        # A database dump needs a path from where it is imported:
        if input_path is None:
            print("To import a RUNTASTIC database dump from a path")
            print("or *gz file, you need to specify the path by handing it over")
            print("to your command line call: --path /path/to/source")
            exit()

        rt = Runtastic()
        rt.configure_core(core_information)
        rt.setup_path(type=source_type,
                      path=input_path)


        p = rt.get_session_Ids()

        rt.import_runtastic_sessions(overwrite=overwrite)

    elif track_source == "strava" and source_type == "gps":
        st = Strava()

        if input_path is None:
            print("To import Strava gps files")

        # test if args.path is file or path and determine what to do:
        if os.path.isfile(input_path):
            st.set_gps_file(gps_file=input_path)
            st.import_strava_gpx()
        elif os.path.isdir(input_path):
            st.set_gps_path(gps_path=input_path)
            st.import_strava_gpx_from_path()

    elif track_source == "strava" and source_type == "api":
        st = Strava()
        st.configure_core(core_information)

        sync_date = date_obj  # extract datetimes to sync
        st.set_activity_dates(sync_date)
        st.import_strava_api()


def find_tracks(track_source, source_type, date):
    """

    :return:
    """

    def evaluate_date(date):
        # prepare the date:
        if "-" in date:
            date_beg = date.split("-")[0]
            date_end = date.split("-")[1]
        elif "-" not in date and "T" not in date:
            date_beg = datetime.datetime.strptime(date, '%Y%m%d')
            date_end = date_beg + datetime.timedelta(hours=24)

        if "T" in date_beg:
            date_beg = datetime.datetime.strptime(date_beg, '%Y%m%dT%H%M')
        else:
            date_beg = datetime.datetime.strptime(date_beg, '%Y%m%d')
        if "T" in date_end:
            date_end = datetime.datetime.strptime(date_end, '%Y%m%dT%H%M')
        else:
            date_end = datetime.datetime.strptime(date_end, '%Y%m%d')

        date_beg = date_beg.timestamp() * 1000
        date_end = date_end.timestamp() * 1000
        return date_beg, date_end

    if date is not None:
        date_beg, date_end = evaluate_date(date)
    else:
        print("You are missing a proper --date <input>")
        exit()
    db_temp = ShelveHandler()
    db_dict = db_temp.read_shelve_by_keys(["db_name",
                                           "db_type",
                                           "db_path",
                                           "db_user",
                                           "db_hash"])

    dbh = DataBaseHandler(db_type=db_dict["db_type"])
    dbh.set_db_path(db_path=db_dict["db_path"])
    dbh.set_db_name(db_name=db_dict["db_name"])

    if date is not None:
        branches = dbh.search_branch(key="start_time", attribute=[date_beg, date_end], how="between")

    for i_branch in branches:
        tr_offset_beg = int(i_branch.get("start_time_timezone_offset")) / 1000
        tr_offset_end = int(i_branch.get("end_time_timezone_offset")) / 1000
        tr_beg = datetime.datetime.utcfromtimestamp(i_branch.get("start_time") / 1000 + tr_offset_beg)
        tr_end = datetime.datetime.utcfromtimestamp(i_branch.get("end_time") / 1000 + tr_offset_end)
        tr_hash = i_branch.get("track_hash")
        tr_title = i_branch.get("title")
        tr_leaves = i_branch.get("leaf")

        cmd = f"Track {tr_hash}: {tr_beg} o {tr_end} | {tr_title}"
        print(cmd)

        if tr_leaves is not None:
            for key, i_leaf in tr_leaves.items():
                cmd1 = f"  - Leaf {key} - {i_leaf.get('leaf_hash')} - {i_leaf.get('status')}"
                print(cmd1)
        else:
            print("No leaves!")


def remove_tracks(track_hash):
    def list_files(path):
        f = []
        for (dirpath, dirnames, filenames) in os.walk(path):
            f.extend(filenames)
            break
        return f

    def get_leaf(path, leaf_name):
        all_leaves = list_files(path)
        all_leaves = [i for i in all_leaves if i.find(".temp") < 0]
        leaf = [i for i in all_leaves if leaf_name in i]
        return leaf

    def del_path(path, backup=False):

        if backup is True:
            path_temp = path + ".temp"
            copyfile(path, path_temp)

        if os.path.isfile(path) is False:
            return False
        try:
            os.remove(path)
        except FileNotFoundError as e:
            return e

        return True

    def restore_file(path):
        path_temp = path + ".temp"
        move(path_temp, path)

    db_temp = ShelveHandler()
    db_dict = db_temp.read_shelve_by_keys(["db_name",
                                           "db_type",
                                           "db_path",
                                           "db_user",
                                           "db_hash"])

    db_path = db_dict["db_path"]

    dbh = DataBaseHandler(db_type=db_dict["db_type"])
    dbh.set_db_path(db_path=db_dict["db_path"])
    dbh.set_db_name(db_name=db_dict["db_name"])

    branch = None
    try:
        branch = dbh.read_branch(key="track_hash", attribute=track_hash)[0]
        print(f"Track hash {track_hash} found in database")
    except IndexError as e:
        print(f"Track hash {track_hash} not found in the database")
        exit()

    print("Start to remove leaves:")
    track_hash = branch.get("track_hash")
    all_leaves = branch.get("leaf")

    for i_leaf_name, obj in all_leaves.items():
        print(f"Leaf names: {i_leaf_name} / {obj.get('leaf_hash')}")

        leaf_hash = obj.get('leaf_hash')
        del_db_approval = dbh.delete_leaf(leaf_name=i_leaf_name,
                                          track_hash=track_hash)
        if del_db_approval is True:
            print(f"Leaf removed")
        else:
            print("Something went wrong while removing the leaf")
            exit()

    # Re-read the track and remove it under the condition of no leaves are in:
    branch = dbh.read_branch(key="track_hash", attribute=track_hash)[0]
    track_hash = branch.get("track_hash")
    if len(branch.get("leaf")) == 0:
        del_db_approval = dbh.delete_branch(key="track_hash", attribute=track_hash)
        if del_db_approval is True:
            print("Done!")


def remove_leaves(track_hash):
    def list_files(path):
        f = []
        for (dirpath, dirnames, filenames) in os.walk(path):
            f.extend(filenames)
            break
        return f

    def get_leaf(path, leaf_name):
        all_leaves = list_files(path)
        all_leaves = [i for i in all_leaves if i.find(".temp") < 0]
        leaf = [i for i in all_leaves if leaf_name in i]
        return leaf

    def del_path(path, backup=False):

        if backup is True:
            path_temp = path + ".temp"
            copyfile(path, path_temp)

        if os.path.isfile(path) is False:
            return False
        try:
            os.remove(path)
        except FileNotFoundError as e:
            return e

        return True

    def restore_file(path):
        path_temp = path + ".temp"
        move(path_temp, path)

    db_temp = ShelveHandler()
    db_dict = db_temp.read_shelve_by_keys(["db_name",
                                           "db_type",
                                           "db_path",
                                           "db_user",
                                           "db_hash"])

    db_path = db_dict["db_path"]

    dbh = DataBaseHandler(db_type=db_dict["db_type"])
    dbh.set_db_path(db_path=db_dict["db_path"])
    dbh.set_db_name(db_name=db_dict["db_name"])

    branch = None
    try:
        branch = dbh.read_branch(key="track_hash", attribute=track_hash)[0]
        print(f"Track hash {track_hash} found in database")
    except IndexError as e:
        print(f"Track hash {track_hash} not found in the database")
        exit()

    print("Select a leaf:")
    track_hash = branch.get("track_hash")
    all_leaves = branch.get("leaf")

    for i_leaf_name, obj in all_leaves.items():
        print(f"Leaf names: {i_leaf_name} / {obj.get('leaf_hash')}")

    input_leaf = input("Prompt a leaf name: ")
    if input_leaf == "c":
        print("nothing to remove")
        exit()

    del_db_approval = dbh.delete_leaf(leaf_name=input_leaf,
                                      track_hash=track_hash)
    if del_db_approval is True:
        print(f"Leaf removed")
    else:
        print("Something went wrong while removing the leaf")
    exit()
