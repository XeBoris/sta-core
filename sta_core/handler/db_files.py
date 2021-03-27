import os
import uuid
import json
import hashlib
import datetime
import pandas as pd
import pandas.io.common #get pandas exceptions
from tinydb import TinyDB, Query, where
from shutil import copyfile, move
from tinydb.operations import delete


class FileDataBase(object):
    """

    """

    def __init__(self):
        self._db_path = None
        self._db_name = None

    def __del__(self):
        pass

    def set_db_path(self, db_path):
        self._db_path = db_path

    def set_db_name(self, db_name):
        self._db_name = db_name

    def _setup(self):
        """
        Setup the FileDataBase class with pre-settings
        :return:
        """
        if self._db_path is None:
            self._db_path = os.path.join(os.path.expanduser("~"), "STA")
        elif os.path.expanduser("~") not in self._db_path:
            self._db_path = os.path.join(os.path.expanduser("~"), self._db_path)

        if self._db_name is None:
            self._db_name = "db0"

        # create the individual database names from the names
        self._db_name_final = f"db-{self._db_name}.tiny"
        self._db_table_users = f"db_{self._db_name}_users"
        self._db_table_tracks = f"db_{self._db_name}_branches"

    def create_db_user(self):
        self._setup()

        print("DBFile:CreateDbUser")

        # create folder which contains the database if not existing:
        if os.path.exists(self._db_path) is False:
            os.mkdir(self._db_path)
            print("Data location successfully created")
            print(" - ", self._db_path)
        else:
            print("Data location exists already")
            print(" - ", self._db_path)

        if self._test_db_table_exists(self._db_table_users) is False:
            db = TinyDB(os.path.join(self._db_path, self._db_name_final))
            tb = db.table(self._db_table_users)
            tb.insert({"init": {"version": 1, "table": self._db_table_users}})
            db.close()
            print(f"TinyDB database {self._db_name_final} created wit table {self._db_table_users}")
        else:
            print(f"TinyDB table {self._db_table_users} exists already")

    def create_db_tracks(self):
        self._setup()

        print("DBFile:CreateDbTracks")

        # create folder which contains the database if not existing:
        if os.path.exists(self._db_path) is False:
            os.mkdir(self._db_path)
            print("Data location successfully created")
            print(" - ", self._db_path)
        else:
            print("Data location exists already")
            print(" - ", self._db_path)

        if self._test_db_table_exists(self._db_table_tracks) is False:
            db = TinyDB(os.path.join(self._db_path, self._db_name_final))
            tb = db.table(self._db_table_tracks)
            tb.insert({"init": {"version": 1, "table": self._db_table_tracks}})
            db.close()
            print(f"TinyDB database {self._db_name_final} created wit table {self._db_table_tracks}")
        else:
            print(f"TinyDB table {self._db_table_tracks} exists already")

    def _test_db_exists(self):
        """
        Private member function
        :return:
        """
        exists = False
        if os.path.exists(os.path.join(self._db_path, self._db_name_final)):
            exists = True
        return exists

    def _test_db_table_exists(self, tablename):
        """
        private member function
        :param tablename:
        :return:
        """
        exists = False
        if self._test_db_exists():
            db = TinyDB(os.path.join(self._db_path, self._db_name_final))
            all_tables = db.tables()
            if tablename in all_tables:
                exists = True
            db.close()
        return exists

    def get_database_exists(self):
        self._setup()
        return self._test_db_exists()

    def get_database_tables_exists(self):
        """
        Check if all required tables are created during initial creation process
        :return:
        """
        self._setup()

        no_missing_table = True
        for i_table in [self._db_table_users, self._db_table_tracks]:
            table_exists = self._test_db_table_exists(i_table)
            if table_exists is False:
                no_missing_table = False
                break
        return no_missing_table

    def create_user(self, init_user_dictionary=None):
        self._setup()

        hash_str = "{surname}{lastname}{birthday}".format(surname=init_user_dictionary.get("user_surname"),
                                                          lastname=init_user_dictionary.get("user_lastname"),
                                                          birthday=init_user_dictionary.get("user_birthday"))
        md5_hash = hashlib.md5(hash_str.encode("utf-8")).hexdigest()[0:8]
        uuid_hash = str(uuid.uuid4()).split("-")[0]
        init_user_dictionary['user_hash'] = f"{md5_hash}{uuid_hash}"

        # ToDO Write some exceptions:
        db = TinyDB(os.path.join(self._db_path, self._db_name_final))
        db.default_table_name = self._db_table_users
        db.insert(init_user_dictionary)
        db.close()

    def search_user(self, user, by="username"):
        self._setup()

        # ToDO Write some exceptions:
        # db = TinyDB(os.path.join(self._db_path, self._db_name_final))
        # db.default_table_name = self._db_table_users

        self._open_tiny_db()
        self.db.default_table_name = self._db_table_users

        User = Query()
        p = []
        if by == "username":
            p = self.db.search(self.user["user_username"] == user)
        elif by == "surname":
            p = self.db.search(self.user["user_surname"] == user)
        elif by == "lastname":
            p = self.db.search(self.user["user_lastname"] == user)
        elif by == "hash":
            p = self.db.search(self.user["user_hash"] == user)

        # db.close()
        self._close_tiny_db()
        return p

    def search_user_by_hash(self, hash=None):
        self._setup()
        f = open(os.path.join(self._db_path, self._db_user_name), "r")
        ret = None
        for i_user in f:
            i_obj = json.loads(i_user)
            if i_obj.get("user_hash") == hash:
                ret = i_obj
                break
        f.close()
        return ret

    def modify_user(self, user_hash, key, value, mode):
        """
        This member function is designed to serve a common purpose:

        db_dict = {"user_name": "name",
                  "strava": [{"client_id": 123, "client_secret": 123, "datetime": 12345}]}

        Top-level keys are e.g. 'user_name' or 'strava'

        Mode: append: key="new", value=something whereas something is a python dictionary
        db_dict = {"user_name": "name",
                   "strava": [{"client_id": 123, "client_secret": 123, "datetime": 12345}],
                   "new": [value] }
        Mode: append: key="strava", value=something whereas 'something' is a python dictionary
        db_dict = {"user_name": "name",
                   "strava": [{"client_id": 123, "client_secret": 123, "datetime": 12345},
                              {"client_id": 456, "client_secret": 456, "datetime": 45678}],
                     }
            --> Only updates the entry when there is a change in any element but 'datetime' :key

        :param user_hash:
        :param key:
        :param value:
        :param mode:
        :return:
        """
        #Open database for users
        self._open_tiny_db()
        self.db.default_table_name = self._db_table_users

        # Select the user by hash:
        db_entry = self.db.get(self.user["user_hash"] == user_hash)
        db_entry_id = db_entry.doc_id

        #Handle both modes: append & update:
        if mode == "append" and key in db_entry and isinstance(db_entry[key], list) is True:
            #We can append the requested element to the list
            value['datetime'] = datetime.datetime.timestamp(datetime.datetime.now())
            db_entry[key].append(value)

            #We assure that the new element does not yet exists yet in our list of dictionaries
            #As criterion we use ALL KEYS BUT 'datetime'
            db_entry_key = [i.copy() for i in db_entry[key]]
            p = []
            for i in db_entry_key:
                i.pop('datetime', None)
                p.append(i)
            contains_duplicates = any(p.count(element) > 1 for element in p)

            #once we know if we have doublicates:
            if contains_duplicates is False:
                print("Update User Database")
                self.db.update({key: db_entry[key]},
                               doc_ids=[db_entry_id])
            else:
                print("No need for an update!")

            del p
            del db_entry_key

        elif mode == "append" and key not in db_entry:
            #Appending a new a new element to a list is only allowed
            #if the related key does not yet exists in the top-level dictionary:
            value['datetime'] = datetime.datetime.timestamp(datetime.datetime.now())
            db_entry[key] = [value]
            self.db.update({key: db_entry[key]},
                           doc_ids=[db_entry_id])

        elif mode == "update":
            #Updating the top-level dictionary is always possible! This operation overwrites
            #the according key.
            db_entry[key] = value
            self.db.update({key: db_entry[key]},
                            doc_ids=[db_entry_id])

        else:
            self._close_tiny_db()
            return False

        self._close_tiny_db()


    def mod_user_by_hash(self, hash, key, value, date_obj):
        """
        Modify the user database - DO NOT USE THIS IN REAL LIFE FOR NOW.
        :param key:
        :param value:
        :return:
        """

        self._open_tiny_db()
        self.db.default_table_name = self._db_table_users

        #Select the user by hash:

        db_entry = self.db.get(self.user["user_hash"] == hash)
        db_entry_id = db_entry.doc_id

        value = json.loads(value)

        db_value = []
        if key not in db_entry:
            value['datetime'] = datetime.datetime.timestamp(datetime.datetime.now())
            db_value = [value]
        else:
            #fill with old information:
            db_value.extend(db_entry[key])

            #prepare new values to be added:
            value['datetime'] = datetime.datetime.timestamp(datetime.datetime.now())
            db_value.append(value)

        #This section decides if you update the database or not:
        if key == 'strava':
            #We count how often THE SAME client_secret is seen.
            #Only if there is a change, write back to database:
            p = [i.get("client_secret") for i in db_value]
            contains_duplicates = any(p.count(element) > 1 for element in p)

            if contains_duplicates is False:
                print("Update User Database")
                self.db.update({key: db_value},
                               doc_ids=[db_entry_id])
            else:
                print("No need for an update!")

        self._close_tiny_db()

        return 0


    def list_user_by_hash(self, user_hash):
        """

        :param user_hash:
        :return:
        """

        self._open_tiny_db()
        self.db.default_table_name = self._db_table_users

        #Select the user by hash:
        db_entry = self.db.get(self.user["user_hash"] == user_hash)
        self._close_tiny_db()

        return db_entry

    # This part handles write/read operations on the tracks database:
    #  - Tracks are like branches of a tree
    def _open_tiny_db(self):
        self._setup()
        self.db = TinyDB(os.path.join(self._db_path, self._db_name_final))
        self.db.default_table_name = self._db_table_tracks  # <- tracks are branches!
        self.user = Query()

    def _close_tiny_db(self):
        self.db.close()

    def write_branch(self,
                     db_operation="new",
                     track=None,
                     track_hash=None
                     ):
        """
        A track has is a branch. And each branch will have leaves!
        To write a branch/track with write_branch(..) you can create a "new" branch or
        "update" an existing branch with this function.
        :param db_operation:
        :param track:
        :return:
        """
        # Open first the track/branch database
        self._open_tiny_db()

        # Start the write/update process:
        if db_operation == "new" and track_hash is not None and track is not None:
            # Look first for the existing hash:
            find_hash = self.db.get(self.user["track_hash"] == track_hash)

            # Insert a new track if the hash does not exists yet:
            if find_hash is None:
                self.db.insert(track)
            else:
                print(f"No new entry possible. A track hash {track_hash} exists already in DB")

        elif db_operation == "update" and track_hash is not None and track is not None:
            # Look first for the existing hash:
            find_hash = self.db.get(self.user["track_hash"] == track_hash)

            if find_hash is None:
                # This if conditions behaves like the "new" option and
                # insert the track to the database:
                self.db.insert(track)
            elif find_hash is not None:
                # Update dictionary with new track information:
                find_hash.update(track)
                # Get hash ID from database for update procedure
                find_hash_id = find_hash.doc_id
                # Update a branch if existing!
                self.db.update(find_hash, doc_ids=[find_hash_id])
            else:
                print("do something else")

        else:
            print("You are trying to handling an unknown database operation!")

        # Close database:
        self._close_tiny_db()

        # If you make to hear, return True
        return True

    def read_branch(self, key=None, attribute=None):
        self._open_tiny_db()
        db_entry = self.db.search(self.user[key] == attribute)
        self._close_tiny_db()
        return db_entry

    def search_branch(self, key=None, attribute=None, how=None):
        self._open_tiny_db()
        if isinstance(attribute, list) and how == "between":
            db_entry = self.db.search((self.user[key] >= attribute[0]) & (self.user[key] <= attribute[1]))
        self._close_tiny_db()
        return db_entry

    def delete_branch(self, key=None, attribute=None):
        self._open_tiny_db()
        db_entry = self.db.get(self.user[key] == attribute)
        delbranch = False
        if db_entry.doc_id is not None:
            try:
                self.db.remove(where(key) == attribute)
                delbranch = True
            except:
                print("Branch could not be deleted.")

        self._close_tiny_db()
        return delbranch

    # This part handles write/read operation on metadata
    #  - metadata to tracks are like leaves which belong to branch

    def write_leaf(self,
                   track_hash=None,
                   leaf_config=None,
                   leaf=None,
                   leaf_type=None
                   ):
        """
        Writing a leaf consist of two operations:
        1) Write the leaf
        2) Adjust the track/branch record

        :param track_hash:
        :param leaf_config:
        :param leaf:
        :param leaf_type:
        :return:
        """
        # leaf hash:
        leaf_hash = leaf_config.get("leaf_hash")

        # Prepare the file based path for storage:
        storage_path = os.path.join(self._db_path, leaf_config.get("name"))

        # If storage directory not exists, create it:
        if os.path.exists(storage_path) is False:
            os.makedirs(storage_path)

        # Open the branch/track database:
        self._open_tiny_db()

        # Get the according branch/track from the database:
        find_hash = self.db.get(self.user["track_hash"] == track_hash)
        if find_hash is None:
            # If hash is not found, return False
            return False

        # Continue with leaf writing:
        find_hash_id = find_hash.doc_id

        # We start to write/update according to the chosen method:
        if leaf_type == "DataFrame":
            # Write the leaf to disk:
            try:
                leaf_storage = os.path.join(storage_path, f"{leaf_hash}.csv")
                leaf.to_csv(path_or_buf=leaf_storage,
                            index=False,
                            # compression="gzip",
                            )
            except:
                print("Something went wrong to write the Pandas DataFrame to disk")
                return False

            # Update the leaf in the track/branch database
            if 'leaf' not in find_hash:
                db_entry = {}
            else:
                db_entry = find_hash.get("leaf")

            db_entry[leaf_config['name']] = leaf_config
            self.db.update({'leaf': db_entry}, doc_ids=[find_hash_id])

        elif leaf_type == "ConfigWrite":
            # Update the leaf in the track/branch database
            if 'leaf' not in find_hash:
                db_entry = {}
            else:
                db_entry = find_hash.get("leaf")

            db_entry[leaf_config['name']] = leaf_config
            self.db.update({'leaf': db_entry}, doc_ids=[find_hash_id])

        # Close the database:
        self._close_tiny_db()

        # If you make it to here, return true
        return True

    def read_leaf(self,
                  directory=None,
                  leaf_hash=None,
                  leaf_type=None
                  ):

        # not used,... maybe later
        # self._open_tiny_db()
        # self._close_tiny_db()

        # create the data path:
        data_path = os.path.join(self._db_path, directory, f"{leaf_hash}.csv")

        df = None

        if leaf_type == "DataFrame" and os.path.exists(data_path) is True:
            try:
                df = pd.read_csv(data_path)
            except pandas.io.common.EmptyDataError as e:
                print(f"{e} found {data_path}")
                df = None
        elif leaf_type == "something" and os.path.exists(data_path) is True:
            df = None
        else:
            df = None

        return df

    def delete_leaf(self,
                    leaf_name=None,
                    track_hash=None,
                    ):

        def list_files(path):
            f = []
            for (dirpath, dirnames, filenames) in os.walk(path):
                f.extend(filenames)
                break
            return f

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

        print("delete:")
        self._open_tiny_db()

        # Get the according branch/track from the database:
        find_hash = self.db.get(self.user["track_hash"] == track_hash)
        print("h:", find_hash)
        if find_hash is None:
            # If hash is not found, return False
            return False
        find_hash_id = find_hash.doc_id

        # Update track by removing the leaf (decouple from database)

        # Remove leaf on disk
        if find_hash.get("leaf") is None:
            return False
        if leaf_name not in find_hash.get("leaf"):
            return False

        leaf_hash = find_hash.get("leaf").get(leaf_name).get("leaf_hash")
        leaf_to_modify = find_hash.get("leaf")
        print(leaf_hash, leaf_to_modify)

        all_leaves = list_files(os.path.join(self._db_path, leaf_name))
        leaf_file = [i for i in all_leaves if i.find(leaf_hash) >= 0]
        leaf_file = os.path.join(self._db_path, leaf_name, leaf_file[0])
        print(leaf_file)

        del_file_appr = del_path(leaf_file, backup=False)
        if del_file_appr is True:

            del leaf_to_modify[leaf_name]
            try:
                self.db.update({'leaf': leaf_to_modify}, doc_ids=[find_hash_id])
            except:
                print("Database entry could not get updated - skip")
                return False

        self._close_tiny_db()

        # If you make it to hear, return True
        return True

    # High level functions for sort cuts. You are only allowed to use functions within this class!

    def get_all_leaves_for_track(self, track_hash=None):
        """

        :param track_hash:
        :return:
        """
        try:
            branch = self.read_branch(key="track_hash",
                                      attribute=track_hash)

            branch_leaves = branch[0]
        except:
            return []

        try:
            return branch_leaves.get("leaf")
        except:
            return []

    def get_all_users(self, by=None):
        """

        :return:
        """

        # ToDO Write some exceptions:
        self._open_tiny_db()
        self.db.default_table_name = self._db_table_users
        result = [r.get(by) for r in self.db]
        self._close_tiny_db()

        return result
