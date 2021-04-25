import json
import hashlib
from .db_files import FileDataBase

class DataBaseHandler(FileDataBase):
    def __init__(self, db_type=None):
        self._db_type = db_type
        self._handler_type = None

        if self._db_type == "FileDataBase":
            self._handler_type = FileDataBase()
        else:
            self._handler_test()

    def __del__(self):
        del self._handler_type

    def _handler_test(self):
        if self._handler_type is None:
            print("You need to specify a database backend handler")
            print("Choose:")
            print(" - FileDataBase")
            exit()

    def set_db_path(self, db_path=None):
        self._handler_type.set_db_path(db_path=db_path)

    def set_db_name(self, db_name=None):
        self._handler_type.set_db_name(db_name=db_name)

    def create_db_user(self):
        return self._handler_type.create_db_user()

    def create_user(self, init_user_dictionary=None):
        return self._handler_type.create_user(init_user_dictionary)

    def modify_user(self, user_hash, key, value, mode):
        return self._handler_type.modify_user(user_hash=user_hash,
                                              key=key,
                                              value=value,
                                              mode=mode)

    def mod_user_by_hash(self, hash=None, key=None, value=None, date_obj=None):
        return self._handler_type.mod_user_by_hash(hash=hash,
                                                   key=key,
                                                   value=value,
                                                   date_obj=date_obj)

    def list_user_by_hash(self, user_hash):
        return self._handler_type.list_user_by_hash(user_hash=user_hash)

    def search_user(self, user=None, by=None):
        return self._handler_type.search_user(user, by)

    def search_user_by_hash(self, hash=None):
        return self._handler_type.search_user_by_hash(hash=hash)

    def create_db_tracks(self):
        return self._handler_type.create_db_tracks()

    def test_db_user(self):
        return self._handler_type.test_db_user()

    def test_db_tracks(self):
        return self._handler_type.test_db_tracks()

    #This part handles write/read operations on the tracks database:
    #  - Tracks are like branches of a tree
    def write_branch(self, db_operation="new", track=None, track_hash=None):
        return self._handler_type.write_branch(db_operation, track, track_hash)

    def read_branch(self, key=None, attribute=None):
        return self._handler_type.read_branch(key=key, attribute=attribute)

    def search_branch(self, key=None, attribute=None, how=None):
        return self._handler_type.search_branch(key=key, attribute=attribute, how=how)

    def delete_branch(self, key=None, attribute=None):
        return self._handler_type.delete_branch(key=key, attribute=attribute)

    #This part handles write/read operation on metadata
    #  - metadata to tracks are like leaves which belong to branch

    def create_leaf_config(self, leaf_name, track_hash, columns, status=None):
        """
        We create a leaf configuration by the input of leaf name, the according
        track_hash (to which the leaf is attached) and the leaf column defintion.
        If there is no existing column defintion we are free to put something else
        in the variable to make it unique.

        We want to create mapping by creating a unique hash sum depending on leaf_name,
        track_hash and columns to re-create the mapping if necessary. In that way, we
        get a unique identifier.

        :param leaf_name:
        :param track_hash:
        :param columns:
        :return:
        """

        # Prepare the leaf dictionary:
        leaf_config = {
            "name": leaf_name,
            "track_hash": track_hash,
            "columns": columns
        }
        # Create a unique hash according to the leaf configuration and add it:
        leaf_hash = hashlib.md5(json.dumps(leaf_config).encode("utf-8")).hexdigest()
        leaf_config['leaf_hash'] = leaf_hash

        # The track hash was necessary for uniqueness but not necessary to store
        # twice in the later database:
        del leaf_config["track_hash"]

        if status is not None:
            leaf_config["status"] = status

        return leaf_config

    def write_leaf(self,
                   track_hash=None,
                   leaf_config=None,
                   leaf=None,
                   leaf_type=None
                   ):
        return self._handler_type.write_leaf(track_hash=track_hash,
                                             leaf_config=leaf_config,
                                             leaf=leaf,
                                             leaf_type=leaf_type)

    def read_leaf(self,
                  directory=None,
                  leaf_hash=None,
                  leaf_type=None
                  ):
        return self._handler_type.read_leaf(directory=directory,
                                            leaf_hash=leaf_hash,
                                            leaf_type=leaf_type)

    def delete_leaf(self,
                    leaf_name=None,
                    track_hash=None
                    ):

        return self._handler_type.delete_leaf(leaf_name=leaf_name,
                                              track_hash=track_hash)

    # Gets:
    def get_database_exists(self):
        return self._handler_type.get_database_exists()

    def get_database_tables_exists(self):
        return self._handler_type.get_database_tables_exists()

    # High level operations:
    def get_all_leaves_for_track(self, track_hash=None):
        return self._handler_type.get_all_leaves_for_track(track_hash=track_hash)

    def get_all_users(self, by=None):
        return self._handler_type.get_all_users(by=by)
