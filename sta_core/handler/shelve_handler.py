import os
import shelve


class ShelveHandler():
    """
    The shelve library handles itself quite well, nevertheless for our specialized purposes here
    we introduce some member functions on top to make our life easier.
    """

    def __init__(self):
        """
        Constructor of the ShelveHandler. The main purpose of the ShelveHandler
        is to allow store temporary information for the database handler.
        """
        self.path = os.path.join(os.path.expanduser("~"), ".sta")
        self.db = None

    def __del__(self):
        """
        Destructor to close the open shelve object if any.
        :return:
        """
        try:
            self.db.close()
        except:
            pass

    def set_shelve_path(self, path):
        self.path = path

    def get_shelve_path(self):
        return self.path

    def _open_shelve(self):
        """
        Open a shelve handler session.
        :return:
        """
        self.db = shelve.open(self.path)

    def _close_shelve(self):
        if self.db is not None:
            self.db.close()

    def write_shelve(self, pairs):
        self._open_shelve()
        for key, value in pairs.items():
            self.db[key] = value
            print("Log: Write: ", key, value)
        self._close_shelve()

    def delete_shelve_key(self, key):
        pass

    def read_shelve_by_keys(self, keys):
        if isinstance(keys, str):
            keys = [keys]

        self._open_shelve()
        ret_dict = {}
        for i_key in keys:
            ret_dict[i_key] = self.db.get(i_key)
        self._close_shelve()

        return ret_dict

    def get_all_shelve_keys(self):
        self._open_shelve()

        all_shelve_keys = [i for i in self.db.keys()]

        self._close_shelve()

        return all_shelve_keys
