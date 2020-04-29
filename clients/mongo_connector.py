from pymongo import MongoClient


class MongoDBConnector(object):
    """This object is used to connect AnalEyeZer with MongoDB"""

    def __init__(self, part):
        self.mongo_conn = None
        self.database = None
        self.collection = None
        self.part = part

    def check_if_uri_is_valid(self, uri):
        """
        This function is used to check if the provided mongo URI is valid

        Args:
            uri: provided MongoDB URI

        Returns: True / False

        """
        try:
            self.mongo_conn = MongoClient(uri)
            self.database = self.mongo_conn.get_database()

            database_collections = self.database.list_collection_names()
            if self.part in database_collections:

                self.collection = self.database[self.part]
                return True
            else:
                return False

        except Exception as ex:
            return False
