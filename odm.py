from pymongo import MongoClient


class DataDB(object):
    MONGO_IP_PORT = ("127.0.0.1", 27017)
    DB_NAME = "WHEATER_PER_CITY"

    def __init__(self, city="Tel_Aviv"):
        self._db_connector = MongoClient(*self.MONGO_IP_PORT)
        self._db = self._db_connector.get_database(self.DB_NAME)
        self.city_collection = city

    @property
    def collection(self):
        return self._db.get_collection(self.city_collection)

    def insert_many(self, documents):
        self._db.get_collection(self.city_collection).insert_many(documents)
        self._db.get_collection(self.city_collection).create_index("date")
