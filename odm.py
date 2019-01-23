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


if __name__ == '__main__':
    import datetime
    import numpy as np


    def convert_date_to_day_in_year(date):
        date = datetime.datetime.strptime(date, '%Y%m%d')
        first_date = datetime.datetime(date.year - 1, 12, 31)  # start of year
        return (date - first_date).days


    def get_season_association(date):
        day_in_year = convert_date_to_day_in_year(date)
        return float(np.sin(np.deg2rad(float(day_in_year) * float(360 / 365.0) + 90)))


    a = DataDB()
    for doc in a.collection.find():
        a.collection.update({"_id": doc["_id"]}, {"$set": {"season_association": get_season_association(doc["date"])}})
