import requests
import datetime
import pandas as pd
import numpy as np
from time import sleep
from bson import ObjectId
from odm import DataDB


def fix_db():
    """
    Fixes db:
     removes duplicate dates
     alerts missing dates
     check whether date is valid
     check whether season association exists
    :return:
    """
    a = DataDB()
    start = list(a.collection.find({}).sort("date", 1).limit(1))[0]
    end = list(a.collection.find({}).sort("date", -1).limit(1))[0]

    start = datetime.datetime.strptime(start["date"], '%Y%m%d')
    end = datetime.datetime.strptime(end["date"], '%Y%m%d')

    missing_dates = []

    for d in pd.date_range(start, end):
        day = "%s%s" % ("0" if d.day < 10 else "", d.day)
        month = "%s%s" % ("0" if d.month < 10 else "", d.month)
        d_str = "%s%s%s" % (d.year, month, day)
        c = list(a.collection.find({"date": d_str}))
        if not c:
            print "missing", d
            missing_dates.append(d)
            continue
        if len(c) > 1:
            print "duplicate", d
            ids = [ObjectId(_id["_id"]) for _id in c[1:]]  # except the first one
            a.collection.delete_many({"_id": {"$in": ids}})
        if c[0]["min_temp"] is None or c[0]["max_temp"] is None or c[0]["avg_temp"] is None:
            print "no data", d

    for d in missing_dates:
        b = DataCollector((d.year, d.month, d.day), (d.year, d.month, d.day))
        b.fetch_data_to_db()

    for d in a.collection.find():
        if len(d["date"]) != 8:
            a.collection.delete_one({"date": d["date"]})
        if "season_association" not in d:
            a.collection.update({"_id": ObjectId(d["_id"])}, {"$set": {"season_association": get_season_association(d["date"])}})


def convert_date_to_day_in_year(date):
    """
    Returns the day related to the year e.g 1.1 is 1/365, 25.1 is 25/365 and etc
    :param date:
    :return:
    """
    date = datetime.datetime.strptime(date, '%Y%m%d')
    first_date = datetime.datetime(date.year - 1, 12, 31)  # start of year
    return (date - first_date).days


def get_season_association(date):
    """
    split the year to 4 seasons:
    winter = 1
    summer = -1
    autumn = 0
    spring  = 0

    we turn each day to degrees by multiply the day with (360/365) (one season is approximately 90 days)
    we start from spring (0) we can sigh that autumn and spring in Israel are pretty much the same.
    that way we're adding 90 to starts from spring
    then, convert to radians and returns the sin (temperature is sensitively changed)

    :param date:
    :return:
    """
    day_in_year = convert_date_to_day_in_year(date)
    return float(np.sin(np.deg2rad(float(day_in_year) * float(360 / 365.0) + 90)))


class WundergroundURLGenerator(object):
    _URL = "https://api-ak.wunderground.com/api/d8585d80376a429e/history_{date_str}/" \
           "lang:EN/units:english/bestfct:1/v:2.0/q/LLSD.json?showObs=0&ttl=120"

    def __init__(self, start_date, end_date):
        self.start_date = datetime.datetime(*start_date)
        self.end_date = datetime.datetime(*end_date)

    @property
    def date_range(self):
        dates = pd.date_range(self.start_date, self.end_date).tolist()
        for d in dates:
            d = d.strftime("%Y%m%d")
            yield d  # 20100103

    @property
    def urls(self):
        for day in self.date_range:
            yield (day, self._URL.format(date_str=day))


class WundergroundAPI(object):
    def __init__(self, url):
        sleep(1)
        self._url = url
        self._content = self._connect(url)

    @staticmethod
    def _connect(url):
        req = requests.get(url)
        if not req.ok:
            raise ValueError("%s is invalid" % url)
        return req.json()

    def f_to_c(self, temperature):
        """
        (f-32) * 5/9
        """
        try:
            return float((float(temperature) - 32.0) * (5.0 / 9.0))
        except (ValueError, TypeError):
            return None

    def _search_temperature_value(self, value):
        if "history" in self._content:
            if "days" in self._content["history"]:
                if len(self._content["history"]["days"]) > 0:
                    return self._content["history"]["days"][0]["summary"][value]
        return None

    @property
    def high_temp(self):
        return self.f_to_c(self._search_temperature_value("max_temperature"))

    @property
    def low_temp(self):
        return self.f_to_c(self._search_temperature_value("min_temperature"))

    @property
    def avg_temp(self):
        if self.high_temp and self.low_temp:
            return (self.high_temp + self.low_temp) / 2.0
        return None


class DataCollector(object):
    _CHUNCKSIZE = 100

    def __init__(self, start_date=None, end_date=None):
        self.db = DataDB()
        if not start_date or not end_date:
            start_date = self._start_date
            end_date = self._end_date
        if start_date != end_date:
            self.url_generator = WundergroundURLGenerator(start_date, end_date)
        else:
            self.url_generator = None

    @property
    def _start_date(self):
        last_date_from_db = list(self.db.collection.find({}).sort("date", -1).limit(1))[0]["date"]
        date_obj = datetime.datetime.strptime(last_date_from_db, "%Y%m%d")
        return date_obj.year, date_obj.month, date_obj.day

    @property
    def _end_date(self):
        now = datetime.datetime.now()
        return now.year, now.month, now.day

    def fetch_data_to_db(self):
        documents = []
        if self.url_generator:
            for data in self.url_generator.urls:
                date = data[0]
                url = data[1]
                req = WundergroundAPI(url)
                documents.append(
                    {"min_temp": req.low_temp, "max_temp": req.high_temp, "avg_temp": req.avg_temp, "date": date,
                     "season_association": get_season_association(date)})
                if len(documents) == self._CHUNCKSIZE:
                    print "Inserting %d recordings to DB" % self._CHUNCKSIZE
                    self.db.insert_many(documents)
                    documents = []
            if len(documents) > 0:
                print "Inserting %d recordings to DB" % len(documents)
                self.db.insert_many(documents)  # adds what left
        fix_db()


if __name__ == '__main__':
    a = DataCollector()
    a.fetch_data_to_db()
