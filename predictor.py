import pandas as pd
import datetime
import warnings
from sklearn import preprocessing
from sklearn.linear_model import LinearRegression

warnings.filterwarnings(action="ignore", module="sklearn", message="^internal gelsd")


class WeatherPredictor(object):
    def __init__(self, dataset):
        self.clf = LinearRegression()
        self._dataset = pd.DataFrame(dataset[1:], columns=dataset[0])
        self.last_date = self._last_date
        self._test_train_splitter_percentage = 0.75  # 75% train 25% test

    @property
    def _last_date(self):
        raw_date = list(self._dataset.loc[[len(self._dataset.index) - 1]]["date"])[0]
        return datetime.datetime.strptime(raw_date, "%Y%m%d")

    def get_date_range(self, days):
        dates = []
        jump = 1
        init_date = self.last_date
        for _ in range(days):
            dates.append(init_date + datetime.timedelta(days=jump))
            init_date = init_date + datetime.timedelta(days=jump)
        return dates

    def _create_features(self):
        self._dataset['min'] = self._dataset['min_temp'].shift(1).rolling(window=1).mean()
        self._dataset['max'] = self._dataset['max_temp'].shift(1).rolling(window=1).mean()
        self._dataset['association'] = self._dataset['season_association'].shift(1).rolling(window=1).mean()
        self._dataset['avg'] = self._dataset['avg_temp'].shift(1).rolling(
            window=7).mean()  # create avg of last 7 temperatures (in windows of 7)
        self._dataset = self._dataset.dropna()

    def _generate_x_y(self, forecast_time):
        self._create_features()

        x = self._dataset[['min', 'max', 'avg', "association"]]
        y = self._dataset[['avg']]

        x = preprocessing.scale(x)  # Scaling due to season associations

        t = int(self._test_train_splitter_percentage * len(self._dataset))
        x_train = x[:t]
        y_train = y[:t]
        x_prediction = x[-forecast_time:]

        return x_train, y_train, x_prediction

    def _predict(self, days):
        x_train, y_train, x_prediction = self._generate_x_y(days)
        self.clf.fit(x_train, y_train)
        return self.clf.predict(x_prediction),

    def forecast(self, days=1):
        prediction, = self._predict(days)
        dates_range = self.get_date_range(days)
        if len(prediction) != len(dates_range):
            raise ValueError("Error")
        for i in range(len(dates_range)):
            print "%s\naverage temperature: %.1f(c)" % (str(dates_range[i].date()), round((prediction[i][0])))
