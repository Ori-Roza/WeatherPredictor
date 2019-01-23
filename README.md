# WeatherPredictor
Average temperature predictor in Tel-aviv using Linear Regression.

* Uses Wunderground json api to retrieve historic temperature.
* Inserts data to MongoDB
* Create datasets fron the following features: daily_min_temp, daily_max_temp, daily_avg_temp and day_season_association.
* Predicts daily temperature using Linear Regression

The main.py gets the days to predict number as an argument ('-d', '--days').

For example: main.py --days=3

output:

2019-01-24
average temperature: 15.0(c)
2019-01-25
average temperature: 15.0(c)
2019-01-26
average temperature: 14.0(c)
