import argparse
from predictor import WeatherPredictor
from dataset_generator import create_datasets


def args():
    parser = argparse.ArgumentParser(
        description='Predict weather of a given date in Tel-Aviv')
    parser.add_argument('-d', '--days', help='days to be forecast', required=False, default=365, type=int)
    return parser.parse_args()


def run():
    days = args().days
    predict = WeatherPredictor(create_datasets())
    predict.forecast(days)


if __name__ == '__main__':
    run()
