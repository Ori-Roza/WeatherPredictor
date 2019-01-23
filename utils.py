import matplotlib.pyplot as plt


def plot_weather(pandas_dataset):
    plt.figure(figsize=(18, 9))
    plt.plot(range(pandas_dataset.shape[0]), (pandas_dataset["avg_temp"]))
    plt.xticks(range(0, pandas_dataset.shape[0], 500), pandas_dataset['date'].loc[::500], rotation=45)
    plt.xlabel('Date', fontsize=18)
    plt.ylabel('avg temp', fontsize=18)
    plt.show()

