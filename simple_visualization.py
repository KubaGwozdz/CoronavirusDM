from datetime import datetime

from data_selection import DataSelector
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


if __name__ == "__main__":
    db = DataSelector()
    data = db.get_number_of_tweets_per_day()
    X = [datetime.strptime(d, "%Y-%m-%d %H:%M:%S").date() for d in data.keys()]
    Y = data.values()

    print(X)
    print(Y)

    ax = plt.gca()
    formatter = mdates.DateFormatter("%Y-%m-%d")
    ax.xaxis.set_major_formatter(formatter)

    locator = mdates.DayLocator()
    ax.xaxis.set_major_locator(locator)

    plt.plot(X, Y)
    plt.show()


    # Set the locator
    # locator = mdates.MonthLocator()  # every month
    # Specify the format - %b gives us Jan, Feb...
    # fmt = mdates.DateFormatter('%b')

    # plt.plot(X,Y)
    # plt.gcf().autofmt_xdate()
    # # ax.set_major_locator(locator)
    # # ax.set_major_formatter(fmt)
    # plt.xlabel('date')
    # plt.ylabel('number of tweets')
    # plt.show()
