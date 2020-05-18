import csv
import statistics as stat
from db_utils.data_selector import DataSelector
from visualizations.simple_visualizations import polish_sentiment


def map_sentiment(word_list):

    sentiments = []
    sentiment = 0

    for word in word_list:

        value = word[6]

        if value == "- m":
            sentiment = -1
        elif value == "- s":
            sentiment = -0.5
        elif value == "+ s":
            sentiment = 0.5
        elif value == "+ m":
            sentiment = 1
        elif value == "amb":
            sentiment = "amb"

        sentiments.append(sentiment)

    ambs = [s for s in sentiments if s == "amb"]

    if len(ambs) > 0:
        tinge = sum([s for s in sentiments if type(s) == int])/len(sentiments)
        sentiments = map(lambda x: x if x != "amb" else tinge, sentiments)

    return stat.mean(sentiments)


def get_words_sentiment(file_path, tweets):

    with open(file_path) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        data = []
        for row in reader:
            data.append(row)

    data = data[1:]
    sentiments = []

    for (id, text, date) in tweets:

        text = text.lower()

        sentiment = 0
        meaningful_counter = 0

        words = text.split()

        for word in words:

            if len(word) > 2:

                word_sentiment = 0

                if not any(s in word for s in "0123456789/£~§@$%^&*()[]{}|\\\"'<>+=-_"):

                    if '#' in word:
                        word = word.replace('#', '')
                    if '.' in word:
                        word = word.replace('.', '')
                    if ',' in word:
                        word = word.replace(',', '')
                    if '?' in word:
                        word = word.replace('?', '')
                    if ':' in word:
                        word = word.replace(':', '')
                    if '!' in word:
                        word = word.replace('!', '')

                    word_data = [row for row in data if row[0] == word and row[6] != '' and row[6] != 'NULL']

                    if len(word_data) > 0:

                        word_sentiment = map_sentiment(word_data)
                        if word_sentiment != 0:
                            meaningful_counter += 1

                sentiment += word_sentiment

        if meaningful_counter == 0:
            meaningful_counter = 1
        sentiments.append((sentiment/meaningful_counter, date))

    return sentiments


def get_daily_sentiment(sentiments):

    daily_sentiments = []
    parsed_dates = []

    for sentiment in sentiments:

        date = sentiment[1]

        if date not in parsed_dates:

            parsed_dates.append(date)
            values = [s[0] for s in sentiments if s[1] == date]

            if len(values) > 1:
                mean_sentiment = stat.mean(values)
                std_sentiment = stat.stdev(values)
                daily_sentiments.append((date, mean_sentiment, std_sentiment))
            elif len(values) == 1:
                mean_sentiment = values[0]
                std_sentiment = 0
                daily_sentiments.append((date, mean_sentiment, std_sentiment))

    return daily_sentiments


if __name__ == '__main__':

    data_selector = DataSelector()

    dir = "../plwordnet_4_0"
    file_path = dir + "/słownik_anotacji_emocjonalnej.csv"

    tweets = data_selector.get_polish_tweets()

    sentiments = get_words_sentiment(file_path, tweets)
    daily_sentiments = get_daily_sentiment(sentiments)
    fig = polish_sentiment(daily_sentiments)

    fig.show()


