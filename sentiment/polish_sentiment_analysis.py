import csv
import statistics as stat
from db_utils.data_selector import DataSelector
from db_utils.data_inserter import DataInserter
from data_processing.pipe import Pipe, Worker

from visualizations.simple_visualizations import polish_sentiment


class PolishSentimentAnalyzer(Worker):
    def __init__(self, id, tweets, pipe):
        super().__init__(id, tweets, pipe)

    def map_sentiment(self, word_list):

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

    def run(self):

        sentiments = []

        for (id, text, date) in self.data:

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

                        word_data = [row for row in self.pipe.wordnet if row[0] == word and row[6] != '' and row[6] != 'NULL']

                        if len(word_data) > 0:

                            word_sentiment = self.map_sentiment(word_data)
                            if word_sentiment != 0:
                                meaningful_counter += 1

                    sentiment += word_sentiment

            if meaningful_counter == 0:
                meaningful_counter = 1
            sentiments.append((sentiment/meaningful_counter, id))

        self.pipe.put_done_data(sentiments)


class PolishSentimentPipe(Pipe):
    def __init__(self, **kwargs):
        if 'lock' in kwargs and 'db_m' in kwargs:
            l = kwargs['lock']
            d = kwargs['db_m']
            super().__init__(DataSelector.get_polish_tweets, DataInserter.update_tweet_polish_sentiment, PolishSentimentAnalyzer,
                             lock=l, db_m=d)
        else:
            super().__init__(DataSelector.get_polish_tweets, DataInserter.update_tweet_polish_sentiment, PolishSentimentAnalyzer)
        self.batch_size = 10
        self.max_threads = 4

        self.wordnet = self.get_wordnet_sentiments()

    def get_wordnet_sentiments(self):
        print("-- Creating wordnet --")
        file_path = "../plwordnet_4_0/słownik_anotacji_emocjonalnej.csv"
        with open(file_path) as csv_file:
            reader = csv.reader(csv_file, delimiter=',')
            data = []
            for row in reader:
                data.append(row)

        data = data[1:]
        print("-- Wordnet created --")
        return data


if __name__ == '__main__':

    pipe = PolishSentimentPipe()
    pipe.run()
