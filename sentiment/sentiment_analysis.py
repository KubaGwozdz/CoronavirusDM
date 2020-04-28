from db_utils.data_inserter import DataInserter
from db_utils.data_selector import DataSelector

from data_processing.pipe import Pipe, Worker

from data_processing.preprocessors import TweetsPreprocessor

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class SentimentAnalyzer(Worker):
    def __init__(self, id, tweets, pipe):
        super().__init__(id, tweets, pipe)

    def run(self):
        result = []
        preprocessor = TweetsPreprocessor()
        for tweet in self.data:
            preprocessed = preprocessor.process_tweet(tweet['text'])
            analyzer = SentimentIntensityAnalyzer()
            pol = analyzer.polarity_scores(preprocessed)['compound']
            """ 
                polarity -> [-1,1]
                    1.0: positive
                    -1.0: negative
            """
            result.append((pol, tweet['id']))
        self.pipe.put_done_data(result)


class SentimentPipe(Pipe):
    def __init__(self, **kwargs):
        if 'lock' in kwargs and 'db_m' in kwargs:
            l = kwargs['lock']
            d = kwargs['db_m']
            super().__init__(DataSelector.get_tweets_to_analyze, DataInserter.update_sentiment, SentimentAnalyzer,
                             lock=l, db_m=d)
        else:
            super().__init__(DataSelector.get_tweets_to_analyze, DataInserter.update_sentiment, SentimentAnalyzer)
        self.batch_size = 1000
        self.max_threads = 10


if __name__ == '__main__':
    pipe = SentimentPipe()
    pipe.run()
