from db_utils.data_inserter import DataInserter
from db_utils.data_selector import DataSelector

from data_processing.pipe import Pipe, Worker

from data_processing.preprocessors import TweetsPreprocessor

from textblob import TextBlob


class SentimentAnalyzer(Worker):
    def __init__(self, id, tweets, pipe):
        super().__init__(id, tweets, pipe)

    def run(self):
        result = []
        preprocessor = TweetsPreprocessor()
        for tweet in self.data:
            preprocessed = preprocessor.process_tweet(tweet['text'])
            analysis = TextBlob(preprocessed)
            """
                subjectivity -> [0,1]
                    0.0: very objective
                    1.0: very subjective
                
                polarity -> [-1,1]
                    1.0: positive
                    -1.0: negative
            """
            result.append((analysis.polarity, analysis.subjectivity, tweet['id']))
        self.pipe.put_done_data(result)


class SentimentPipe(Pipe):
    def __init__(self):
        super().__init__(DataSelector.get_tweets_to_analyze, DataInserter.update_sentiment, SentimentAnalyzer)
        self.batch_size = 1000
        self.max_threads = 10


if __name__ == '__main__':
    pipe = SentimentPipe()
    pipe.run()
