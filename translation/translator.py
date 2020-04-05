from googletrans import Translator as GoogleTrans

from db_utils.data_inserter import DataInserter
from db_utils.data_selector import DataSelector

from data_processing.pipe import Pipe, Worker

from data_processing.preprocessors import TweetsPreprocessor

from textblob import TextBlob

import time


class TranslatorWithSentimient(Worker):
    def __init__(self, id, data, pipe):
        super().__init__(id, data, pipe)
        self.preprocessor = TweetsPreprocessor()

    def run(self):
        result = []
        for tweet in self.data:
            try:
                translator = GoogleTrans()
                text = self.preprocessor.process_tweet_to_translation(tweet['text'])

                translation = translator.translate(text=text, dest='en', src=tweet['lang'])
                text_eng = translation.text

                analysis = TextBlob(self.preprocessor.process_translated_to_analyze(text_eng))
                time.sleep(5)
            except Exception as e:
                if len(result) < len(self.data)/2:
                    print('T' + str(self.id) + ': quota exceeded, stopping..\n' + str(e))
                    self.pipe.quote_exceeded = True
                    break
                else:
                    break
            result.append((text_eng, analysis.polarity, analysis.subjectivity, tweet['id']))

        self.pipe.put_done_data(result)


class TranslatedSentimentPipe(Pipe):
    def __init__(self):
        super().__init__(DataSelector.get_tweets_to_translate, DataInserter.update_translate_and_sentiment,
                         TranslatorWithSentimient)
        self.batch_size = 10
        self.max_threads = 1
        self.max_threads = 1


if __name__ == '__main__':
    pipe = TranslatedSentimentPipe()
    pipe.run()
