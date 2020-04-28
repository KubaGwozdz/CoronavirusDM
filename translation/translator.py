from googletrans import Translator as GoogleTrans
from translate import Translator as MyMemoryTrans

from db_utils.data_inserter import DataInserter
from db_utils.data_selector import DataSelector

from data_processing.pipe import Pipe, Worker

from data_processing.preprocessors import TweetsPreprocessor

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import time


class TranslatorWithSentimient(Worker):
    def __init__(self, id, data, pipe):
        super().__init__(id, data, pipe)
        self.preprocessor = TweetsPreprocessor()

    def run(self):
        result = []
        for tweet in self.data:
            try:
                # translator = GoogleTrans()
                translator = MyMemoryTrans(to_lang='en', from_lang=tweet['lang'], email='gwozdz.jakub@gmail.com')
                text = self.preprocessor.process_tweet_to_translation(tweet['text'])

                text_eng = translator.translate(text)
                print(text)
                print(text_eng)
                if 'MYMEMORY WARNING:' in text_eng:
                    print('YOU USED ALL AVAILABLE FREE TRANSLATIONS FOR TODAY, finishing..')
                    self.pipe.stop()
                    break
                # text_eng = translation.text       #Googletrans

                analyzer = SentimentIntensityAnalyzer()
                pol = analyzer.polarity_scores(self.preprocessor.process_translated_to_analyze(text_eng))['compound']

                result.append((text_eng, pol, tweet['id']))
                time.sleep(1)
            except Exception as e:
                if len(result) < len(self.data) / 2:
                    print('T' + str(self.id) + ': quota exceeded..\n' + str(e))
                    self.pipe.quote_exceeded = True
                    break
                else:
                    break

        self.pipe.put_done_data(result)


class TranslatedSentimentPipe(Pipe):
    def __init__(self, **kwargs):
        if 'lock' in kwargs and 'db_m' in kwargs:
            l = kwargs['lock']
            d = kwargs['db_m']
            super().__init__(DataSelector.get_tweets_to_translate, DataInserter.update_translate_and_sentiment,
                             TranslatorWithSentimient, lock=l, db_m=d)
        else:
            super().__init__(DataSelector.get_tweets_to_translate, DataInserter.update_translate_and_sentiment,
                             TranslatorWithSentimient)
        self.batch_size = 5
        self.max_threads = 1
        self.num_loc_threads = 1


if __name__ == '__main__':
    pipe = TranslatedSentimentPipe()
    pipe.run_one()
