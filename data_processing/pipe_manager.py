from db_utils.data_inserter import DataInserter
from db_utils.data_selector import DataSelector
from db_utils.db_manager import DBManager

import threading

from geolocation.users_locations import LocalizationPipe
from sentiment.sentiment_analysis import SentimentPipe
from translation.translator import TranslatedSentimentPipe


class PipeManager:
    def __init__(self):
        self.db_m = DBManager()
        self._db_access = threading.Lock()

    def start(self):
        trans_pipe = TranslatedSentimentPipe(lock=self._db_access, db_m=self.db_m)
        trans_pipe.run_one()

        loc_pipe = LocalizationPipe(lock=self._db_access, db_m=self.db_m)
        loc_pipe.run()


if __name__ == '__main__':
    manager = PipeManager()
    manager.start()
