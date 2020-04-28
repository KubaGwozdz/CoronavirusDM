import sqlite3
from datetime import datetime
from configuration import database_path


class DBManager:
    def __init__(self):
        self.connection = sqlite3.connect(database_path)
        self.connection.row_factory = sqlite3.Row
        self.cur = self.connection.cursor()
        self.today = self.parse_datetime_to_date(datetime.today())

    def parse_to_date(self, date_str):
        d = datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')
        return d.strftime('%Y-%m-%d')

    def parse_to_datetime(self, datetime_str):
        d = datetime.strptime(datetime_str, '%a %b %d %H:%M:%S %z %Y')
        return d.strftime('%Y-%m-%d %H:%M:%S')

    def parse_datetime_to_date(self, datetime):
        return datetime.strftime('%Y-%m-%d')
