from db_utils import DBManager


class DataSelector(DBManager):
    def __init__(self):
        super().__init__()

    def get_number_of_tweets_per_day(self):
        select_sql = "SELECT created_at , count(*) AS NUMBER FROM tweet " \
                     "WHERE created_at > '2020-02-01' " \
                     "GROUP BY DATE(created_at)"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            date = row[0]
            num = row[1]
            result[date] = num
        return result

    def get_most_popular_users(self):
        select_sql = "SELECT screen_name, followers_count, friends_count " \
                     "FROM user ORDER BY followers_count DESC, friends_count DESC LIMIT 100"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row[0]] = (row[1], row[2])
        return result
