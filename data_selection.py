from datetime import datetime

from db_utils import DBManager


class DataSelector(DBManager):
    def __init__(self):
        super().__init__()

    def get_number_of_tweets_per_day(self):
        select_sql = "SELECT created_at , count(*) AS NUMBER FROM tweet " \
                     "WHERE created_at > '2020-03-07' " \
                     "GROUP BY DATE(created_at)"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            date = row[0]
            num = row[1]
            result[date] = num
        return result

    def get_most_popular_hashtags_per_day(self):
        select_sql = "SELECT hashtag.text, t.created_at, count(*) AS number from hashtag " \
                     "JOIN hashtag_tweet h_t on hashtag.id = h_t.hashtag_id " \
                     "JOIN tweet t on h_t.tweet_id = t.id " \
                     "WHERE t.created_at > '2020-03-07' AND h_t.hashtag_id in (SELECT hashtag_id FROM hashtag_tweet " \
                     "GROUP BY hashtag_id " \
                     "ORDER BY count(*) DESC " \
                     "LIMIT 10) " \
                     "GROUP BY h_t.hashtag_id, date(t.created_at)"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            if row['text'] not in result:
                result[row['text']] = dict()
                result[row['text']]['dates'] = []
                result[row['text']]['numbers'] = []

            result[row['text']]['dates'].append(datetime.strptime(row['created_at'], "%Y-%m-%d %H:%M:%S").date())
            result[row['text']]['numbers'].append(row['number'])

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
