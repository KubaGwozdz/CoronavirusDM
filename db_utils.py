import sqlite3


def connect_to_db():
    con = sqlite3.connect('./CoronavirusDB')
    cur = con.cursor()
    return cur


class DBManager:
    def __init__(self):
        self.connection = sqlite3.connect('./CoronavirusDB')
        self.cur = self.connection.cursor()

    def insert_tweet(self, tweet):
        user_id = tweet['user_id']
        print(tweet)
        sql = "INSERT INTO tweet " \
              "(id, user_id, text, lang, quote_count, reply_count, retweet_count, favourite_count, created_at, in_reply_to_status_id, in_reply_to_user_id, quoted_status_id)" \
              "VALUES " \
              "(?, ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?)"
        if 'quoted_status_id' in tweet:
            val = (tweet['id'], tweet['user_id'], tweet['text'], tweet['lang'], tweet['quote_count'], tweet['reply_count'], tweet['retweet_count'], tweet['favourite_count'], tweet['created_at'], tweet['in_reply_to_status_id'], tweet['in_reply_to_user_id'], tweet['quoted_status_id'])
        else:
            val = (tweet['id'], tweet['user_id'], tweet['text'], tweet['lang'], tweet['quote_count'], tweet['reply_count'], tweet['retweet_count'], tweet['favourite_count'], tweet['created_at'], tweet['in_reply_to_status_id'], tweet['in_reply_to_user_id'], None)

        # self.cur.execute(sql, val)





        # self.cur.execute("""
        # SELECT * FROM user
        #     WHERE id = ?""", (user_id,))
        #
        # user = self.cur.fetchone()
        # if user is None:
        #     user_id = 1     #UNKNOWN
        # else:
        #     use
        # print(user)
