import sqlite3


def connect_to_db():
    con = sqlite3.connect('./CoronavirusDB')
    cur = con.cursor()
    return cur


class DBManager:
    def __init__(self):
        self.connection = sqlite3.connect('./CoronavirusDB')
        self.connection.row_factory = sqlite3.Row
        self.cur = self.connection.cursor()

    def update_tweet(self, new_tweet, old_tweet):
        names = []
        vals = []
        fields = ['quote_count', 'reply_count', 'retweet_count', 'favourite_count']
        for field in fields:
            if new_tweet[field] > old_tweet[field]:
                names.append(field)
                vals.append(new_tweet[field])

        if len(names) > 0:
            update_sql = "UPDATE tweet SET "
            for name in names:
                update_sql += name + ' = ?, '
                update_sql = update_sql[:-1]


    def insert_tweet(self, tweet):
        id = tweet['id']

        select_sql = "SELECT id, quote_count, reply_count, retweet_count, favourite_count FROM tweet WHERE id = ?"
        self.cur.execute(select_sql, (id,))

        tweet_db = self.cur.fetchone()
        if tweet_db is not None:
            self.update_tweet(tweet, tweet_db)
        else:
            insert_sql = "INSERT INTO tweet " \
                  "(id, user_id, text, lang, quote_count, reply_count, retweet_count, favourite_count, created_at, in_reply_to_status_id, in_reply_to_user_id, quoted_status_id)" \
                  "VALUES " \
                  "(?, ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?)"
            if 'quoted_status_id' in tweet:
                val = (
                    tweet['id'], tweet['user_id'], tweet['text'], tweet['lang'], tweet['quote_count'], tweet['reply_count'],
                    tweet['retweet_count'], tweet['favorite_count'], tweet['created_at'], tweet['in_reply_to_status_id'],
                    tweet['in_reply_to_user_id'], tweet['quoted_status_id'])
            else:
                val = (
                    tweet['id'], tweet['user_id'], tweet['text'], tweet['lang'], tweet['quote_count'], tweet['reply_count'],
                    tweet['retweet_count'], tweet['favorite_count'], tweet['created_at'], tweet['in_reply_to_status_id'],
                    tweet['in_reply_to_user_id'], None)

            self.cur.execute(insert_sql, val)
            self.connection.commit()

    def close_db(self):
        self.connection.close()




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
