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

    def save_usermentions(self, tweet):
        um = tweet['user_mentions']
        um_tw_us = list(map(lambda x: (tweet['id'], x['id']), um))

        insert_sql = "INSERT INTO user_mentions (tweet_id, user_id) VALUES (?, ?)"
        self.cur.executemany(insert_sql, um_tw_us)
        self.connection.commit()

    def save_hashtags(self, tweet, hashtags_dict):
        hashtags = []
        for tag in hashtags_dict:
            hashtags.append(tag['text'])

        if len(hashtags) > 0:
            select_sql = "SELECT id, text FROM hashtag WHERE "
            for i in range(len(hashtags)):
                select_sql += "text = ? OR "
            select_sql = select_sql[:-4]
            self.cur.execute(select_sql, tuple(hashtags))

            hashtags_db = self.cur.fetchall()

            existing_hashtag_ids = []
            existing_hashtags = []
            for tag in hashtags_db:
                existing_hashtag_ids.append(tag['id'])
                existing_hashtags.append(tag['text'])

            # print(hashtags)
            # print(existing_hashtags)
            new_hashtags = list(filter(lambda tag: tag not in existing_hashtags, hashtags))

            insert_hashtags_sql = "INSERT INTO hashtag (text) VALUES (?)"
            for tag in new_hashtags:
                self.cur.execute(insert_hashtags_sql, (tag,))
                self.connection.commit()
                existing_hashtag_ids.append(self.cur.lastrowid)

            insert_hash_tweet_sql = "INSERT INTO hashtag_tweet (hashtag_id, tweet_id) VALUES (?, ?)"
            existing_hashtag_ids = list(map(lambda x: (x, tweet['id']), existing_hashtag_ids))
            self.cur.executemany(insert_hash_tweet_sql, existing_hashtag_ids)
            self.connection.commit()

    def update_tweet(self, new_tweet, old_tweet):
        names = []
        vals = []
        fields = ['quote_count', 'reply_count', 'retweet_count', 'favorite_count']
        for field in fields:
            if new_tweet[field] > old_tweet[field]:
                names.append(field)
                vals.append(new_tweet[field])

        if len(names) > 0:
            update_sql = "UPDATE tweet SET "
            for name in names:
                update_sql += name + ' = ?, '
            update_sql = update_sql[:-2]
            update_sql += "WHERE id = " + str(old_tweet['id'])
            self.cur.execute(update_sql, tuple(vals))
            self.connection.commit()

    def insert_tweet(self, tweet):
        id = tweet['id']

        select_sql = "SELECT id, quote_count, reply_count, retweet_count, favorite_count FROM tweet WHERE id = ?"
        self.cur.execute(select_sql, (id,))

        tweet_db = self.cur.fetchone()
        if tweet_db is not None:
            self.update_tweet(tweet, tweet_db)
        else:
            insert_sql = "INSERT INTO tweet " \
                         "(id, user_id, text, lang, quote_count, reply_count, retweet_count, favorite_count, created_at, in_reply_to_status_id, in_reply_to_user_id, quoted_status_id)" \
                         "VALUES " \
                         "(?, ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,?)"
            if 'quoted_status_id' in tweet:
                val = (
                    tweet['id'], tweet['user_id'], tweet['text'], tweet['lang'], tweet['quote_count'],
                    tweet['reply_count'],
                    tweet['retweet_count'], tweet['favorite_count'], tweet['created_at'],
                    tweet['in_reply_to_status_id'],
                    tweet['in_reply_to_user_id'], tweet['quoted_status_id'])
            else:
                val = (
                    tweet['id'], tweet['user_id'], tweet['text'], tweet['lang'], tweet['quote_count'],
                    tweet['reply_count'],
                    tweet['retweet_count'], tweet['favorite_count'], tweet['created_at'],
                    tweet['in_reply_to_status_id'],
                    tweet['in_reply_to_user_id'], None)

            self.cur.execute(insert_sql, val)
            self.connection.commit()

            hashtags = tweet['hashtags']
            self.save_hashtags(tweet, hashtags)
            self.save_usermentions(tweet)

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
