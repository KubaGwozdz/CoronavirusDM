from db_utils.db_manager import DBManager


class DataInserter(DBManager):
    def __init__(self, manager=None):
        if manager is None:
            super().__init__()
        else:
            self.connection = manager.connection
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

    def fast_insert_user_mentions(self, tweets):
        insert_sql = "INSERT OR IGNORE INTO user_mentions (tweet_id, user_id) VALUES (?, ?)"
        vals = []
        for tweet in tweets:
            for um in tweet['user_mentions']:
                vals.append((tweet['id'], um['id']))
        self.cur.executemany(insert_sql, vals)
        self.connection.commit()

    def fast_insert_hashtags(self, tweets):
        hashtag_tweets = dict()
        for tweet in tweets:
            for tag in tweet['hashtags']:
                txt = tag['text']
                if txt not in hashtag_tweets.keys():
                    hashtag_tweets[txt] = []
                hashtag_tweets[txt].append(tweet['id'])

        insert_h_sql = "INSERT OR IGNORE INTO hashtag (text) VALUES (?)"
        vals = list(map(lambda x: (x,)
                        , hashtag_tweets.keys()))
        self.cur.executemany(insert_h_sql, vals)
        self.connection.commit()

        h = list(hashtag_tweets.keys())
        tags = []
        MAX_VARIABLES_NUMBER = 900
        if len(h) > MAX_VARIABLES_NUMBER:
            for i in range(0, len(h), MAX_VARIABLES_NUMBER):
                questionmarks = '?' * len(h[i:i + MAX_VARIABLES_NUMBER])
                select_h_sql = "SELECT id, text FROM hashtag WHERE text IN ({})".format(','.join(questionmarks))
                self.cur.execute(select_h_sql, h[i:i + MAX_VARIABLES_NUMBER])
                tags = tags + self.cur.fetchall()

        else:
            questionmarks = '?' * len(h)
            select_h_sql = "SELECT id, text FROM hashtag WHERE text IN ({})".format(','.join(questionmarks))
            self.cur.execute(select_h_sql, h)
            tags = self.cur.fetchall()

        h_id_tw_id = []
        for tag in tags:
            for tweet_id in hashtag_tweets[tag['text']]:
                h_id_tw_id.append((tag['id'], tweet_id))

        insert_h_tw_sql = "INSERT OR IGNORE INTO hashtag_tweet (hashtag_id, tweet_id)  VALUES (?, ?)"
        self.cur.executemany(insert_h_tw_sql, h_id_tw_id)
        self.connection.commit()

    def fast_insert_tweets(self, tweets):
        insert_sql = "INSERT INTO tweet (id, user_id, text, lang, quote_count, reply_count, retweet_count, favorite_count, created_at, in_reply_to_status_id, in_reply_to_user_id, quoted_status_id)" \
                     "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET " \
                     "quote_count= MAX(excluded.quote_count, tweet.quote_count)," \
                     "reply_count= MAX(excluded.reply_count, tweet.reply_count)," \
                     "retweet_count= MAX(excluded.retweet_count, tweet.retweet_count)," \
                     "favorite_count= MAX(excluded.favorite_count, tweet.favorite_count)"

        vals = list(
            map(
                lambda tweet: (
                    tweet['id'], tweet['user_id'], tweet['text'], tweet['lang'], tweet['quote_count'],
                    tweet['reply_count'],
                    tweet['retweet_count'], tweet['favorite_count'], self.parse_to_datetime(tweet['created_at']),
                    tweet['in_reply_to_status_id'],
                    tweet['in_reply_to_user_id'], tweet['quoted_status_id'])
                if 'quoted_status_id' in tweet else
                (
                    tweet['id'], tweet['user_id'], tweet['text'], tweet['lang'], tweet['quote_count'],
                    tweet['reply_count'],
                    tweet['retweet_count'], tweet['favorite_count'], self.parse_to_datetime(tweet['created_at']),
                    tweet['in_reply_to_status_id'],
                    tweet['in_reply_to_user_id'], None)
                , tweets
            )
        )
        self.cur.executemany(insert_sql, vals)
        self.connection.commit()
        self.fast_insert_user_mentions(tweets)
        self.fast_insert_hashtags(tweets)

    def update_user(self, new_user, old_user):
        names = []
        vals = []
        fields = ['followers_count', 'friends_count']
        for field in fields:
            if new_user[field] > old_user[field]:
                names.append(field)
                vals.append(new_user[field])

        if len(names) > 0:
            update_sql = "UPDATE user SET "
            for name in names:
                update_sql += name + ' = ?, '
            update_sql = update_sql[:-2]
            update_sql += "WHERE id = " + str(old_user['id'])
            self.cur.execute(update_sql, tuple(vals))
            self.connection.commit()

    def insert_user(self, user):
        # print(user)
        id = user['id']

        select_sql = "SELECT id, followers_count, friends_count FROM user WHERE id = ?"
        self.cur.execute(select_sql, (id,))

        user_db = self.cur.fetchone()
        if user_db is not None:
            self.update_user(user, user_db)
        else:
            insert_sql = "INSERT INTO user " \
                         "(id, screen_name, location, followers_count, friends_count, created_at) VALUES " \
                         "(?, ?, ?, ?, ?, ?)"
            val = (user['id'], user['screen_name'], user['location'], user['followers_count'], user['friends_count'],
                   user['created_at'])
            self.cur.execute(insert_sql, val)
            self.connection.commit()

    def fast_insert_users(self, users):
        insert_sql = "INSERT INTO user (id, screen_name, location, followers_count, friends_count, created_at) " \
                     "values (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET " \
                     "followers_count= MAX(excluded.followers_count, user.followers_count)," \
                     "friends_count = MAX(excluded.friends_count, user.friends_count)"

        vals = list(map(lambda user: (
            user['id'], user['screen_name'], user['location'], user['followers_count'], user['friends_count'],
            self.parse_to_date(user['created_at'])), users))

        self.cur.executemany(insert_sql, vals)
        self.connection.commit()

    def insert_retweets(self, retweets):
        insert_sql = "INSERT OR IGNORE INTO retweet" \
                     " (id, user_id, tweet_id, created_at, reply_count, retweet_count, " \
                     "favorite_count, in_reply_to_status_id, in_reply_to_user_id) " \
                     "VALUES (?,?,?,?,?,?,?,?,?)"
        vals = list(map(lambda retweet: (
            retweet['id'], retweet['user_id'], retweet['tweet_id'], self.parse_to_datetime(retweet['created_at']),
            retweet['reply_count'], retweet['retweet_count'], retweet['favorite_count'],
            retweet['in_reply_to_status_id'], retweet['in_reply_to_user_id']), retweets))

        self.cur.executemany(insert_sql, vals)
        self.connection.commit()

    def update_users_country_code(self, users):
        update_sql = "UPDATE user SET country_code = ? WHERE id = ?"

        self.cur.executemany(update_sql, users)
        self.connection.commit()

    def update_translated(self, translated):
        update_sql = "UPDATE tweet " \
                     "SET text_eng = ? " \
                     "WHERE id == ?"
        vals = list(map(lambda translation: (
            translation[0], translation[1]
        ),translated))

        self.cur.executemany(update_sql, vals)
        self.connection.commit()

    def close_db(self):
        self.connection.close()
