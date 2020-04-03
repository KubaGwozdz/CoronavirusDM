from datetime import datetime

from db_utils.db_manager import DBManager


class DataSelector(DBManager):
    def __init__(self, manager=None):
        if manager is None:
            super().__init__()
        else:
            self.connection = manager.connection
            self.cur = self.connection.cursor()
        self.graph_users = 500
        self.graph_nodes = []
        self.is_executed = False

    def get_number_of_tweets_per_day(self):
        select_sql = "SELECT created_at , count(*) AS number FROM tweet " \
                     "WHERE created_at > '2020-03-07' and DATE(created_at) != '2020-03-17' " \
                     "GROUP BY DATE(created_at)"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            date = row['created_at']
            num = row['number']
            result[date] = num
        return result

    def get_number_of_retweets_per_day(self):
        select_sql = "SELECT created_at , count(*) AS number FROM retweet " \
                     "WHERE created_at > '2020-03-07' and DATE(created_at) != '2020-03-17' " \
                     "GROUP BY DATE(created_at)"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            date = row['created_at']
            num = row['number']
            result[date] = num
        return result

    def get_most_popular_hashtags_per_day(self):
        select_sql = "SELECT hashtag.text, t.created_at, count(*) AS number from hashtag " \
                     "JOIN hashtag_tweet h_t on hashtag.id = h_t.hashtag_id " \
                     "JOIN tweet t on h_t.tweet_id = t.id " \
                     "WHERE t.created_at > '2020-03-07' and DATE(created_at) != '2020-03-17' " \
                     "AND h_t.hashtag_id in (SELECT hashtag_id FROM hashtag_tweet " \
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

    def get_most_popular_hashtags(self):
        select_sql = "SELECT h.text, count(*) AS number from hashtag AS h " \
                     "JOIN hashtag_tweet ht on h.id = ht.hashtag_id " \
                     "WHERE ht.hashtag_id in (SELECT hashtag_id FROM  hashtag_tweet " \
                     "GROUP BY hashtag_id " \
                     "ORDER BY count(*) DESC " \
                     "LIMIT 10) " \
                     "GROUP BY ht.hashtag_id"

        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row['text']] = row['number']
        print(result)
        return result

    def get_most_popular_users_followers(self):
        select_sql = "SELECT screen_name, followers_count " \
                     "FROM user ORDER BY followers_count DESC LIMIT 100"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row['screen_name']] = row['followers_count']
        return result

    def get_most_popular_users_friends(self):
        select_sql = "SELECT screen_name, friends_count " \
                     "FROM user ORDER BY friends_count DESC LIMIT 100"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row['screen_name']] = row['friends_count']
        return result

    def get_most_active_users(self):
        select_sql = "SELECT u.screen_name, count(*) AS number FROM tweet t " \
                     "JOIN user u on t.user_id = u.id " \
                     "WHERE t.created_at > '2020-03-07' " \
                     "GROUP BY t.user_id " \
                     "ORDER BY count(*) DESC " \
                     "LIMIT 10"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row['screen_name']] = row['number']
        return result

    def get_most_active_users_retweets(self):
        select_sql = "SELECT u.screen_name, count(*) AS number FROM retweet t " \
                     "JOIN user u on t.user_id = u.id " \
                     "WHERE t.created_at > '2020-03-07' " \
                     "GROUP BY t.user_id " \
                     "ORDER BY count(*) DESC " \
                     "LIMIT 10"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row['screen_name']] = row['number']
        return result

    def get_most_active_users_per_day_tweets(self):
        select_sql = "SELECT u.screen_name, t.created_at, count(*) AS number FROM tweet t " \
                     "JOIN user u on t.user_id = u.id " \
                     "WHERE t.created_at > '2020-03-07' and t.created_at <> '2020-03-17' and u.id in " \
                     "(SELECT t.user_id from tweet t " \
                     "GROUP BY t.user_id " \
                     "ORDER BY count(*) DESC " \
                     "LIMIT 10) " \
                     "GROUP BY t.user_id, date(t.created_at) " \
                     "ORDER BY t.created_at"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()

        result = dict()
        for row in data:
            if row['screen_name'] not in result:
                result[row['screen_name']] = dict()
                result[row['screen_name']]['dates'] = []
                result[row['screen_name']]['numbers'] = []

            result[row['screen_name']]['dates'].append(datetime.strptime(row['created_at'], "%Y-%m-%d %H:%M:%S").date())
            result[row['screen_name']]['numbers'].append(row['number'])
        return result

    def get_graph_nodes(self):
        if len(self.graph_nodes) == 0:
            select_sql = "SELECT t.user_id AS id from tweet t " \
                         "GROUP BY t.user_id " \
                         "ORDER BY sum(t.retweet_count) DESC " \
                         "LIMIT " + str(self.graph_users)
            self.cur.execute(select_sql)
            data = self.cur.fetchall()
            data = [u['id'] for u in data]
            self.graph_nodes = str(tuple(data))
            return self.graph_nodes
        else:
            return self.graph_nodes

    def get_user_nodes(self):
        select_sql = "SELECT u.id, u.screen_name, u.followers_count, u.friends_count FROM user u " \
                     "WHERE u.id IN" + self.get_graph_nodes()
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        return data

    def get_nodes_weights(self):
        select_sql = "SELECT t.user_id AS user_id, sum(t.quote_count) AS quotes, sum(t.reply_count) AS replies, sum(t.retweet_count) AS retweets FROM tweet t " \
                     "WHERE t.user_id IN " + self.get_graph_nodes() + " GROUP BY t.user_id;"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        return data

    def get_followers_weights(self):
        select_sql = "SELECT u.id AS user_id, u.followers_count AS number FROM user u " \
                     "WHERE u.id IN " + self.get_graph_nodes()
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        return data

    def get_usermentions_weights(self):
        select_sql = "SELECT um.user_id AS user_id, count(*) AS number FROM user_mentions um " \
                     "WHERE um.user_id IN " + self.get_graph_nodes() + " GROUP BY um.user_id;"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        return data

    def get_retweet_edges(self):
        select_sql = "SELECT DISTINCT  r.user_id AS user_A, t.user_id AS user_B FROM retweet r " \
                     "JOIN tweet t on r.tweet_id = t.id " \
                     "WHERE t.user_id <> r.user_id AND t.user_id IN " + self.get_graph_nodes() + " AND r.user_id IN " + self.get_graph_nodes()
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        return data

    def get_quote_edges(self):
        select_sql = "SELECT DISTINCT t_o.user_id AS user_A, t_q.user_id AS user_B FROM tweet t_o " \
                     "JOIN tweet t_q on t_q.quoted_status_id = t_o.id " \
                     "WHERE t_o.user_id IN " + self.get_graph_nodes() + " AND t_q.user_id IN " + self.get_graph_nodes()
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        return data

    def get_tw_lang_per_day(self):
        select_sql = "SELECT t.lang, DATE(t.created_at) AS date, count(*) AS number FROM tweet t " \
                     "WHERE DATE(t.created_at) <> '2020-03-17' AND DATE(t.created_at) > '2020-03-07' " \
                     "GROUP BY DATE (t.created_at), t.lang;"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()

        result = dict()
        for row in data:
            if row['lang'] not in result:
                result[row['lang']] = dict()
                result[row['lang']]['dates'] = []
                result[row['lang']]['numbers'] = []

            result[row['lang']]['dates'].append(datetime.strptime(row['date'], "%Y-%m-%d").date())
            result[row['lang']]['numbers'].append(row['number'])
        return result

    def get_influencers_per_day(self):
        select_sql = "SELECT u.screen_name, DATE(t.created_at) AS date, count(*) AS number, sum(t.retweet_count) AS retweet_count, sum(t.reply_count) AS reply_count, sum(t.quote_count) AS quote_count from tweet t " \
                     "JOIN user u ON u.id = t.user_id " \
                     "WHERE DATE(t.created_at) <> '2020-03-17' AND DATE(t.created_at) > '2020-03-07' AND u.screen_name in ('realDonaldTrump', 'BarackObama', 'WHO', 'MorawieckiM', 'Pontifex_es', 'UN', 'BorisJohnson', 'AndrzejDuda', 'PremierRP', 'MZ_GOV_PL', 'GIS_gov', 'GiuseppeConteIT', 'EmmanuelMacron', 'sanchezcastejon', 'GermanyDiplo') " \
                     "GROUP BY DATE(t.created_at), t.user_id"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()

        result = dict()
        for row in data:
            if row['screen_name'] not in result:
                result[row['screen_name']] = dict()
                result[row['screen_name']]['dates'] = []
                result[row['screen_name']]['numbers'] = []
                result[row['screen_name']]['retweet_count'] = []
                result[row['screen_name']]['reply_count'] = []
                result[row['screen_name']]['quote_count'] = []

            result[row['screen_name']]['dates'].append(datetime.strptime(row['date'], "%Y-%m-%d").date())
            result[row['screen_name']]['numbers'].append(row['number'])
            result[row['screen_name']]['retweet_count'].append(row['retweet_count'])
            result[row['screen_name']]['reply_count'].append(row['reply_count'])
            result[row['screen_name']]['quote_count'].append(row['quote_count'])
        return result

    def get_newly_created_accounts(self):
        select_sql = "SELECT u.created_at, count(*) AS number FROM user u " \
                     "WHERE u.created_at >= '2020-01-01' " \
                     "GROUP BY u.created_at"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()

        result = dict()
        for row in data:
            result[row['created_at']] = row['number']
        return result

    def get_newly_created_accounts_pattern(self):
        select_sql = "SELECT u.created_at, count(*) AS number FROM user u " \
                     "WHERE u.created_at > '2019-12-31' " \
                     "AND (u.screen_name LIKE '%cov%' OR u.screen_name LIKE '%corona%' OR u.screen_name LIKE '%virus%') " \
                     "GROUP BY u.created_at"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()

        result = dict()
        for row in data:
            result[row['created_at']] = row['number']
        return result

    def get_users_to_localize(self, batch_size):
        select_sql = "SELECT u.id, u.location FROM user u " \
                     "WHERE u.country_code ISNULL AND u.location IS NOT NULL " \
                     "ORDER BY u.followers_count DESC ;"
        if not self.is_executed:
            self.cur.execute(select_sql)
            self.is_executed = True

        data = self.cur.fetchmany(batch_size)
        return data

    def get_tweets_to_translate(self):
        select_sql = "SELECT t.id, t.text FROM tweet t " \
                     "WHERE t.text_eng isnull AND t.lang != 'en' AND t.lang != 'und' " \
                     "AND t.created_at > '2020-03-07'" \
                     "LIMIT 10"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()

        result = dict()
        for row in data:
            result[row['id']] = row['text']
        return result
