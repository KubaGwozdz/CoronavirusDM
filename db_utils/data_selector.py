from datetime import datetime

import country_converter as coco
import geolocation.us_states as states_mapper

from db_utils.db_manager import DBManager

from math import sqrt


class DataSelector(DBManager):
    def __init__(self, manager=None):
        if manager is None:
            super().__init__()
        else:
            self.connection = manager.connection
            self.cur = self.connection.cursor()
        self.graph_users = 1000
        # self.graph_users = 50
        self.graph_nodes = []
        self.is_executed = False

    def get_number_of_tweets_per_day(self):
        select_sql = "SELECT created_at , count(*) AS number FROM tweet " \
                     "WHERE created_at > '2020-03-07' and DATE(created_at) != '2020-03-17' and DATE(created_at) != '2020-04-29' " \
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
                     "WHERE created_at > '2020-03-07' and DATE(created_at) != '2020-03-17' and DATE(created_at) != '2020-04-29' " \
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
                     "WHERE t.created_at > '2020-03-07' and DATE(created_at) != '2020-03-17' and DATE(created_at) != '2020-04-29' " \
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

    def get_most_popular_hashtags(self, from_date=None, to_date=None):
        from_date, to_date = self.parse_from_to_date(from_date, to_date)

        select_sql = "SELECT h.text, count(*) AS number from hashtag AS h " \
                     "JOIN hashtag_tweet ht on h.id = ht.hashtag_id " \
                     "WHERE ht.hashtag_id in (SELECT hashtag_id FROM  hashtag_tweet ht " \
                     "JOIN tweet t on t.id = ht.tweet_id " \
                     "WHERE t.created_at >= '" + from_date + "' AND t.created_at <= '" + to_date + "' " \
                                                                                                   "GROUP BY ht.hashtag_id " \
                                                                                                   "ORDER BY count(*) DESC " \
                                                                                                   "LIMIT 10) " \
                                                                                                   "GROUP BY ht.hashtag_id ORDER BY number DESC"

        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row['text']] = row['number']
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

    def get_most_active_users(self, from_date=None, to_date=None):
        from_date, to_date = self.parse_from_to_date(from_date, to_date)

        select_sql = "SELECT u.screen_name, count(*) AS number FROM tweet t " \
                     "JOIN user u on t.user_id = u.id " \
                     "WHERE t.created_at >= '" + from_date + "' AND t.created_at <= '" + to_date + "' " \
                                                                                                   "GROUP BY t.user_id " \
                                                                                                   "ORDER BY count(*) DESC " \
                                                                                                   "LIMIT 10"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row['screen_name']] = row['number']
        return result

    def get_most_active_users_retweets(self, from_date=None, to_date=None):
        from_date, to_date = self.parse_from_to_date(from_date, to_date)

        select_sql = "SELECT u.screen_name, count(*) AS number FROM retweet t " \
                     "JOIN user u on t.user_id = u.id " \
                     "WHERE t.created_at >= '" + from_date + "' AND t.created_at <= '" + to_date + "' " \
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
                     "WHERE t.created_at > '2020-03-07' and t.created_at <> '2020-03-17' and t.created_at <> '2020-04-29' and u.id in " \
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

    def get_retweet_edges(self, from_date=None, to_date=None):
        from_date, to_date = self.parse_from_to_date(from_date, to_date)

        select_sql = "SELECT DISTINCT  r.user_id AS user_A, t.user_id AS user_B FROM retweet r " \
                     "JOIN tweet t on r.tweet_id = t.id " \
                     "WHERE t.user_id <> r.user_id AND r.created_at >= '" + from_date + "' AND r.created_at <= '" + to_date + "' AND t.user_id IN " + self.get_graph_nodes() + " AND r.user_id IN " + self.get_graph_nodes()
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        return data

    def get_quote_edges(self, from_date=None, to_date=None):
        from_date, to_date = self.parse_from_to_date(from_date, to_date)

        select_sql = "SELECT DISTINCT t_o.user_id AS user_A, t_q.user_id AS user_B FROM tweet t_o " \
                     "JOIN tweet t_q on t_q.quoted_status_id = t_o.id " \
                     "WHERE t_q.created_at >= '" + from_date + "' AND t_q.created_at <= '" + to_date + "' AND t_o.user_id IN " + self.get_graph_nodes() + " AND t_q.user_id IN " + self.get_graph_nodes()
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        return data

    def get_tw_lang_per_day(self):
        select_sql = "SELECT t.lang, DATE(t.created_at) AS date, count(*) AS number FROM tweet t " \
                     "WHERE DATE(t.created_at) <> '2020-03-17' AND DATE(t.created_at) <> '2020-04-29' AND DATE(t.created_at) > '2020-03-07' " \
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
                     "WHERE DATE(t.created_at) <> '2020-03-17' AND DATE(t.created_at) <> '2020-04-29' AND DATE(t.created_at) > '2020-03-07' AND u.screen_name in ('realDonaldTrump', 'BarackObama', 'WHO', 'MorawieckiM', 'Pontifex_es', 'UN', 'BorisJohnson', 'AndrzejDuda', 'PremierRP', 'MZ_GOV_PL', 'GIS_gov', 'GiuseppeConteIT', 'EmmanuelMacron', 'sanchezcastejon', 'GermanyDiplo') " \
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

    def get_tweets_to_translate(self, batch_size):
        select_sql = "SELECT t.id, t.text, t.lang FROM tweet t " \
                     "JOIN user u on u.id = t.user_id " \
                     "WHERE t.text_eng isnull AND t.lang != 'en' AND t.lang != 'und' AND t.lang IN ('it', 'pl') AND u.country_code IS NOT NULL AND t.sentiment_pol isnull " \
                     "AND t.created_at > '2020-03-07'"
        if not self.is_executed:
            self.cur.execute(select_sql)
            self.is_executed = True

        data = self.cur.fetchmany(batch_size)
        return data

    def get_tweets_to_analyze(self, batch_size):
        select_sql = "SELECT t.id, t.text FROM tweet t " \
                     "JOIN user u on u.id = t.user_id " \
                     "WHERE t.text_eng isnull AND t.lang == 'en' AND u.country_code IS NOT NULL AND t.sentiment_pol isnull " \
                     "AND t.created_at > '2020-03-07'"

        if not self.is_executed:
            self.cur.execute(select_sql)
            self.is_executed = True

        data = self.cur.fetchmany(batch_size)
        return data

    def get_tweets_per_country(self, from_date=None, to_date=None, divided_by_population=False):
        from_date, to_date = self.parse_from_to_date(from_date, to_date)

        select_sql = "SELECT u.country_code, count(*) AS number FROM tweet t " \
                     "JOIN user u on u.id = t.user_id " \
                     "WHERE u.country_code IS NOT NULL AND u.country_code <> 'und' AND t.created_at >= '" + from_date + "' AND t.created_at <= '" + to_date + "' " \
                                                                                                                                                              "GROUP BY u.country_code"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        result['country_code'] = []
        result['country_name'] = []
        result['number'] = []
        for row in data:
            cc = coco.convert(names=row['country_code'], to='ISO3')
            cn = coco.convert(cc, to='name_short')
            result['country_code'].append(cc)
            result['country_name'].append(cn)
            result['number'].append(row['number'])
        if divided_by_population:
            population = self.get_countries_populations(result['country_name'], all=True)
            for i in range(len(result['country_name'])):
                name = result['country_name'][i]
                if name in population.keys():
                    if population[name] is not None:
                        result['number'][i] /= population[name]
                    else:
                        result['number'][i] = 0
                else:
                    result['number'][i] = 0
        return result

    def get_sentiment_per_country(self, from_date=None, to_date=None):
        from_date, to_date = self.parse_from_to_date(from_date, to_date)

        select_sql = "SELECT u.country_code, avg(t.sentiment_pol) AS polarity FROM tweet t " \
                     "JOIN user u on u.id = t.user_id " \
                     "WHERE u.country_code IS NOT NULL AND u.country_code <> 'und' AND t.created_at >= '" + from_date + "' AND t.created_at <= '" + to_date + "' " \
                                                                                                                                                              "GROUP BY u.country_code"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        result['country_code'] = []
        result['country_name'] = []
        result['polarity'] = []
        for row in data:
            cc = coco.convert(names=row['country_code'], to='ISO3')
            cn = coco.convert(cc, to='name_short')
            result['country_code'].append(cc)
            result['country_name'].append(cn)
            result['polarity'].append(row['polarity'])

        return result

    def get_sentiment_per_state(self, from_date=None, to_date=None):
        from_date, to_date = self.parse_from_to_date(from_date, to_date)

        select_sql = "SELECT u.state_code, avg(t.sentiment_pol) AS polarity FROM tweet t " \
                     "JOIN user u on u.id = t.user_id " \
                     "WHERE u.country_code IS NOT NULL AND u.country_code = 'us' AND t.created_at >= '" + from_date + "' AND t.created_at <= '" + to_date + "' " \
                                                                                                                                                            "GROUP BY u.state_code"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        result['state_code'] = []
        result['state_name'] = []
        result['polarity'] = []
        for row in data:
            sc = row['state_code']
            sn = states_mapper.code_to_state(row['state_code'])
            result['state_code'].append(sc)
            result['state_name'].append(sn)
            result['polarity'].append(row['polarity'])
        return result

    def get_sentiment_per_country_per_day(self):
        select_sql = "SELECT u.country_code, avg(t.sentiment_pol) AS sentiment, AVG(t.sentiment_pol*t.sentiment_pol) - AVG(t.sentiment_pol)*AVG(t.sentiment_pol) AS variance, DATE(t.created_at) AS created_at from tweet t " \
                     "JOIN user u on u.id = t.user_id " \
                     "WHERE u.country_code IN ('us','pl', 'gb', 'it') AND t.created_at > '2020-03-07' AND t.created_at <> '2020-03-17' AND t.created_at <> '2020-04-29' " \
                     "GROUP BY u.country_code, DATE(t.created_at);"

        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()

        for row in data:
            cc = coco.convert(names=row['country_code'], to='ISO3')
            if cc not in result.keys():
                cn = coco.convert(cc, to='name_short')
                result[cc] = dict()
                result[cc]['dates'] = []
                result[cc]['sentiment'] = []
                result[cc]['stdev'] = []
                result[cc]['name'] = cn

            result[cc]['dates'].append(row['created_at'])
            sentiment = row['sentiment'] if row['sentiment'] is not None else 0
            stdev = sqrt(row['variance']) if row['variance'] is not None else 0
            result[cc]['sentiment'].append(sentiment)
            result[cc]['stdev'].append(stdev)
        return result

    def get_world_sentiment_per_day(self):
        select_sql = "SELECT avg(t.sentiment_pol) AS sentiment, AVG(t.sentiment_pol*t.sentiment_pol) - AVG(t.sentiment_pol)*AVG(t.sentiment_pol) AS variance, DATE(t.created_at) AS created_at from tweet t " \
                     "JOIN user u on u.id = t.user_id " \
                     "WHERE t.created_at > '2020-03-07' AND t.created_at <> '2020-03-17' AND t.created_at <> '2020-04-29' " \
                     "GROUP BY DATE(t.created_at);"

        self.cur.execute(select_sql)
        data = self.cur.fetchall()

        result = dict()
        result['dates'] = []
        result['sentiment'] = []
        result['stdev'] = []
        for row in data:
            sentiment = row['sentiment'] if row['sentiment'] is not None else 0
            stdev = sqrt(row['variance']) if row['variance'] is not None else 0
            result['dates'].append(row['created_at'])
            result['sentiment'].append(sentiment)
            result['stdev'].append(stdev)

        return result

    def get_sentiment_per_state_per_day(self):
        select_sql = "SELECT u.state_code, avg(t.sentiment_pol) AS sentiment, AVG(t.sentiment_pol*t.sentiment_pol) - AVG(t.sentiment_pol)*AVG(t.sentiment_pol) AS variance, DATE(t.created_at) AS created_at from tweet t " \
                     "JOIN user u on u.id = t.user_id " \
                     "WHERE u.country_code == 'us' AND u.state_code is not null AND t.created_at > '2020-03-07' AND t.created_at <> '2020-03-17' AND t.created_at <> '2020-04-29' " \
                     "GROUP BY u.state_code, DATE(t.created_at);"

        self.cur.execute(select_sql)
        data = self.cur.fetchall()

        result = dict()
        for row in data:
            sc = row['state_code']
            if sc not in result.keys():
                sn = states_mapper.code_to_state(row['state_code'])

                result[sc] = dict()
                result[sc]['dates'] = []
                result[sc]['sentiment'] = []
                result[sc]['stdev'] = []
                result[sc]['name'] = sn

            result[sc]['dates'].append(row['created_at'])
            sentiment = row['sentiment'] if row['sentiment'] is not None else 0
            stdev = sqrt(row['variance']) if row['variance'] is not None else 0
            result[sc]['sentiment'].append(sentiment)
            result[sc]['stdev'].append(stdev)

        select_sql = "SELECT avg(t.sentiment_pol) AS sentiment, AVG(t.sentiment_pol*t.sentiment_pol) - AVG(t.sentiment_pol)*AVG(t.sentiment_pol) AS variance, DATE(t.created_at) AS created_at from tweet t " \
                     "JOIN user u on u.id = t.user_id " \
                     "WHERE u.country_code == 'us' AND t.created_at > '2020-03-07' AND t.created_at <> '2020-03-17' AND t.created_at <> '2020-04-29' " \
                     "GROUP BY DATE(t.created_at) " \
                     "ORDER BY t.created_at;"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()

        result['us'] = dict()
        result['us']['sentiment'] = []
        result['us']['stdev'] = []
        result['us']['dates'] = []
        for row in data:
            sentiment = row['sentiment'] if row['sentiment'] is not None else 0
            stdev = sqrt(row['variance']) if row['variance'] is not None else 0
            result['us']['sentiment'].append(sentiment)
            result['us']['stdev'].append(stdev)
            result['us']['dates'].append(row['created_at'])

        return result

    def get_hashtags_cloud(self, from_date=None, to_date=None):
        from_date, to_date = self.parse_from_to_date(from_date, to_date)

        select_sql = "SELECT h.text, count(ht.hashtag_id) AS popularity  from hashtag h " \
                     "JOIN hashtag_tweet ht on h.id = ht.hashtag_id " \
                     "JOIN tweet t on ht.tweet_id = t.id " \
                     "WHERE t.created_at >= '" + from_date + "' AND t.created_at <= '" + to_date + "'" \
                                                                                                   "GROUP BY ht.hashtag_id " \
                                                                                                   "ORDER BY popularity desc " \
                                                                                                   "LIMIT 10000"
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row['text']] = row['popularity']
        return result


    # ----- EPIDEMIC MODEL -----
    def get_countries(self):
        select_sql = "SELECT id, name FROM country WHERE state == ''"
        self.cur.execute(select_sql)
        result = self.cur.fetchall()
        return result

    def get_country_id(self, country_name: str):
        select_sql = "SELECT id FROM country c WHERE c.state == '' AND c.name == '" + country_name + "' "
        self.cur.execute(select_sql)
        result = self.cur.fetchone()[0]
        return result

    def get_countries_populations(self, countries_names, state_name=None, all=False):
        countries_names_str = ', '.join(list(map(lambda country: "'" + country + "'", countries_names)))

        select_sql = "SELECT c.name AS country_name, c.population FROM country c "
        if not all:
            select_sql += " WHERE c.name in ( " + countries_names_str + " ) "
        if state_name is not None:
            select_sql += " AND c.state == '" + state_name + "' "
        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        for row in data:
            result[row['country_name']] = row['population']
        return result

    def get_epidemic_data_in(self, countries_names: list, columns: list, epidemic_name: str, to_date=None,
                             since_epidemy_start=False, state_name=None, from_date=None):
        if to_date is None:
            to_date = self.today
        else:
            to_date = self.parse_datetime_to_date(to_date)

        columns_str = ', '.join(columns)
        countries_str = ', '.join(list(map(lambda country: "'" + country + "'", countries_names)))

        select_sql = "SELECT c.name AS country_name, e.date, " + columns_str + " FROM " + epidemic_name + " e " \
                                                                                                          "JOIN COUNTRY c on c.id = e.country_id " \
                                                                                                          "WHERE c.name in (" + countries_str + ") AND date <= '" + to_date + "' "
        if state_name is None:
            state_name = ''
        select_sql += " AND c.state == '" + state_name + "' "
        if from_date is not None:
            select_sql += " AND e.date >= '" + from_date + "' "

        select_sql += " ORDER BY e.date ASC"

        self.cur.execute(select_sql)
        raw_data = self.cur.fetchall()

        data = dict()
        data['dates'] = []
        population = self.get_countries_populations(countries_names, state_name)
        for country_name in countries_names:
            data[country_name] = dict()
            data[country_name]['population'] = population[country_name]
            for column in columns:
                data[country_name][column] = []

        epidemy_started = not since_epidemy_start
        for row in raw_data:
            if epidemy_started:
                if row['date'] not in data['dates']:
                    data['dates'].append(row['date'])
                for column in columns:
                    data[row['country_name']][column].append(row[column])
            else:
                for c in columns:
                    if row[c] > 0:
                        epidemy_started = True
        return data

    def get_polish_tweets(self, batch_size):
        select_sql = "SELECT t.id, t.text, date(t.created_at) FROM tweet t " \
                     "JOIN  user u on u.id == t.user_id " \
                     "WHERE t.lang == 'pl' and date(t.created_at) > date('2020-03-07') and " \
                     "date(t.created_at) <> date('2020-03-17') AND date(t.created_at) <> date('2020-04-29')" \
                     "and t.sentiment_pol isnull"

        if not self.is_executed:
            self.cur.execute(select_sql)
            self.is_executed = True

        result = self.cur.fetchmany(batch_size)
        return result

    def get_polish_tweets_sentiment(self):
        select_sql = "SELECT avg(t.sentiment_pol) AS sentiment, " \
                     "AVG(t.sentiment_pol*t.sentiment_pol) - AVG(t.sentiment_pol)*AVG(t.sentiment_pol) AS variance, " \
                     "DATE(t.created_at) AS created_at from tweet t " \
                     "WHERE t.sentiment_pol is not null AND lang = 'pl' " \
                     "GROUP BY DATE(t.created_at);"

        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        result['sentiment'] = []
        result['stdev'] = []
        result['date'] = []

        for row in data:
            result['sentiment'].append(row['sentiment'])
            result['stdev'].append(row['variance'])
            result['date'].append(row['created_at'])

        return result

    def get_number_daily_polish_tweets(self):
        select_sql = "SELECT count(*) AS number, date(t.created_at) as created_at FROM tweet t " \
                     "WHERE t.lang == 'pl' and date(t.created_at) > date('2020-03-07') and " \
                     "date(t.created_at) <> date('2020-03-17') AND date(t.created_at) <> date('2020-04-29')" \
                     "GROUP BY date(created_at)"

        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        result = dict()
        result['number'] = []
        result['date'] = []

        for row in data:
            result['number'].append(row['number'])
            result['date'].append(row['created_at'])

        return result

    def get_tweets_per_day_in(self, country, state, from_date, to_date):
        select_sql = "SELECT DATE(t.created_at) AS date, count(*) AS number, count(case when t.sentiment_pol > 0 THEN t.sentiment_pol end) AS positive, count(case when t.sentiment_pol <= 0 then t.sentiment_pol end) AS negative FROM tweet t " \
                     "JOIN user u on t.user_id = u.id " \
                     "WHERE t.created_at >= '" + from_date + "' AND t.created_at <= '" + to_date + "' AND u.country_code = '" + country + "' "
        if state is not None:
            select_sql += "AND u.state_code = '" + state + "'"
        select_sql += " GROUP BY DATE(t.created_at) ORDER BY date "

        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        total = []
        positive = []
        negative = []

        for row in data:
            total.append(row['number'])
            positive.append(row['positive'])
            negative.append(row['negative'])
        return total, positive, negative

    def get_number_of_users_in(self, country_code, state=None):
        select_sql = "SELECT count(*) AS number FROM user WHERE country_code = '" + country_code + "' "
        if state is not None:
            select_sql += " AND state_code = '" + state + "'"

        self.cur.execute(select_sql)
        data = self.cur.fetchall()
        return data[0]['number']


    def get_sentiment_data_in(self, country_code: str, to_date=None, since_epidemy_start=False):

        if to_date is None:
            to_date = self.today
        else:
            to_date = self.parse_datetime_to_date(to_date)

        code_str = "'" + country_code + "'"


        select_all = "SELECT count(*) AS users, data FROM ( " + \
                         "SELECT count(*), DATE(t.created_at) AS data " + \
                         "FROM tweet t " + \
                         "JOIN user u ON t.user_id = u.id " + \
                         "WHERE t.sentiment_pol is not null " + \
                         "AND u.country_code = " + code_str + " " + \
                         "GROUP BY DATE(t.created_at), t.user_id) " + \
                     "GROUP BY data "

        select_positive = "SELECT count(*) AS users, data FROM ( " + \
                         "SELECT count(*), DATE(t.created_at) AS data " + \
                         "FROM tweet t " + \
                         "JOIN user u ON t.user_id = u.id " + \
                         "WHERE t.sentiment_pol > 0.1 " + \
                         "AND u.country_code = " + code_str + " " + \
                         "GROUP BY DATE(t.created_at), t.user_id) " + \
                     "GROUP BY data "
        select_negative = "SELECT count(*) AS users, data FROM ( " + \
                         "SELECT count(*), DATE(t.created_at) AS data " + \
                         "FROM tweet t " + \
                         "JOIN user u ON t.user_id = u.id " + \
                         "WHERE t.sentiment_pol < -0.1 " + \
                         "AND u.country_code = " + code_str + " " + \
                         "GROUP BY DATE(t.created_at), t.user_id) " + \
                     "GROUP BY data "
        select_neutral = "SELECT count(*) AS users, data FROM ( " + \
                         "SELECT count(*), DATE(t.created_at) AS data " + \
                         "FROM tweet t " + \
                         "JOIN user u ON t.user_id = u.id " + \
                         "WHERE t.sentiment_pol <= 0.1 AND t.sentiment_pol >= -0.1 " + \
                         "AND u.country_code = " + code_str + " " + \
                         "GROUP BY DATE(t.created_at), t.user_id) " + \
                     "GROUP BY data "


        self.cur.execute(select_all)
        raw_data_all = self.cur.fetchall()

        self.cur.execute(select_positive)
        raw_data_positive = self.cur.fetchall()

        self.cur.execute(select_negative)
        raw_data_negative = self.cur.fetchall()

        self.cur.execute(select_neutral)
        raw_data_neutral = self.cur.fetchall()

        data = dict()
        data['dates'] = []
        data[country_code] = dict()
        data[country_code]["users"] = []
        data[country_code]["negative"] = []
        data[country_code]["positive"] = []
        data[country_code]["neutral"] = []

        epidemy_started = not since_epidemy_start
        for row in raw_data_all:
            data[country_code]["users"].append((row["users"], row["data"]))
        for row in raw_data_negative:
            data[country_code]["negative"].append((row["users"], row["data"]))
        for row in raw_data_positive:
            data[country_code]["positive"].append((row["users"], row["data"]))
        for row in raw_data_neutral:
            data[country_code]["neutral"].append((row["users"], row["data"]))

        return data
