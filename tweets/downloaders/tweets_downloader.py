from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json
from dateutil.parser import parse

from tweets.downloaders.passwords import access_token, access_token_secret, consumer_key, consumer_secret

tracklist = ['#Coronvirus', '#COVID19', '#COVID_19', '#COVID-19', '#2019nCov', '#CoronaVirusUpdate', '#CoronaOutbreak',
             '#Coronavirus', '#Koronawirus']


class CoronavirusListener(StreamListener):

    def parse_tweets(self, data):
        tweet = dict()
        tweet_fields = ['created_at', 'id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'quote_count',
                        'reply_count', 'retweet_count', 'favorite_count', 'lang', 'is_quote_status']

        for field in tweet_fields:
            tweet[field] = data[field]

        tweet['user_id'] = data['user']['id']

        if 'extended_tweet' in data:
            tweet['text'] = data['extended_tweet']['full_text']
            tweet['hashtags'] = data['extended_tweet']['entities']['hashtags']
            tweet['user_mentions'] = data['extended_tweet']['entities']['user_mentions']
        else:
            tweet['text'] = data['text']
            tweet['hashtags'] = data['entities']['hashtags']
            tweet['user_mentions'] = data['entities']['user_mentions']

        if data['is_quote_status']:
            tweet['quoted_status_id'] = data['quoted_status_id']
            res = (tweet, self.parse_user(data))
            if ('quoted_status' not in data):
                return [res]
            return [res] + self.parse_tweets(data['quoted_status'])

        res = (tweet, self.parse_user(data))
        return [res]

    def parse_user(self, data):
        user = dict()
        user_fields = ['id', 'screen_name', 'location', 'followers_count', 'friends_count', 'created_at']

        for field in user_fields:
            user[field] = data['user'][field]

        return user

    def on_data(self, data):
        try:
            loaded_tweet = json.loads(data)

            if loaded_tweet['user']['location'] != None:
                date = parse(loaded_tweet['created_at'])
                date_str = str(date.day) + '_' + str(date.month) + '_' + str(date.year)

                users_filename = date_str + 'users.json'
                tweets_filename = date_str + 'tweets.json'
                retweets_filename = date_str + 'retweets.json'

                if 'retweeted_status' in loaded_tweet:  # is a retweet
                    retweet_fields = ['created_at', 'id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'quote_count',
                                      'reply_count', 'retweet_count', 'favorite_count']
                    retweet = dict()

                    for field in retweet_fields:
                        retweet[field] = loaded_tweet[field]

                    retweet['tweet_id'] = loaded_tweet['retweeted_status']['id']
                    retweet['user_id'] = loaded_tweet['user']['id']

                    tweets_users = self.parse_tweets(loaded_tweet['retweeted_status'])
                    retweet_user = self.parse_user(loaded_tweet)

                    with open(users_filename, 'a') as f:
                        f.write(json.dumps(retweet_user))
                        f.write(',\n')

                    with open(retweets_filename, 'a') as f:
                        f.write(json.dumps(retweet))
                        f.write(',\n')

                else:
                    tweets_users = self.parse_tweets(loaded_tweet)

                with open(tweets_filename, 'a') as tweets_file:
                    with open(date_str + 'users.json', 'a') as users_file:
                        for (tweet, user) in tweets_users:
                            tweets_file.write(json.dumps(tweet))
                            tweets_file.write(',\n')

                            users_file.write(json.dumps(user))
                            users_file.write(',\n')

        except BaseException as e:
            print(str(e))
            return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == '__main__':
    listener = CoronavirusListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)
    while True:
        try:
            stream.filter(track=tracklist)
        except BaseException as e:
            print(str(e))
            continue
