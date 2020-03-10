import tweepy
from tweets_downloader import CoronavirusListener, tracklist, access_token, access_token_secret, consumer_key, consumer_secret
import json

# l = CoronavirusListener()
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)


l = CoronavirusListener()

for search in tracklist:
    for tweet in tweepy.Cursor(api.search, q=search, count=100, until="2020-03-07", include_rts=True).items():
        l.on_data(json.dumps(tweet._json))
