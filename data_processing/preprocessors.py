import re
from nltk.tokenize import word_tokenize
from string import punctuation
from nltk.corpus import stopwords


class TweetsPreprocessor:
    def __init__(self):
        self._stopwords = set(stopwords.words('english') + list(punctuation) + ['AT_USER', 'URL'])

    def process_tweets(self, list_of_tweets):
        processedTweets = []
        for tweet in list_of_tweets:
            processedTweets.append(self.process_tweet(tweet['text']))
        return processedTweets

    def process_tweet(self, tweet):
        tweet = tweet.lower()  # convert text to lower-case
        tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet)  # remove URLs
        tweet = re.sub('@[^\s]+', 'AT_USER', tweet)  # remove usernames
        tweet = re.sub(r'#([^\s]+)', r'\1', tweet)  # remove the # in #hashtag
        tweet = word_tokenize(tweet)  # remove repeated characters (helloooooooo into hello)
        return ' '.join([word for word in tweet if word not in self._stopwords])

    def process_tweet_to_translation(self, tweet):
        tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet)  # remove URLs
        tweet = re.sub('@[^\s]+', 'AT_USER', tweet)  # remove usernames
        tweet = re.sub(r'#([^\s]+)', r'\1', tweet)  # remove the # in #hashtag
        return tweet

    def process_translated_to_analyze(self, tweet):
        tweet = word_tokenize(tweet)  # remove repeated characters (helloooooooo into hello)
        return ' '.join([word for word in tweet if word not in self._stopwords])


