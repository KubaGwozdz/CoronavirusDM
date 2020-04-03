from googletrans import Translator
from db_utils.data_selector import DataSelector


def translate(db):
    tweets = db.get_tweets_to_translate()

    translator = Translator()
    translated = dict()

    for tweet_id in tweets.keys():
        translation = translator.translate(tweets[tweet_id], dest='en')
        translated[tweet_id] = translation.text

    return translated


if __name__ == "__main__":
    db = DataSelector()

    te = translate(db)
    print(te)
