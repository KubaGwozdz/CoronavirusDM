import json
import db_utils as db
import file_parser

tweet_filenames = ['7_3_2020tweets.json']
data_dir = './data/tweets/'

if __name__ == "__main__":
    db = db.DBManager()
    for filename in tweet_filenames:
        with open(data_dir + filename) as f:
            data = json.load(f)
            for tweet in data:
                # print(tweet)
                db.insert_tweet(tweet)
                # db.close_db()
