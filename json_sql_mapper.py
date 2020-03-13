import json
import db_utils as db
import file_parser

tweet_filenames = ['11_3_2020retweets.json']
data_dir = './data/retweets/'

if __name__ == "__main__":
    db = db.DBManager()
    for filename in tweet_filenames:
        with open(data_dir + filename) as f:
            data = json.load(f)
            if 'retweets' in data_dir:
                db.insert_retweets(data)
            elif 'users' in data_dir:
                for user in data:
                    db.insert_user(user)
            else:
                for tweet in data:
                    db.insert_tweet(tweet)
