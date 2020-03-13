import json
import db_utils as db
import file_parser

rt_filenames = ['8_3_2020retweets.json']
tw_data_dir = './data/tweets/'
rt_data_dir = './data/retweets/'
usr_data_dir = './data/users/'

data_dir = rt_data_dir

if __name__ == "__main__":
    db= db.DBManager()
    for filename in rt_filenames:
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
