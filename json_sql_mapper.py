import json
import db_utils as db

rt_filenames = ['13_3_2020retweets.json', '14_3_2020retweets.json']
tw_data_dir = './data/tweets/'
rtw_data_dir = './data/retweets/'
usr_data_dir = './data/users/'

data_dir = rtw_data_dir

if __name__ == "__main__":
    db= db.DBManager()
    for filename in rt_filenames:
        print(filename)
        with open(data_dir + filename) as f:
            data = json.load(f)
            if 'retweets' in data_dir:
                db.insert_retweets(data)
            elif 'users' in data_dir:
                    db.fast_insert_users(data)
            else:
                db.fast_insert_tweets(data)
