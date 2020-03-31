import json
import os
from db_utils.data_inserter import DataInserter

tw_data_dir = '../../data/tweets/'
rtw_data_dir = '../../data/retweets/'
usr_data_dir = '../../data/users/'

data_dir = usr_data_dir

if __name__ == "__main__":
    db = DataInserter()
    filenames = []
    for filename in os.listdir(data_dir):
        if '.DS' not in filename and '_in' not in filename:
            filenames.append(filename)

    for filename in filenames:
        print(filename)
        with open(data_dir + filename) as f:
            data = json.load(f)
            if 'retweets' in data_dir:
                db.insert_retweets(data)
            elif 'users' in data_dir:
                db.fast_insert_users(data)
            else:
                db.fast_insert_tweets(data)
