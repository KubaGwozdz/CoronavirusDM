import os
import json


def parse_file(filename):
    with open(filename, 'rb+') as f:
        f.seek(-2, os.SEEK_END)
        f.truncate()

    with open(filename, "r+") as f:
        s = f.read()
        f.seek(0)
        f.write("[" + s)
        s = f.read()
        f.write(']')



# if __name__ == "__main__":
    # parse_file('./data/retweets/25_3_2020retweets.json')



