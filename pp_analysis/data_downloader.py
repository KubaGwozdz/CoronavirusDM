import csv
import os
from db_utils.data_inserter import DataInserter
import datetime

def parse_date(date):

    try:
        parsed_date = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        try:
            parsed_date = datetime.datetime.strptime(date, '%Y/%m/%d %H:%M')
        except ValueError:
            try:
                parsed_date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    date = '0' + date
                    parsed_date = datetime.datetime.strptime(date, '%m/%d/%y %H:%M')
                except ValueError:
                    parsed_date = datetime.datetime.strptime(date, '%m/%d/%Y %H:%M')

    return parsed_date


def parse_csv_file_before_1_march(path):    # csv files have different labels

    with open(path) as csv_file:

        reader = csv.reader(csv_file, delimiter=',')
        data = []
        for row in reader:
            data.append(row)

    parsed_data = []

    for row in data:
        if row[0] != '' and 'Province/State' not in row[0]:
            state = row[0]
            tmp = [r for r in data if r[0] == state]
            sum_confirmed = sum([int(c[3]) for c in tmp if c[3] != ''])
            sum_deaths = sum([int(c[4]) for c in tmp if c[4] != ''])
            sum_recovered = sum([int(c[5]) for c in tmp if c[5] != ''])
            country = tmp[0][1]
            state = tmp[0][0]
            date = parse_date(tmp[0][2])
            tmp = (country, state, date, '', '',sum_confirmed,sum_deaths,sum_recovered,None)
            if tmp not in parsed_data:
                parsed_data.append(tmp)
        elif 'Province/State' not in row[0]:
            country = row[1]
            state = row[0]
            date = parse_date(row[2])
            confirmed = 0 if row[3] == '' else int(row[3])
            deaths = 0 if row[4] == '' else int(row[4])
            recovered = 0 if row[5] == '' else int(row[5])
            tmp = (country, state, date, '', '', confirmed, deaths, recovered, None)
            if tmp not in parsed_data:
                parsed_data.append(tmp)

    return parsed_data[1:]

def parse_csv_file_1_to_21_march(path):
    with open(path) as csv_file:

        reader = csv.reader(csv_file, delimiter=',')
        data = []
        for row in reader:
            data.append(row)

    parsed_data = []

    for row in data:
        if row[0] != '' and 'Province/State' not in row[0]:
            state = row[0]
            tmp = [r for r in data if r[0] == state]
            sum_confirmed = sum([int(c[3]) for c in tmp if c[3] != ''])
            sum_deaths = sum([int(c[4]) for c in tmp if c[4] != ''])
            sum_recovered = sum([int(c[5]) for c in tmp if c[5] != ''])
            country = tmp[0][1]
            state = tmp[0][0]
            date = parse_date(tmp[0][2])
            lat = [l[6] for l in tmp if l[6] != '']
            long = [l[7] for l in tmp if l[7] != '']
            if len(lat) != 0:
                lat = lat[0]
            else:
                lat = ''
            if len(long) != 0:
                long = long[0]
            else:
                long = ''
            tmp = (country, state, date, lat, long,sum_confirmed,sum_deaths,sum_recovered, None)
            if tmp not in parsed_data:
                parsed_data.append(tmp)
        elif 'Province/State' not in row[0]:
            country = row[1]
            state = row[0]
            date = parse_date(row[2])
            lat = row[6]
            long = row[7]
            confirmed = 0 if row[3] == '' else int(row[3])
            deaths = 0 if row[4] == '' else int(row[4])
            recovered = 0 if row[5] == '' else int(row[5])
            tmp = (country, state, date, lat, long, confirmed, deaths, recovered, None)
            if tmp not in parsed_data:
                parsed_data.append(tmp)

    return parsed_data[1:]


def parse_csv_file_after_21_march(path):

    with open(path) as csv_file:

        reader = csv.reader(csv_file, delimiter=',')
        data = []
        for row in reader:
            if row[0] != 'unassigned':
                data.append(row[2:-1])

    parsed_data = []

    for row in data[1:]:
        if row[0] != '' and 'Province_State' not in row[0]:
            state = row[0]
            tmp = [r for r in data if r[0] == state]
            sum_confirmed = sum([int(c[5]) for c in tmp if c[5] != ''])
            sum_deaths = sum([int(c[6]) for c in tmp if c[6] != ''])
            sum_recovered = sum([int(c[7]) for c in tmp if c[7] != ''])
            sum_active = sum([int(c[8]) for c in tmp if c[8] != ''])
            country = tmp[0][1]
            state = tmp[0][0]
            date = parse_date(tmp[0][2])
            lat = [l[3] for l in tmp if l[3] != '']
            long = [l[4] for l in tmp if l[4] != '']
            if len(lat) != 0:
                lat = lat[0]
            else:
                lat = ''
            if len(long) != 0:
                long = long[0]
            else:
                long = ''
            tmp = (country, state, date, lat, long,sum_confirmed,sum_deaths,sum_recovered,sum_active)
            if tmp not in parsed_data:
                parsed_data.append(tmp)
        elif 'Province_State' not in row[0]:
            country = row[1]
            state = row[0]
            date = parse_date(row[2])
            lat = row[3]
            long = row[4]
            confirmed = 0 if row[5] == '' else int(row[5])
            deaths = 0 if row[6] == '' else int(row[6])
            recovered = 0 if row[7] == '' else int(row[7])
            active = 0 if row[8] == '' else int(row[8])
            tmp = (country, state, date, lat, long,confirmed,deaths,recovered,active)
            if tmp not in parsed_data:
                parsed_data.append(tmp)

    return parsed_data[1:]



if __name__ == "__main__":

 data_inserter = DataInserter()

 directory = '/Users/juliazur/Documents/AGH/8semestr/ED/project/Covid-19-data/csse_covid_19_data/csse_covid_19_daily_reports'
 for filename in os.listdir(directory):
     if filename.endswith('.csv'):
         if filename.startswith("01") or filename.startswith("02"):
             file = directory + '/' + filename
             data = parse_csv_file_before_1_march(file)
             data_inserter.insert_epidemic_data(data)
         elif filename.startswith("03-0") or filename.startswith("03-1") or filename.startswith("03-20") or filename.startswith("03-21"):
             file = directory + '/' + filename
             data = parse_csv_file_1_to_21_march(file)
             data_inserter.insert_epidemic_data(data)
         else:
             file = directory + '/' + filename
             data = parse_csv_file_after_21_march(file)
             data_inserter.insert_epidemic_data(data)
