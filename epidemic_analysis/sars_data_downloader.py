import csv
from db_utils.data_inserter import DataInserter
import datetime
from epidemic_analysis import covid19_data_downloader


def parse_date(date):

    parsed_date = datetime.datetime.strptime(date, '%Y-%m-%d')

    return parsed_date


def insert_get_countries(dataset):

    parsed_countries = []
    countries = []

    for row in dataset[1:]:
        country = row[1]

        if country not in parsed_countries:
            parsed_countries.append(country)

            iso3 = covid19_data_downloader.get_iso3_country(country)
            state = ''
            population = None

            countries.append((country, iso3, state, population))

    inserter = DataInserter()

    return inserter.insert_country(countries)


def parse_csv_files(file_path):

    with open(file_path) as csv_file:

        reader = csv.reader(csv_file, delimiter=';')
        data = []
        for row in reader:
            data.append(row)

    countries_data = insert_get_countries(data)
    parsed_data = []

    for row in data[1:]:

        date = row[0]
        country = row[1]
        conf = int(row[2])
        death = int(row[3])
        recov = int(row[4])

        country_id = [row[0] for row in countries_data if row[1] == country][0]

        old_data = [row for row in parsed_data if country_id == row[0] and date == row[1]]

        if len(old_data) > 0:

            confs = [int(row[2]) for row in data if row[1] == country and row[0] == date]
            deaths = [int(row[3]) for row in data if row[1] == country and row[0] == date]
            recovs = [int(row[4]) for row in data if row[1] == country and row[0] == date]

            sum_confs = sum(confs)
            sum_deaths = sum(deaths)
            sum_recovs = sum(recovs)

            index = parsed_data.index(old_data[0])
            parsed_data[index] = (old_data[0][0], old_data[0][1], sum_confs, sum_deaths, sum_recovs)

        else:

            parsed_data.append((country_id,date,conf,death,recov))

    for row in parsed_data:
        date = row[1]
        parsed_date = parse_date(date)

        index = parsed_data.index(row)
        parsed_data[index] = (row[0], parsed_date, row[2], row[3], row[4])

    return parsed_data


if __name__ == "__main__":

     data_inserter = DataInserter()

     directory = '../data/SARS-03-data'
     file_path = directory + '/' + 'sars_2003_complete_dataset_clean.csv'

     data = parse_csv_files(file_path)
     # print(data)
     # data_inserter.insert_sars_data(data)