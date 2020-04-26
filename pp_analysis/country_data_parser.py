import csv
from db_utils.data_inserter import DataInserter
from db_utils.data_selector import DataSelector


def get_country_population_density(file_path):

    with open(file_path) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        data = []
        for row in reader:
            data.append(row)

    data_selector = DataSelector()

    parsed_data = []

    country_data = data_selector.get_countries()

    for row in data[1:]:

        country_name = row[0]
        population = row[1]
        density = row[4]

        country_id = [c[0] for c in country_data if country_name == c[1]]

        if len(country_id) > 0:

            parsed_data.append((int(population),int(density),country_id[0]))

    return parsed_data


if __name__ == '__main__':

    data_inserter = DataInserter()

    directory = '../country_data'
    file_path = directory + '/' + 'population_by_country_2020.csv'

    data = get_country_population_density(file_path)
    # print(data)
    # data_inserter.update_country_population(data)