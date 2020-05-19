import csv
from db_utils.data_inserter import DataInserter
import datetime
from geolocation import us_states


def parse_date(date):

    try:
        parsed_date = datetime.datetime.strptime(date, '%m/%d/%y')
    except ValueError:
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


def get_iso3_country(country):
    iso3_country = None

    with open('../data/Covid-19-data/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv') as csv_file:

        reader = csv.reader(csv_file, delimiter=',')
        data = []
        for row in reader:
            data.append(row)

    for row in data[1:]: # first row - column names
        if country == row[7]:
            iso3_country = row[2]

    return iso3_country


def insert_get_countries(global_conf, us_deaths):

    parsed_countries = []
    parsed_states = []
    countries = []

    for row in global_conf[1:]:
        country = row[1]

        if country not in parsed_countries:
            parsed_countries.append(country)

            iso3 = get_iso3_country(country)
            state = ''
            population = None

            countries.append((country, iso3, state, population))

    for row in us_deaths[1:]:
        state = row[6]

        if state not in parsed_states:
            parsed_states.append(state)

            state_code = us_states.state_to_code(state)

            country = row[7]
            iso3 = row[2]
            population = row[11]

            if state_code is None:
                state_code = state
            countries.append((country, iso3, state_code, population))

    data_inserter = DataInserter()

    return data_inserter.insert_country(countries)


def parse_csv_files(path_global_conf, path_global_deaths, path_us_conf, path_us_deaths, path_global_recov,
                    starting_date=None):

    if starting_date is None:
        starting_date = '1/22/20'

    with open(path_global_conf) as csv_file:

        reader = csv.reader(csv_file, delimiter=',')
        global_conf = []
        for row in reader:
            global_conf.append(row)

    with open(path_global_deaths) as csv_file:

        reader = csv.reader(csv_file, delimiter=',')
        global_deaths = []
        for row in reader:
            global_deaths.append(row)

    with open(path_us_conf) as csv_file:

        reader = csv.reader(csv_file, delimiter=',')
        us_conf = []
        for row in reader:
            us_conf.append(row)

    with open(path_us_deaths) as csv_file:

        reader = csv.reader(csv_file, delimiter=',')
        us_deaths = []
        for row in reader:
            us_deaths.append(row) # one more column than in confirmed

    with open(path_global_recov) as csv_file:

        reader = csv.reader(csv_file, delimiter=',')
        global_recov = []
        for row in reader:
            global_recov.append(row)

    parsed_countries = []
    parsed_data = []

    countries_data = insert_get_countries(global_conf, us_deaths)

    for row in global_conf[1:]: # first row - column names

        country = row[1]

        if country not in parsed_countries:

            parsed_countries.append(country)

            country_id = [row[0] for row in countries_data if row[1] == country][0]

            date_row = global_conf[0]
            starting_date_index = date_row.index(starting_date)

            for i in range(starting_date_index, len(global_conf[0])):
                date = global_conf[0][i]

                tmp = [c for c in global_conf if c[1] == country]
                sum_conf = 0
                for t in tmp:
                    sum_conf += int(t[i])

                old_data = [v for v in parsed_data if country_id == v[0] and date in v[1]]

                if len(old_data) > 0:

                    input_data = (old_data[0][0], old_data[0][1], sum_conf, old_data[0][3], old_data[0][4])
                    index = parsed_data.index(old_data[0])
                    parsed_data[index] = input_data

                else:

                    input_data = (country_id, date, sum_conf, 0, 0)
                    parsed_data.append(input_data)

    parsed_countries = []

    for row in global_deaths[1:]: # first row - column names

        country = row[1]

        if country not in parsed_countries:

            parsed_countries.append(country)

            country_id = [row[0] for row in countries_data if row[1] == country][0]

            date_row = global_deaths[0]
            starting_date_index = date_row.index(starting_date)

            for i in range(starting_date_index, len(global_deaths[0])):

                date = global_deaths[0][i]

                tmp = [c for c in global_deaths if c[1] == country]
                sum_deaths = 0
                for t in tmp:
                    sum_deaths += int(t[i])

                old_data = [v for v in parsed_data if country_id == v[0] and date in v[1]]

                if len(old_data) > 0:

                    input_data = (old_data[0][0], old_data[0][1], old_data[0][2], sum_deaths, old_data[0][4])
                    index = parsed_data.index(old_data[0])
                    parsed_data[index] = input_data

                else:

                    input_data = (country_id, date, 0, sum_deaths, 0)
                    parsed_data.append(input_data)

    parsed_countries = []

    for row in global_recov[1:]:  # first row - column names

        country = row[1]

        if country not in parsed_countries:

            parsed_countries.append(country)

            country_id = [row[0] for row in countries_data if row[1] == country][0]

            date_row = global_recov[0]
            starting_date_index = date_row.index(starting_date)

            for i in range(starting_date_index, len(global_recov[0])):

                date = global_recov[0][i]

                tmp = [c for c in global_recov if c[1] == country]
                sum_recov = 0
                for t in tmp:
                    sum_recov += int(t[i])

                old_data = [v for v in parsed_data if country_id == v[0] and date in v[1]]

                if len(old_data) > 0:

                    input_data = (old_data[0][0], old_data[0][1], old_data[0][2], old_data[0][3], sum_recov)
                    index = parsed_data.index(old_data[0])
                    parsed_data[index] = input_data

                else:

                    input_data = (country_id, date, 0, 0, sum_recov)
                    parsed_data.append(input_data)

    start_point = len(parsed_data)

    parsed_states = []

    for row in us_conf[1:]:

        state = row[6]

        state_code = us_states.state_to_code(state)
        if state_code == 'und':
            state_code = state

        if state not in parsed_states:

            parsed_states.append(state)

            country_id = [row[0] for row in countries_data if row[2] == state_code][0]

            date_row = us_conf[0]
            starting_date_index = date_row.index(starting_date)

            for i in range(starting_date_index, len(us_conf[0])):

                date = us_conf[0][i]

                tmp = [c for c in us_conf if c[6] == state]
                sum_conf = 0
                for t in tmp:
                    sum_conf += int(t[i])

                old_data = [v for v in parsed_data[start_point:] if country_id == v[0] and date in v[1]]

                if len(old_data) > 0:

                    input_data = (old_data[0][0], old_data[0][1], sum_conf, old_data[0][3], old_data[0][4])
                    index = parsed_data.index(old_data[0])
                    parsed_data[index] = input_data

                else:

                    input_data = (country_id, date, sum_conf, 0, 0)
                    parsed_data.append(input_data)

    parsed_states = []

    for row in us_deaths[1:]:

        state = row[6]

        state_code = us_states.state_to_code(state)
        if state_code == 'und':
            state_code = state

        if state not in parsed_states:

            parsed_states.append(state)

            country_id = [row[0] for row in countries_data if row[2] == state_code][0]

            date_row = us_deaths[0]
            starting_date_index = date_row.index(starting_date)

            for i in range(starting_date_index, len(us_deaths[0])):

                date = us_deaths[0][i]

                tmp = [c for c in us_deaths if c[6] == state]
                sum_deaths = 0
                for t in tmp:
                    sum_deaths += int(t[i])

                old_data = [v for v in parsed_data[start_point:] if country_id == v[0] and date in v[1]]

                if len(old_data) > 0:

                    input_data = (old_data[0][0], old_data[0][1], old_data[0][2], sum_deaths, old_data[0][4])
                    index = parsed_data.index(old_data[0])
                    parsed_data[index] = input_data

                else:

                    input_data = (country_id, date, 0, sum_deaths, 0)
                    parsed_data.append(input_data)

    for d in parsed_data:
        date = d[1]
        parsed_date = parse_date(date)
        index = parsed_data.index(d)
        parsed_data[index] = (d[0], parsed_date, d[2], d[3], d[4])

    return parsed_data


if __name__ == "__main__":

     data_inserter = DataInserter()

     directory = '../data/Covid-19-data/csse_covid_19_data/csse_covid_19_time_series'
     path_global_conf_file = directory + '/' + 'time_series_covid19_confirmed_global.csv'
     path_global_deaths_file = directory + '/' + 'time_series_covid19_deaths_global.csv'
     path_us_conf_file = directory + '/' + 'time_series_covid19_confirmed_US.csv'
     path_us_deaths_file = directory + '/' + 'time_series_covid19_deaths_US.csv'
     path_global_recov_file = directory + '/' + 'time_series_covid19_recovered_global.csv'

     data = parse_csv_files(path_global_conf_file, path_global_deaths_file, path_us_conf_file, path_us_deaths_file,
                            path_global_recov_file) # no starting date = all, data='no-zero-month/day/last-two-numbers-year'
     # print(data)
     data_inserter.insert_covid19_data(data)
