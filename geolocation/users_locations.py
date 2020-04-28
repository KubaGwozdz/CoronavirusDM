import time

from geopy.geocoders import Nominatim, Photon
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded

from db_utils.data_inserter import DataInserter
from db_utils.data_selector import DataSelector

from data_processing.pipe import Pipe, Worker

import geolocation.us_states as states_mapper



class LocationsFinder(Worker):
    def __init__(self, id, users, pipe):
        super().__init__(id, users, pipe)
        self.geolocator = Nominatim(user_agent="CoronaVirus" + str(id), timeout=100000)

    def locate_user(self, location_str):
        while True:
            try:
                location = self.geolocator.geocode(location_str, exactly_one=True, addressdetails=True)
                country_code = location.raw['address']['country_code']
                state_code = None
                if country_code == 'us':
                    if 'state' in location.raw['address']:
                        state_code = states_mapper.state_to_code(location.raw['address']['state'])
                return country_code, state_code
            except KeyError:
                print(location.raw)
                return 'und', None
            except AttributeError:
                return 'und', None

    def run(self):
        s= self.id %5
        time.sleep(s)
        result = []
        for user in self.data:
            id = user['id']
            location_str = user['location']
            try:
                country_code, state_code = self.locate_user(location_str)
                result.append((country_code, state_code, id))
                time.sleep(5)
            except GeocoderQuotaExceeded or GeocoderTimedOut or Exception:
                if len(result) == 0:
                    print('T' + str(self.id) + ': quota exceeded, stopping..')
                    self.pipe.stop()
                    break
                else:
                    break
        self.pipe.put_done_data(result)
        print("Thread: " + str(self.id) + " finished")


class LocalizationPipe(Pipe):
    def __init__(self, **kwargs):
        if 'lock' in kwargs and 'db_m' in kwargs:
            l = kwargs['lock']
            d = kwargs['db_m']
            super().__init__(DataSelector.get_users_to_localize, DataInserter.update_users_country_code, LocationsFinder, lock=l, db_m=d)
        else:
            super().__init__(DataSelector.get_users_to_localize, DataInserter.update_users_country_code, LocationsFinder)
        self.batch_size = 10
        self.num_loc_threads = 1
        self.max_threads = 3  # 5 worked..


if __name__ == "__main__":
    pipe = LocalizationPipe()
    pipe.run()

