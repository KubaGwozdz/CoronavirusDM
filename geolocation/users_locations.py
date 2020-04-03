import queue
import time

from geopy.geocoders import Nominatim, Photon
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded

import threading

from db_utils.data_inserter import DataInserter
from db_utils.data_selector import DataSelector
from db_utils.db_manager import DBManager

from data_processing.pipe import Pipe, Worker


class LocationsFinder(Worker):
    def __init__(self, id, users, pipe):
        super().__init__(id, users, pipe)
        self.geolocator = Nominatim(user_agent="CoronaVirus" + str(id), timeout=100000)

    def locate_user(self, location_str):
        while True:
            try:
                location = self.geolocator.geocode(location_str, exactly_one=True, addressdetails=True)
                country_code = location.raw['address']['country_code']
                return country_code
            except KeyError:
                print(location.raw)
                return 'und'
            except AttributeError:
                return 'und'

    def run(self):
        result = []
        for user in self.users:
            id = user['id']
            location_str = user['location']
            try:
                country_code = self.locate_user(location_str)
                time.sleep(5)
            except GeocoderQuotaExceeded or GeocoderTimedOut or Exception:
                if len(result) == 0:
                    print('T' + str(self.id) + ': quota exceeded, stopping..')
                    self.pipe.quote_exceeded = True
                    break
                else:
                    break
            result.append((country_code, id))
        self.pipe.put_done_data(result)
        print("Thread: " + str(self.id) + " finished")


class LocalizationPipe(Pipe):
    def __init__(self):
        super().__init__(DataSelector.get_users_to_localize, DataInserter.update_users_country_code, LocationsFinder)


if __name__ == "__main__":
    pipe = LocalizationPipe()
    pipe.run()
