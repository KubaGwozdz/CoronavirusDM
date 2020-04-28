import queue
import threading

from db_utils.data_inserter import DataInserter
from db_utils.data_selector import DataSelector
from db_utils.db_manager import DBManager


class Worker(threading.Thread):
    def __init__(self, id, data, pipe):
        threading.Thread.__init__(self)
        self.id = id
        self.data = data
        self.pipe = pipe

    def run(self):
        """result = []

        -------------------------

        do sth and add to result

        if want to stop processing:
            self.pipe.quote_exceeded = True

        -------------------------

        self.pipe.put_done_data(result)
        print("Thread: " + str(self.id) + " finished")"""
        pass


class Pipe(threading.Thread):
    def __init__(self, select_fun, insert_fun, worker_class, lock=threading.Lock(), db_m=DBManager()):
        threading.Thread.__init__(self)
        self.new_data = queue.Queue()
        self.done_data = queue.Queue()
        self.db_m = db_m
        self._db_access = lock
        self.db_insert = DataInserter(self.db_m)
        self.db_select = DataSelector(self.db_m)
        self.num_loc_threads = 2
        self.max_threads = 5  # 5 worked..
        self.batch_size = 10
        self.quota_exceeded = False
        self.select_fun = select_fun.__name__
        self.insert_fun = insert_fun.__name__
        self.worker_class = worker_class

    def get_new_data(self):
        batch = []
        it = 0
        while it < self.batch_size and not self.new_data.empty():
            batch.append(self.new_data.get())
            it += 1
        return batch

    def get_done_data(self):
        batch = []
        it = 0
        while it < self.batch_size and not self.done_data.empty():
            batch.append(self.done_data.get())
            it += 1
        return batch

    def put_new_data(self, data):
        for d in data:
            self.new_data.put(d)

    def put_done_data(self, data):
        for d in data:
            self.done_data.put(d)

    def stop(self):
        self.quota_exceeded = True

    def run(self):
        worker_threads = []
        epoch_count = 0
        select_scale = 5

        with self._db_access:
            new_data = self.db_select.__getattribute__(self.select_fun)(self.batch_size * select_scale)
        print('Data selected')
        self.put_new_data(new_data)
        thread_id = 0

        while not self.new_data.empty() and not self.quota_exceeded:
            print("----- Beginning " + str(epoch_count) + " epoch -----")
            worker_threads = [t for t in worker_threads if t.is_alive()]
            print("Active threads: " + str(len(worker_threads)))
            print("Data to process: " + str(self.new_data.qsize()))
            for i in range(self.num_loc_threads):
                thread = self.worker_class(thread_id, self.get_new_data(), self)
                thread.start()
                worker_threads.append(thread)
                thread_id += 1

            print("Processing started")

            if len(worker_threads) > self.max_threads:
                print('Too many to process, waiting..')
                for t in worker_threads[:-self.max_threads // 2]:
                    t.join()
                print('Resuming...')

            print("Inserting started")
            print("Data to insert: " + str(self.done_data.qsize()))
            if not self.done_data.empty():
                with self._db_access:
                    while not self.done_data.empty():
                        self.db_insert.__getattribute__(self.insert_fun)(self.get_done_data())

            with self._db_access:
                new_data = self.db_select.__getattribute__(self.select_fun)(self.batch_size * select_scale)
            print('New data selected')
            self.put_new_data(new_data)
            epoch_count += 1

        print('--- No more data ---')
        print('Joining threads')
        for t in worker_threads:
            t.join()
        print("All thread finished, inserting last..")

        if not self.done_data.empty():
            with self._db_access:
                while not self.done_data.empty():
                    self.db_insert.__getattribute__(self.insert_fun)(self.get_done_data())
        print('--- Everything added ---')

    def run_one(self):
        epoch_count = 0

        with self._db_access:
            new_data = self.db_select.__getattribute__(self.select_fun)(self.batch_size)
        print('Data selected')
        self.put_new_data(new_data)

        while not self.new_data.empty() and not self.quota_exceeded:
            print("----- Beginning " + str(epoch_count) + " epoch -----")
            print(self.quota_exceeded)

            worker = self.worker_class(1, self.get_new_data(), self)
            worker.start()
            print("Processing started")
            worker.join()
            print("Data to insert: " + str(self.done_data.qsize()))
            with self._db_access:
                self.db_insert.__getattribute__(self.insert_fun)(self.get_done_data())
                new_data = self.db_select.__getattribute__(self.select_fun)(self.batch_size)
            print('New data selected')
            self.put_new_data(new_data)
            epoch_count += 1

        if not self.done_data.empty():
            with self._db_access:
                while not self.done_data.empty():
                    self.db_insert.__getattribute__(self.insert_fun)(self.get_done_data())
        print('--- Everything added ---')
