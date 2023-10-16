"""
    Support classes for the data communication simulations.
    The classes create data source and state manager
    objects.

    Dependencies: support_func module.
    Dependencies of support packages: msgpack
"""

import time
import random
import support_func as sf
from csv import writer
from collections import deque


class Data_Source:
    def __init__(self, address, time_step, cycle_limit, cache_size):
        self.address = address
        self.time_step = time_step
        self.cycle_limit = cycle_limit
        self.cache_size = cache_size
        self.cache = deque([])

    def start(self):
        for i in range(0, self.cycle_limit):
            start = time.time()
            self.add_data_to_cache()
            if len(self.cache) >= self.cache_size*0.5:
                self.manage_cache()
            sf.time_delay(start, self.time_step)

    def add_data_to_cache(self):
        """
            Function to add data to the cache
            while maintaining the cache at or
            below the cache size.
        """
        if len(self.cache) >= self.cache_size:
            self.cache.popleft()
        self.cache.append(random.randint(0, 10))

    def manage_cache(self):
        """
            Function to read comms from state
            manager and either export cache
            or request to export cache.
        """
        flags = sf.read_msg(address=f'comms/{self.address}')
        if flags != 'Failed' and flags['a'] == 1:
            print(f"{self.address} EXPORTED {len(self.cache)}")
            self.export_cache()
            sf.write_msg(address=f'comms/{self.address}', msg={'a':1, 'r':0})
        elif flags != 'Failed' and flags['r'] == 0:
            sf.write_msg(address=f'comms/{self.address}', msg={'a':0, 'r':1})

    def export_cache(self):
        """
            Function to read and update the
            database with the current cache,
            then empty the cache.
        """
        data = sf.read_msg(address='data/database')
        data.extend(self.cache)
        sf.write_msg(address='data/database', msg=data)
        self.cache = deque([])


class State_Manager:
    def __init__(self, device_addresses, time_step, cycle_limit):
        self.addresses = device_addresses
        self.time_step = time_step
        self.cycle_limit = cycle_limit
        self.queue = deque([])
        self.analytics = []
        self.archive_size = 0
        self.busy = False
        self.build_pipeline()        

    def start(self):
        self.start_time = time.time()
        for i in range(0, self.cycle_limit):
            start = time.time()
            for address in self.addresses:
                flags = sf.read_msg(address=f'comms/{address}')
                self.check_if_still_busy(address, flags)
                self.update_queue(address, flags)
                self.update_analytics(flags)
            self.set_next_access()
            self.export_analytics()
            sf.time_delay(start, self.time_step)

    def check_if_still_busy(self, address, flags):
        """
            A function to check if the currently
            accessing data source has finished,
            then remove it from queue and update
            the archive size value.
        """
        if flags != 'Failed' and flags['r'] == 0 and flags['a'] == 1:
            sf.write_msg(address=f'comms/{address}', msg={'a':0, 'r':0})
            self.queue.popleft()
            self.update_archive()
            self.busy = False

    def update_archive(self):
        archive = sf.read_msg(address='data/database')
        self.archive_size = len(archive)

    def update_queue(self, address, flags):
        if flags != 'Failed' and flags['r'] == 1 and address not in self.queue:
            self.queue.append(address)

    def update_analytics(self, flags):
        if flags != 'Failed':
            self.analytics.append(flags['a'])
        else:
            self.analytics.append(0)

    def set_next_access(self):
        """
            Function to communicate with the next
            data source in the queue if the state
            manager is not busy. This grants the
            next object access to the database.
        """
        if not self.busy and self.queue:
            address = self.queue[0]
            sf.write_msg(address=f'comms/{address}', msg={'a':1, 'r':1})
            self.busy = True

    def export_analytics(self):
        """
            Function to export a set of analytics
            to a local CSV file.
        """
        data = [time.time()-self.start_time] + [i for i in self.analytics] + [self.archive_size]
        with open('data/analytics.csv', 'a', newline='') as file:
            writer_object = writer(file)
            writer_object.writerow(data)
        self.analytics = []

    def build_pipeline(self):
        # Initialise communication to devices.
        for address in self.addresses:
            sf.write_msg(address=f'comms/{address}', msg={'a':0, 'r':0})
        # Initialise the main data store.
        sf.write_msg(address='data/database', msg=[])
        # Initialise analytics logging.
        headers = ['time'] + [f'd{i}_comms' for i in self.addresses] + ['archive_size']
        with open('data/analytics.csv', 'w', newline='') as file:
            writer_object = writer(file)
            writer_object.writerow(headers)
