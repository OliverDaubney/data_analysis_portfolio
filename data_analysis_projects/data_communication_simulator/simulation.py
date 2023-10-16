"""
    Data Communication Simulation
    -----------------------------
    A data communication simulation which instantiates a simple
    pipeline structure and logs the analytics of communication
    to a CSV file in the data directory. The state manager
    makes use of communication files in the comms directory
    to control access and offloading of a stream of data from
    simulated sources. Multiprocessing is used to make sure
    that each component of the data pipeline is running
    separately.

    This simulation outputs a series of messages to show that
    the data sources are exporting data. The analytics of the
    pipeline are automatically logged to a CSV file in the 
    data directory.

    Dependencies: support_class module.
    Dependencies of support packages: msgpack
"""

import sys
from multiprocessing import Process
from support_class import Data_Source, State_Manager


def main():
    # Constructing the components of the pipeline.
    state = State_Manager(device_addresses=[1, 2, 3], time_step=0.02, cycle_limit=2200)
    data_1 = Data_Source(address=1, time_step=0.08, cycle_limit=600, cache_size=100)
    data_2 = Data_Source(address=2, time_step=0.07, cycle_limit=600, cache_size=100)
    data_3 = Data_Source(address=3, time_step=0.10, cycle_limit=600, cache_size=100)

    # Setting up multiprocessing.
    p1 = Process(target=state.start, daemon=True)
    p2 = Process(target=data_1.start, daemon=True)
    p3 = Process(target=data_2.start, daemon=True)
    p4 = Process(target=data_3.start, daemon=True)
    processes = [p1, p2, p3, p4]
    
    # Run the processes on separate cores.
    try:
        for p in processes:
            p.start()
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
        print("Program interrupted.")
    finally:
        for p in processes:
            p.join()


if __name__ == '__main__':
    main()
    sys.exit()