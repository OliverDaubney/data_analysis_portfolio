"""
    Support functions for the data communication simulations.
    The functions handle communication between the separate
    components of the pipeline by reading and updating the
    contents of specified address files.

    An additional function also supports setting the
    correct time delay.

    Dependencies: msgpack
"""

import time
import msgpack


def write_msg(address, msg):
    try:
        message = msgpack.packb(msg)
        with open(f'{address}.msgpack', 'wb') as file:
            file.write(message)
    except:
        return 'Failed'

def read_msg(address):
    try:
        with open(f'{address}.msgpack', 'rb') as file:
            data = file.read()
        return msgpack.unpackb(data)
    except:
        return 'Failed'

def time_delay(start, time_step):
    end = time.time()
    function_run = end - start
    if function_run < time_step:
        time.sleep(time_step-function_run)
