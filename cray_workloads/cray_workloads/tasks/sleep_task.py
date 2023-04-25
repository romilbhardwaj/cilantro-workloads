import random
import time


def sleep_task(duration):
    '''
    Simple task that sleeps for the given duration before returning True
    :param duration: seconds to sleep for.
    :return: True when task completes.
    '''
    time.sleep(duration)
    # print(f"Total time slept: {end - start}")
    return True