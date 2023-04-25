"""
    Tests ray.
    -- kirthevasank
"""

import logging
import random
import threading
import time

import ray

from cray_workloads.tasks.sleep_task import sleep_task

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    ray.init()
    task = ray.remote(sleep_task)
    t = []
    times = []
    for i in range(0,100):
        times.append(time.time())
        t.append(task.remote(1))
    ray.get(t)
    completion = time.time()
