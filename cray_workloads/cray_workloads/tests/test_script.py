import logging
import random
import threading
import time

logger = logging.getLogger(__name__)


class MyThreadedClass(object):
    def __init__(self):
        # Runs a thread in the background to update resource count.
        self.resource_count = self.get_resource_count()

        resource_update_thread = threading.Thread(target=self.update_local_resource_count_thread, args=(), daemon=False)
        resource_update_thread.start()

    def update_local_resource_count_thread(self, sleep_time=1):
        print("Running resource count updater thread.")
        while True:
            new_resource_count = self.get_resource_count()
            if new_resource_count != self.resource_count:
                print(f"Got new resource count! Setting resources to {new_resource_count}")
                self.resource_count = new_resource_count
            time.sleep(sleep_time)

    def get_resource_count(self) -> int:
        return random.randint(1,10)

    def run_loop(self):
        while 1:
            print(f"In main loop. Resource count is {self.resource_count}")
            time.sleep(1)

if __name__ == '__main__':
    crd = MyThreadedClass()
    print("Hello.")
    crd.run_loop()