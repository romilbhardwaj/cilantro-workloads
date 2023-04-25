import copy
import gc
import random
import string
import time
from itertools import chain
from typing import List, Dict, Tuple

import ray
from cray_workloads.load_generators.base_load_generator import BaseLoadGenerator, InsufficientStateError

from cray_workloads import CONSTANTS
from cray_workloads.classes.basejob import BaseJob

from cray_workloads.utils.data_structures import GeneratorLen


class InfiniteTaskJob(BaseJob):

    def __init__(self,
                 reward_fn: callable,
                 sigma_fn: callable,
                 name: str = None,
                 base_task_unitdemand: float = 1,
                 DEFAULT_RES_TYPE=CONSTANTS.RESOURCE_TYPE_CPU,
                 watch_loop_sleeptime: float = 1,
                 load_generator_interval: float = 1,
                 load_generator: BaseLoadGenerator = None,
                 delete_old_work: bool = True):
        """
        Runs the same task infinite times as resources become available for a given resource allocation.
        :param reward_fn: Reward function that translates a list of task_metrics dictionaries to a reward value
        :param name: Name for the job string
        :param base_task_unitdemand: Resources required by each task. Eg. 1 cpu.
        :param watch_loop_sleeptime: Time to sleep between waiting for tasks to complete.
        :param load_generator_interval: Interval in seconds at which the load generator is polled to generate load
        """
        self.name = name if name else ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
        self.base_task_unitdemand = base_task_unitdemand
        self.DEFAULT_RES_TYPE = DEFAULT_RES_TYPE
        self.watch_loop_sleeptime = watch_loop_sleeptime
        self.load_generator_interval = load_generator_interval
        self.running_containers = {}
        self.task_metrics = {}
        self.work_queue = GeneratorLen(iter(()), 0)
        self.run_watch_loop = False
        self.load_generator = load_generator
        self.delete_old_work = delete_old_work
        self.work_added_this_round = 0
        self.last_round_load = 0
        super(InfiniteTaskJob, self).__init__(reward_fn, sigma_fn)

    def add_work(self,
                 new_work,  # Iterable
                 delete_old_work = True):
        '''
        Adds a list of new work to the work queue.
        '''
        if delete_old_work:
            self.work_queue = GeneratorLen(iter(()), 0)
        self.work_added_this_round += len(new_work)
        self.work_queue = GeneratorLen(chain(self.work_queue, new_work), len(self.work_queue) + len(new_work))
        # print(f'[Job {self.name}] Work len is now ({len(self.work_queue)})')

    def empty_work(self):
        '''
        Empties the work queue
        '''
        self.work_queue = iter(())

    def get_load_estimate(self) -> int:
        try:
            load_estimate = self.load_generator.get_load_estimate()
        except InsufficientStateError:
            load_estimate = self.last_round_load
        load_estimate = load_estimate / self.load_generator.load_normaliser
        return load_estimate

    def get_current_load(self) -> int:
        # NOTE - This wont be accurate at the start of the round!
        return len(self.work_queue)

    def launch_tasks(self,
                     available_resources: float):
        '''
        Saturates allocated resources by launching tasks till capacity is filled
        :param available_resources: Number of resources available
        :return:
        '''
        # print("Launching tasks with {} resources. Current work queue size {}.".format(available_resources, len(self.work_queue)))
        while (available_resources - self.base_task_unitdemand >= 0) and (len(self.work_queue) > 0):
            container, args, kwargs = next(self.work_queue)
            if container is not None:
                res_demand = {self.DEFAULT_RES_TYPE: self.base_task_unitdemand}
                task_id = container.remote(args, kwargs, resources=res_demand)
                self.running_containers[task_id] = container  # Task -> Container map
                available_resources -= res_demand[self.DEFAULT_RES_TYPE]
        # print("[Job {}] Running {} containers.".format(self.name, len(self.running_containers)))
        return available_resources

    def round_start_callback(self, resource_allocation):
        '''
        Called at the start of a task
        :param resource_allocation:
        :return:
        '''
        # print(f"[Job {self.name}] In round start")
        # Get load estimate from load generator for logging
        self.work_added_this_round = 0
        self.load_generator.round_start_callback()
        self.round_start_load_estimate = self.get_load_estimate()

        # Get the actual round start load from load generator and add work
        start_work_generator = self.load_generator.generate_start_load()
        if self.delete_old_work:
            self.work_round_prestart = 0
        else:
            self.work_round_prestart = self.get_current_load()
        self.add_work(start_work_generator, delete_old_work=self.delete_old_work)

        # Set resource allocations
        self.resource_allocation = resource_allocation
        self.resource_utilization = 0

        assert len(self.running_containers) == 0
        self.task_metrics = {}
        self.available_resources = self.resource_allocation

        # Launch task while resources are available and get leftovers
        self.available_resources = self.launch_tasks(self.available_resources)

        # Start fetching jobs from now on
        # self.last_work_generate_time = time.time()

        # Run loop
        self.run_watch_loop = True
        # print(f"[Job {self.name}] Done round start")

    def fetch_and_add_work(self, start_time, end_time):
        '''
        Fetches work (if any) from the load generator between start and end time and adds it to the work_queue.
        Periodically called in watch_loop.
        '''
        new_work = self.load_generator.get_load(start_time=start_time, end_time=end_time)
        self.add_work(new_work, delete_old_work=False)
        return len(new_work)

    # @profile
    def watch_loop(self):
        '''
        loop which watches status of current tasks and launches new ones when they complete.
        :return:
        '''
        print(f"[Job {self.name}] Starting watch loop.")
        job_start_time = time.time()
        self.last_work_generate_time = 0
        while True:
            while self.run_watch_loop:
                self.watch_loop_running = True
                # print("Running containers {}".format(self.running_containers.keys()))
                if len(self.running_containers) == 0:
                    done_tasks = []
                    time.sleep(self.watch_loop_sleeptime)   # OOM workaround till https://github.com/ray-project/ray/issues/15644 gets resolved
                else:
                    done_tasks, _ = ray.wait(list(self.running_containers.keys()), num_returns=len(self.running_containers),
                                             timeout=self.watch_loop_sleeptime)
                now = time.time()
                if now - self.last_work_generate_time > self.load_generator_interval:
                    # If it's been a while since you fetched work, do it now.
                    # TODO: Implement time latching here - should request at the start of a second to ensure work is returned for non continous traces.
                    self.fetch_and_add_work(start_time=self.last_work_generate_time-job_start_time, end_time=now-job_start_time)
                    self.last_work_generate_time = now
                total_relinquished_resources = {}
                for task in done_tasks:
                    # TODO(perf): Speedup by calling ray.get once on all completed tasks and updating task containers
                    done_container = self.running_containers[task]
                    # print("Container {} finished. Resources: {}".format(done_container, done_container.resources))

                    # Get results
                    try:
                        done_container.get()
                    except (ray.exceptions.TaskCancelledError, ray.exceptions.WorkerCrashedError):#ray.exceptions.RayWorkerError
                        # Task was killed - ignore this task, get resources back and continue
                        pass
                    else:
                        # Task was successful
                        this_task_metrics = done_container.get_metrics()
                        self.task_metrics[task] = this_task_metrics

                    # Add resources to total_relinquished resources
                    container_resources = done_container.resources
                    total_relinquished_resources = {
                        key: total_relinquished_resources.get(key, 0) + container_resources.get(key, 0) for key in
                        set(total_relinquished_resources) | set(container_resources)}

                # Remove container from running containers
                for task in done_tasks:
                    del self.running_containers[task]
                # Homogeneous resources: Get the DEFAULT_RES_TYPE and use that as the resource
                # print("Total {}".format(total_relinquished_resources))
                self.available_resources += total_relinquished_resources.get(self.DEFAULT_RES_TYPE, 0)
                if self.available_resources > 0:
                    self.available_resources = self.launch_tasks(self.available_resources)
            gc.collect()
            self.watch_loop_running = False

    def round_completion_callback(self):
        '''
        Called when a scheduling round is complete. Must kill all running tasks and return the utility value.
        :return: reward, sigma (estimate of confidence)
        '''
        self.run_watch_loop = False
        while self.watch_loop_running:
            pass    # Busy wait for watch loop to complete
        for task_id, container in self.running_containers.items():
            container.cancel()  # TODO: Change to soft-kill with timeout instead of hard termination
        incomplete_task_metrics = []
        for c in list(self.running_containers.keys()):
            incomplete_task_metrics.append({'enqueue_time': self.running_containers[c].timestamps['enqueue_time'],
                                            'submit_time': self.running_containers[c].timestamps['submit_time']})
            del self.running_containers[c]
        self.last_round_load = self.work_added_this_round

        # Additional round state required for reward computation and reporting
        load_round_total_to_report = (self.work_round_prestart + self.work_added_this_round) / \
                                      self.load_generator.load_normaliser
        round_state = {'load_round_prestart': self.work_round_prestart,
                       'load_round_estimate': self.round_start_load_estimate,
                       'load_round_total': self.work_round_prestart + self.work_added_this_round, # Total load for this round - start + generated
                       'load_round_total_to_report': load_round_total_to_report,
                       'load_round_end': self.get_current_load()}

        reward = self.reward_fn(self.task_metrics.values(), round_state)
        sigma = self.sigma_fn(self.task_metrics.values(), round_state)
        # print(f"[Job {self.name}] Completed {len(self.task_metrics)} tasks. Reward: {reward}. Latencies: {[x['latency'] for x in self.task_metrics.values()]}")
        self.running_containers = {}
        return {'reward': reward,
                'sigma': sigma,
                'task_metrics': list(self.task_metrics.values()),
                'incomplete_task_metrics': incomplete_task_metrics,
                **round_state}

InfiniteTaskJobActor = ray.remote(InfiniteTaskJob)
