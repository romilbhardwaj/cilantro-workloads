import json
import logging
import os
import threading
import time
from typing import Dict

import ray

from cray_workloads.classes.infinite_task_job import InfiniteTaskJobActor
from cray_workloads.classes.job_container import JobContainer

logger = logging.getLogger(__name__)


class CRayDriver(object):
    """
    This is the driver that is called from shell and runs the CilantroRay Workloads.
    """

    def __init__(self,
                 log_output_dir: str = None,
                 workload: Dict = None,
                 utility_report_frequency: float = 1,
                 task_watch_loop_sleep_time=0.1):
        """
        :param log_output_dir: Output dir for the log
        :param workload: A dict containing {'reward_fn', 'sigma_fn', 'load_generator'}
        :param utility_report_frequency: Time in seconds between reporting utilities. If this is too small, tasks may no complete on time. If too long, new resource allocations will take time to be applied.
        :param task_watch_loop_sleep_time: Time to sleep between checking for task completions.
        """
        if not ray.is_initialized():
            raise EnvironmentError("Ray is not initialized yet! Please intitialize ray before calling cray driver.")
        self.log_output_dir = log_output_dir
        self.workload = workload
        self.workload['watch_loop_sleeptime'] = task_watch_loop_sleep_time
        self.workload['load_generator_interval'] = 0.1
        self.utility_report_frequency = utility_report_frequency

        # These are the number of resources allocated to this cluster.
        # This is externally controlled by k8s, we just need to poll it's state.
        self.resource_count = self.get_resource_count()
        logger.info(f"Got an initial resource count: {self.resource_count}")

        # Start thread to update resource count in background.
        resource_update_thread = threading.Thread(target=self.update_local_resource_count_thread, args=())
        resource_update_thread.start()

    def update_local_resource_count_thread(self, sleep_time=1):
        logger.info("Running resource count updater thread.")
        while True:
            new_resource_count = self.get_resource_count()
            if new_resource_count != self.resource_count:
                logger.info(f"Got new resource count! Setting resources to {new_resource_count}")
                self.resource_count = new_resource_count
            time.sleep(sleep_time)

    def get_resource_count(self) -> int:
        """
        Returns the number of CPU resources in Ray
        :return: CPU count in ray
        """
        return int(ray.cluster_resources().get("CPU", 0))  # Return 0 if no CPUs foun

    def run_loop(self):
        container = JobContainer(job_actor=InfiniteTaskJobActor,
                                 job_actor_kwargs=self.workload)
        job_handle = container.launch_job()

        while True:
            # Start the job
            round_resource_allocation = self.resource_count
            logger.info(f"Running job with {round_resource_allocation} resources.")
            start_time = time.time()
            try:
                ray.get(job_handle.round_start_callback.remote(resource_allocation=round_resource_allocation))

                logger.info(f"Sleeping for {self.utility_report_frequency}")
                time.sleep(self.utility_report_frequency)

                end_time = time.time()
                result = ray.get(job_handle.round_completion_callback.remote())
            except Exception as e:
                logger.error(f"Something went wrong but DONT PANIC. I'll not report any utility this window and "
                             f"restart the actor from scratch. Error: {e}")
                container = JobContainer(job_actor=InfiniteTaskJobActor,
                                         job_actor_kwargs=self.workload)
                job_handle = container.launch_job()
                continue
            # Omitting reward and sigma because they are computed by cilantro client from the
            # task metrics.
            current_resource_count = self.resource_count    # Updated in the background thread
            utility_message = {# 'reward': result['reward'],
                               # 'sigma': result['sigma'],
                               'tasks_completed_count': len(result['task_metrics']),
                               'task_metrics': result['task_metrics'],
                               'incomplete_task_metrics': result['incomplete_task_metrics'],
                               'round_start_time': start_time,
                               'round_end_time': end_time,
                               'alloc': round_resource_allocation,
                               'alloc_round_end': current_resource_count,
                               'load_round_total': result['load_round_total'],
                               'load_round_total_normalized': result['load_round_total_to_report'],
                               'load_round_end': result['load_round_end']}

            logger.info(f"Round complete with allocation {round_resource_allocation}. Completed {utility_message['tasks_completed_count']} tasks.")# . Got utility_message: {utility_message}")
            # Write to disk
            self.write_utility_to_disk(utility_message)

    def write_utility_to_disk(self,
                              utility_message: Dict):
        """
        Writes the utility message to a log file on disk.
        The utility message is written as a json.
        :return:
        """
        timestr = time.strftime("%Y%m%d-%H%M%S-%f")[:-3]
        log_filename = "output_%s.log" % timestr  # This will be the final name of the log
        log_filepath = os.path.join(self.log_output_dir, log_filename)

        with open(log_filepath, 'w') as f:
            json.dump(utility_message, f, sort_keys=True, indent=4)
