import time
import ray

from cray_workloads import CONSTANTS


class TaskContainer(object):
    def __init__(self,
                 remote_task: callable,
                 default_args: list = None,
                 default_kwargs: dict = None,
                 timestamps = None,
                 name = None):
        self.resources = {}
        self.remote_task = remote_task
        self.default_args = default_args or []
        self.default_kwargs = default_kwargs or {}
        self.initialize_time = time.time()
        self.task_id = None
        self.timestamps = timestamps    # Dictionary storing timestamps of task events. Can be pre-initialized
        self.completion_time = None
        self.name = name

    def remote(self,
               task_args: list = None,
               task_kwargs: dict = None,
               resources: dict = None):
        '''
        Launches the remote task with a Ray like API. Uses default args if task_args and/or task_kwargs are not specified
        :return:
        '''
        self.task_args = task_args or self.default_args
        self.task_kwargs = task_kwargs or self.default_kwargs
        if resources is None:
            resources = {}
        self.resources = resources
        n_cpus = self.resources.get(CONSTANTS.RESOURCE_TYPE_CPU, 0)
        n_gpus = self.resources.get(CONSTANTS.RESOURCE_TYPE_GPU, 0)
        custom_resources = {k: v for k, v in self.resources.items() if k not in ["cpu", "gpu"]}
        now = time.time()
        self.timestamps['submit_time'] = now
        # print(f"[{self.name}] Started at {now}. Timestamps is {self.timestamps}")
        task_id = self.remote_task.options(num_cpus=n_cpus,
                                           num_gpus=n_gpus,
                                           resources=custom_resources).remote(*self.task_args, **self.task_kwargs)
        self.task_id = task_id
        return task_id

    def get(self,
            timeout: float = None):
        if not self.task_id:
            raise Exception("Task is not yet running.")
        self.result = ray.get(self.task_id)
        now = time.time()
        self.timestamps['completion_time'] = now
        # print(f"[{self.name}]Ended at {now}. job_runtime: {self.timestamps['completion_time'] - self.timestamps['submit_time']}. Timestamps is {self.timestamps}")
        return self.result

    def get_metrics(self):
        if hasattr(self, 'result'):
            return {'job_runtime': self.timestamps['completion_time'] - self.timestamps['submit_time'],
                    'enqueue_time': self.timestamps['enqueue_time'],
                    'latency': self.timestamps['completion_time'] - self.timestamps['enqueue_time']}
        else:
            raise Exception('Task not done yet.')

    def wait(self):
        ray.wait(self.task_id)

    def cancel(self):
        if not hasattr(self, 'result'):
            ray.cancel(self.task_id, force=True)