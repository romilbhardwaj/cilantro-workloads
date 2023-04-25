import random
import time
from typing import List, Dict

from cray_workloads.load_generators.trace_load_generator import TraceLoadGenerator
from cray_workloads.classes.task_container import TaskContainer
from cray_workloads.utils.data_structures import GeneratorLen


class TraceTaskLoadGenerator(TraceLoadGenerator):
    """
    Generates tasks with arbitrary python functions and args according to load from a trace file
    """

    def __init__(self,
                 base_task: callable = None,
                 base_task_args: List = None,
                 base_task_kwargs: Dict = None,
                 trace_path: str = None,
                 trace_scale_factor: float = 1,
                 load_normaliser: float = 1):
        """
        :param: base_task: The ray task to use as work
        :param: trace_scale_factor: Scale the trace query rates by this factor
        """
        self.base_task_args = base_task_args
        self.base_task_kwargs = base_task_kwargs
        super(TraceTaskLoadGenerator, self).__init__(base_task=base_task,
                                                     trace_path=trace_path,
                                                     trace_scale_factor=trace_scale_factor,
                                                     load_normaliser=load_normaliser)

    def _generate_work_list(self, size):
        enqueue_time = time.time()
        work = GeneratorLen(
            ([TaskContainer(self.base_task, timestamps={'enqueue_time': enqueue_time}), self.base_task_args, self.base_task_kwargs] for i in range(size)),
            size)
        return work
