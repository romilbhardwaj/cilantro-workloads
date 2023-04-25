"""
    DB query serving workload.
    -- kirthevasank
"""

import logging
import time
from typing import List
# Local
from cray_workloads.load_generators.trace_load_generator import TraceLoadGenerator
from cray_workloads.classes.task_container import TaskContainer
from cray_workloads.utils.data_structures import GeneratorLen


logger = logging.getLogger(__name__)


class TraceDBQueryLoadGenerator(TraceLoadGenerator):
    """
    Generates tasks for db query serving according to load from a trace file.
    """

    def __init__(self,
                 base_task: callable = None,
                 db_path: str = None,
                 queries: List[str] = None,
                 trace_path: str = None,
                 sleep_time: float = None,
                 trace_scale_factor: float = 1,
                 load_normaliser: float = 1):
        """
        :param: base_task: The ray task to use as work
        :param: base_task_args: Default args to the base task
        :param: base_task_kwargs: Default kwargs for the base task
        :param: trace_scale_factor: Scale the trace query rates by this factor
        """
        self.db_path = db_path
        self.queries = queries
        self.sleep_time = sleep_time
        super().__init__(base_task=base_task,
                         trace_path=trace_path,
                         trace_scale_factor=trace_scale_factor,
                         load_normaliser=load_normaliser)

    def _generate_work_list(self, size):
        """ Generate work list. """
        kwargs = {}
        if size <= len(self.queries):
            load_queries = self.queries[0:size]
        else:
            q, r = divmod(size, len(self.queries))
            load_queries = q * self.queries + self.queries[:r]  # Repeat the queries till
                                                                # round_load_size.
        enqueue_time = time.time()
        work = GeneratorLen(
            ([TaskContainer(self.base_task, timestamps={'enqueue_time': enqueue_time}),
             [self.db_path, q, self.sleep_time], kwargs] for q in load_queries), len(load_queries))
        return work

