"""
    Fixed load generator.
    -- kirthevasank
"""

# pylint: disable=arguments-differ

import time
import numpy as np
from cray_workloads.load_generators.base_load_generator import BaseLoadGenerator, \
                                                               InsufficientStateError
from cray_workloads.utils.data_structures import GeneratorLen


class FixedLoadGenerator(BaseLoadGenerator):
    """
    Base class that reads a trace to generate load.
    """

    def __init__(self,
                 base_task: callable = None,
                 request_qps: float = None,
                 load_normaliser: float = 1):
        """
        :param: base_task: The ray task to use as work
        :param: base_task_args: Default args to the base task
        :param: base_task_kwargs: Default kwargs for the base task
        :param: trace_scale_factor: Scale the trace query rates by this factor
        """
        self.base_task = base_task
        self.request_qps = request_qps
        self.load_normaliser = load_normaliser
        self._last_request_time = None
        super().__init__()

    def _generate_work_list(self, size):
        """ Generate work. """
        raise NotImplementedError("Implement in a child class.")

    def _get_num_queries(self):
        """ Returns the number of queries. """
        curr_time = time.time()
        if self._last_request_time is None:
            self._last_request_time = curr_time
            return 1
        time_diff = curr_time - self._last_request_time
        num_queries = int(np.round(self.request_qps * time_diff))
        self._last_request_time = curr_time
        return num_queries

    def generate_start_load(self):
        '''
        No work is submitted at the start of the trace
        '''
        self._get_num_queries()
        return GeneratorLen(iter(()), 0)

    def get_load(self, *args, **kwargs):
        '''
        Returns a list of work [[TaskContainer, TaskArgs, TaskKwargs]] whenever polled.
        '''
        # pylint: disable=unused-argument
        num_queries = self._get_num_queries()
        work = self._generate_work_list(num_queries)
        return work

    def get_load_estimate(self):
        """ Get load estimate. """
        # Load estimate is simply the number of queries generated in the round
        raise InsufficientStateError("Calculate load estimate from last round's work.")

