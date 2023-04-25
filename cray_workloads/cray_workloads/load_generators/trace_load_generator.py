"""
    Trace Load generator.
    -- romilbhardwaj
    -- kirthevasank
"""

# pylint: disable=arguments-differ

import numpy as np
import pandas as pd
# Cray
from cray_workloads.load_generators.base_load_generator import BaseLoadGenerator, \
                                                               InsufficientStateError
from cray_workloads.utils.data_structures import GeneratorLen


class TraceLoadGenerator(BaseLoadGenerator):
    """
    Base class that reads a trace to generate load.
    """

    def __init__(self,
                 base_task: callable = None,
                 trace_path: str = None,
                 trace_scale_factor: float = 1,
                 load_normaliser: float = 1):
        """
        :param: base_task: The ray task to use as work
        :param: base_task_args: Default args to the base task
        :param: base_task_kwargs: Default kwargs for the base task
        :param: trace_scale_factor: Scale the trace query rates by this factor
        """
        self.base_task = base_task
        self.trace_path = trace_path
        self.trace_scale_factor = trace_scale_factor
        self.trace_df = self._read_trace(self.trace_path, self.trace_scale_factor)
        self.load_normaliser = load_normaliser
        super().__init__()

    def _read_trace(self, trace_path, scale_factor):
        """ Reads trace. """
        qps_trace_df = pd.read_csv(trace_path, index_col='timestamp')
        # Scale data ---------------
        qps_trace_df.data = (np.round(qps_trace_df.data * scale_factor)).astype('uint64')
        # Convert timestamps to seconds, start at zero and set/sort index.
        qps_trace_df.index = qps_trace_df.index / 1000    # Convert to seconds.
        qps_trace_df.index = qps_trace_df.index.astype('uint64')
        return self._convert_qps_trace_to_interarrivals(qps_trace_df)

    @ staticmethod
    def _convert_qps_trace_to_interarrivals(qps_trace_df):
        """ Converts QPS traces to inter-arrivals. """
        ret_data_list = []
        ret_timestamp_list = []
        all_indices = list(qps_trace_df.index)
        all_indices.sort()
        for idx, start_time in enumerate(all_indices[:-1]):
            next_time = all_indices[idx + 1]
            num_arrivals = int(qps_trace_df.loc[start_time, 'data'])
            time_stamps = list(np.linspace(start_time, next_time, num=num_arrivals+1))[:-1]
            ret_timestamp_list.extend(time_stamps)
            ret_data_list.extend([1] * num_arrivals)
        # Create a new data frame
        ret_df = pd.DataFrame({'data': ret_data_list}, index=ret_timestamp_list)
#         pd.set_option("display.max_rows", None, "display.max_columns", None)
#         print(ret_df.head(100))
#         print(ret_df.shape, qps_trace_df.sum())
#         import pdb; pdb.set_trace()
        return ret_df

    def _generate_work_list(self, size):
        """ Generate work. """
        raise NotImplementedError("Implement in a child class.")

    def generate_start_load(self):
        '''
        No work is submitted at the start of the trace
        '''
        return GeneratorLen(iter(()), 0)

    def get_load(self, start_time, end_time):
        '''
        Returns a list of work [[TaskContainer, TaskArgs, TaskKwargs]] whenever polled.
        '''
#         start_idx, end_idx = self.trace_df.index.searchsorted([start_time, end_time], side='left')
#         end_idx = max(0, end_idx-1)
        num_queries = sum(self.trace_df.loc[start_time:end_time].data)
        pd.set_option("display.max_rows", None, "display.max_columns", None)
#         print(f"Requesting {num_queries} at {start_time} to {end_time}")
#         if start_time > 2:
#             import pdb; pdb.set_trace()
        work = self._generate_work_list(num_queries)
        return work

    def get_load_estimate(self):
        """ Get load estimate. """
        # Load estimate is simply the number of queries generated in the round
        raise InsufficientStateError("Calculate load estimate from last round's work.")

