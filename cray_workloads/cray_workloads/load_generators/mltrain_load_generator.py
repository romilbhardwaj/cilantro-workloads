"""
    ML model training workload.
    -- kirthevasank
    -- romilbhardwaj
"""

import logging
import pickle
import time
# Local
from cray_workloads.load_generators.fixed_load_generator import FixedLoadGenerator
from cray_workloads.classes.task_container import TaskContainer
from cray_workloads.utils.data_structures import GeneratorLen

logger = logging.getLogger(__name__)

DFLT_MAX_TR_DATA = 2000
# DFLT_MAX_TR_DATA = 10


class MLTrainLoadGenerator(FixedLoadGenerator):
    """
    Prediction serving load generator.
    """

    def __init__(self,
                 base_task,
                 data_path: str,
                 batch_size: int,
                 num_iters: int,
                 sleep_time: float,
                 request_qps: float,
                 max_num_tr_data: int = DFLT_MAX_TR_DATA,
                 load_normaliser: float = 1):
        """
        :param: base_task: The ray task to use as work.
        :param: trace_scale_factor: Scale the trace query rates by this factor.
        """
        super().__init__(base_task=base_task,
                         request_qps=request_qps,
                         load_normaliser=load_normaliser)
        with open(data_path, 'rb') as data_file:
            data = pickle.load(data_file, encoding='latin1')
            data_file.close()
        self.batch_size = batch_size
        self.num_iters = num_iters
        X_tr = data['train']['x'][:max_num_tr_data, :]
        Y_tr = data['train']['y'][:max_num_tr_data]
        self.train_data_lists = [(X_tr[idx:idx+batch_size, :], Y_tr[idx:idx+batch_size])
                                 for idx in range(0, len(X_tr) - batch_size, batch_size)]
        self.train_data_lists_curr = self.train_data_lists[:]
        self.train_data_list_index = 0
        self.sleep_time = sleep_time

    def _generate_work_list(self, size):
        """ Generates a list of work. """
        kwargs = {}
        # Prepare the data ----------------------------------------------------------------------
        if size > len(self.train_data_lists):
            q, r = divmod(size, len(self.train_data_lists))
            curr_train_data = q * self.train_data_lists + self.train_data_lists[:r]
        else:
            if size > len(self.train_data_lists) - self.train_data_list_index:
                self.train_data_list_index = 0
            curr_train_data = \
                self.train_data_lists[self.train_data_list_index: self.train_data_list_index + size]
            self.train_data_list_index += size
        # Generate the tasks --------------------------------------------------------------------
        enqueue_time = time.time()
        work = GeneratorLen(
            ([TaskContainer(self.base_task, timestamps={'enqueue_time': enqueue_time}),
              [curr_train_data[i][0], curr_train_data[i][1], self.num_iters, self.sleep_time],
               kwargs]
             for i in range(size)), size)
#         print('Generated work of size %d, %d: %s, %s'%(
#             size, len(curr_train_data), work, [len(elem[0]) for elem in curr_train_data]))
#         import pdb; pdb.set_trace()
        return work

