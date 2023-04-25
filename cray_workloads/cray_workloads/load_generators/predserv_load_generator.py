"""
    Prediction serving workload.
    -- kirthevasank
    -- romilbhardwaj
"""

import logging
import pickle
import time
from sklearn.ensemble import RandomForestRegressor
# Local
from cray_workloads.load_generators.trace_load_generator import TraceLoadGenerator
from cray_workloads.classes.task_container import TaskContainer
from cray_workloads.utils.data_structures import GeneratorLen

logger = logging.getLogger(__name__)

DFLT_MAX_TE_DATA = 3200
DFLT_MAX_TR_DATA = 2000
# DFLT_MAX_TR_DATA = 10


class PredServLoadGenerator(TraceLoadGenerator):
    """
    Prediction serving load generator.
    """

    def __init__(self,
                 base_task,
                 data_path: str,
                 serve_chunk_size: int,
                 sleep_time: float,
                 max_num_tr_data: int = DFLT_MAX_TR_DATA,
                 max_num_te_data: int = DFLT_MAX_TE_DATA,
                 model_path: str = '',
                 trace_path: str = None,
                 trace_scale_factor: float = 1,
                 load_normaliser: float = 1):
        """
        :param: base_task: The ray task to use as work
        :param: trace_scale_factor: Scale the trace query rates by this factor
        """
        super().__init__(base_task=base_task,
                         trace_path=trace_path,
                         trace_scale_factor=trace_scale_factor,
                         load_normaliser=load_normaliser)
        with open(data_path, 'rb') as data_file:
            data = pickle.load(data_file, encoding='latin1')
            data_file.close()
        X_te = data['vali']['x'][:max_num_te_data, :]
        if model_path == 'train':
            X_tr = data['train']['x'][:max_num_tr_data, :]
            Y_tr = data['train']['y'][:max_num_tr_data]
            self.model = RandomForestRegressor()
            self.model.fit(X_tr, Y_tr)
            logger.info('Trained model %s', self.model)
#             # Save the image -------------------------------------------------------------
#             with open('../../train_data/news_rfr.p', 'wb') as model_file:
#                 pickle.dump(self.model, model_file)
#                 model_file.close()
        elif model_path.endswith('.p'):
            self.model = model_path
            logger.info('Task will load model from %s', self.model)
        else:
            raise ValueError('Urecognised option "%s" for model_path.'%(model_path))
        self.X_test_lists = [X_te[idx:idx+serve_chunk_size, :] for idx in
                             range(0, len(X_te) - serve_chunk_size, serve_chunk_size)]
        self.X_test_lists_curr = self.X_test_lists[:]
        self.X_te_index = 0
        self.sleep_time = sleep_time
        self.serve_chunk_size = serve_chunk_size

    def _generate_work_list(self, size):
        """ Generates a list of work. """
        kwargs = {}
        # Prepare the data ---------------------------------------------------------------------
        if size > len(self.X_test_lists):
            q, r = divmod(size, len(self.X_test_lists))
            load_test_data = q * self.X_test_lists + self.X_test_lists[:r]
        else:
            if size > len(self.X_test_lists) - self.X_te_index:
                self.X_te_index = 0
            load_test_data = self.X_test_lists[self.X_te_index: self.X_te_index + size]
            self.X_te_index += size
        # Generate the tasks --------------------------------------------------------------------
        enqueue_time = time.time()
        work = GeneratorLen(
            ([TaskContainer(self.base_task, timestamps={'enqueue_time': enqueue_time}),
              [self.model, load_test_data[i], self.sleep_time], kwargs]
             for i in range(size)), size)
#         print('Generated work of size %d, %d: %s, %s'%(
#             size, len(load_test_data), work, [len(elem) for elem in load_test_data]))
#         import pdb; pdb.set_trace()
        return work

