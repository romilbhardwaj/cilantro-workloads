"""
    Define the workloads here.
    -- romilbhardwaj
    -- kirthevasank
"""

# pylint: disable=import-error

from argparse import ArgumentParser
import glob
import json
import logging
import os
import ray
# Cray
from cray_workloads.load_generators.trace_db_query_load_generator import TraceDBQueryLoadGenerator
from cray_workloads.load_generators.mltrain_load_generator import MLTrainLoadGenerator
from cray_workloads.load_generators.predserv_load_generator import PredServLoadGenerator
from cray_workloads.load_generators.trace_sleep_load_generator import TraceTaskLoadGenerator
from cray_workloads.tasks.db_query_task import db_query_task
from cray_workloads.tasks.ml_training_task import ml_training_task
from cray_workloads.tasks.predserv_task import prediction_serving_task
from cray_workloads.tasks.sleep_task import sleep_task
from cray_workloads.utils.db_utils import get_sql_commands

logger = logging.getLogger(__name__)


class WorkloadDefinition:
    """
    A helper class that encapsulates a workload's task, utilities and load definitions.
    """

    def __init__(self, *args, **kwargs):
        """ constructor. """
        # All args must be passed and saved here.
        pass

    def get_reward_fn(self):
        """ get reward function. """
        raise NotImplementedError

    def get_sigma_fn(self):
        """ get sigma function. """
        raise NotImplementedError

    def get_load_generator(self):
        """ get load generator. """
        raise NotImplementedError

    @staticmethod
    def add_args_to_parser(parser: ArgumentParser):
        """ Add arguments to parser. """
        # Add all init arguments here so that they can be passed through argparse.
        pass


class SleepTraceWorkloadDefinition(WorkloadDefinition):
    """ Sleep trace workload definition. """

    def __init__(self,
                 sleep_time: int,
                 trace_path: str,
                 *args,
                 trace_scalefactor: float = 1,
                 **kwargs):
        """ constructor. """
        self.sleep_time = sleep_time
        self.trace_path = trace_path
        self.task = ray.remote(sleep_task)
        self.trace_scalefactor = trace_scalefactor
        super().__init__(*args, **kwargs)

    def get_reward_fn(self):
        """ reward function. """
        # reward_fn = lambda metrics, round_state: compute_throughput_reward(
        #     len(metrics), round_state['load_round_total'],
        #     throughput_divide_reward_by=self.reward_divide_by)
        reward_fn = lambda metrics, round_state: 0
        return reward_fn

    def get_sigma_fn(self):
        """ sigma function. """
        # sigma_fn = lambda metrics, round_state: compute_throughput_sigma(
        #     round_state['load_round_total'], self.sigma_divide_by, self.sigma_round_duration)
        # Return just 0 since sigma and reward are calculated at the cilantro client
        sigma_fn = lambda metrics, round_state: -1
        return sigma_fn

    def get_load_generator(self):
        """ load generator. """
        load_gen = TraceTaskLoadGenerator(base_task=self.task,
                                          base_task_args=[self.sleep_time],
                                          base_task_kwargs=None,
                                          trace_path=self.trace_path,
                                          trace_scale_factor=self.trace_scalefactor)
        return load_gen

    @staticmethod
    def add_args_to_parser(parser: ArgumentParser):
        """ add args to parser """
        parser.add_argument("--sleep-time", type=float, default=0.8,
                            help="Duration for sleep task to sleep.")
        parser.add_argument("--trace-path", type=str, default='twit-b1000-n88600.csv',
                            help="Path to trace for load generation.")
        parser.add_argument("--trace-scalefactor", type=float, default=1,
                            help="Factor to scale load with (load = trace x scalefactor)")


class PredServWorkloadDefinition(WorkloadDefinition):
    """ Prediction serving workload definition. """

    def __init__(self,
                 ps_data_path: str,
                 trace_path: str,
                 sleep_time: float,
                 serve_chunk_size: int,
                 ps_model_path: str,
                 trace_scalefactor: float = 1,
                 *args,
                 **kwargs):
        """ constructor. """
        self.trace_path = trace_path
        self.ps_data_path = ps_data_path
        self.task = ray.remote(prediction_serving_task)
        self.serve_chunk_size = serve_chunk_size
        self.sleep_time = sleep_time
        self.ps_model_path = ps_model_path
        self.trace_scalefactor = trace_scalefactor
        super().__init__(*args, **kwargs)

    def get_reward_fn(self):
        """ reward function. """
        reward_fn = lambda metrics, round_state: 0
        return reward_fn

    def get_sigma_fn(self):
        """ sigma function. """
        sigma_fn = lambda metrics, round_state: -1
        return sigma_fn

    def get_load_generator(self):
        """ load generator. """
        load_gen = PredServLoadGenerator(base_task=self.task,
                                         data_path=self.ps_data_path,
                                         serve_chunk_size=self.serve_chunk_size,
                                         trace_path=self.trace_path,
                                         sleep_time=self.sleep_time,
                                         trace_scale_factor=self.trace_scalefactor,
                                         model_path=self.ps_model_path,
                                        )
        return load_gen

    @staticmethod
    def add_args_to_parser(parser: ArgumentParser):
        """ add args to parser """
        parser.add_argument("--sleep-time", type=float,
                            help="Duration for sleep task to sleep.")
        parser.add_argument("--trace-path", type=str, default='twit-b1000-n88600.csv',
                            help="Path to trace for load generation.")
        parser.add_argument("--ps-data-path", type=str,
                            help="Path with data.")
        parser.add_argument("--serve-chunk-size", type=int,
                            help="Path with data.")
        parser.add_argument("--ps-model-path", type=str, default='',
                            help="Path with model.")
        parser.add_argument("--trace-scalefactor", type=float, default=1,
                            help="Factor to scale load with (load = trace x scalefactor)")


class MLTrainWorkloadDefinition(WorkloadDefinition):
    """ ML training workload definition. """

    def __init__(self,
                 train_data_path: str,
                 train_batch_size: int,
                 train_num_iters: int,
                 sleep_time: float,
                 trace_scalefactor: float = 1,
                 request_qps: int = 1000,
                 *args,
                 **kwargs):
        """ constructor. """
        self.train_data_path = train_data_path
        self.train_batch_size = train_batch_size
        self.train_num_iters = train_num_iters
        self.task = ray.remote(ml_training_task)
        self.sleep_time = sleep_time
        self.trace_scalefactor = trace_scalefactor
        self.request_qps = request_qps
        super().__init__(*args, **kwargs)

    def get_reward_fn(self):
        """ reward function. """
        reward_fn = lambda metrics, round_state: 0
        return reward_fn

    def get_sigma_fn(self):
        """ sigma function. """
        sigma_fn = lambda metrics, round_state: -1
        return sigma_fn

    def get_load_generator(self):
        """ load generator. """
        load_gen = MLTrainLoadGenerator(base_task=self.task,
                                        data_path=self.train_data_path,
                                        batch_size=self.train_batch_size,
                                        num_iters=self.train_num_iters,
                                        sleep_time=self.sleep_time,
                                        request_qps=self.request_qps,
                                       )
        return load_gen

    @staticmethod
    def add_args_to_parser(parser: ArgumentParser):
        """ add args to parser """
        parser.add_argument("--sleep-time", type=float,
                            help="Duration for sleep task to sleep.")
        parser.add_argument("--train-data-path", type=str,
                            help="Path with data.")
        parser.add_argument("--train-batch-size", type=int,
                            help="Train data batch size.")
        parser.add_argument("--train-num-iters", type=int,
                            help="Num iters")
        parser.add_argument("--train-request-qps", type=float, default=1000,
                            help='QPS for sending training data')


class DBWorkloadDefinition(WorkloadDefinition):
    """ ML training workload definition. """

    def __init__(self,
                 db_path: str,
                 trace_path: str,
                 sleep_time: float,
                 query_bins_path: str,
                 queries_file_path: str,
                 query_bin: int,
                 trace_scalefactor: float = 1,
                 *args,
                 **kwargs):
        """ constructor. """
        self.db_path = db_path
        self.trace_path = trace_path
        self.task = ray.remote(db_query_task)
        self.sleep_time = sleep_time
        self.trace_scalefactor = trace_scalefactor
        # Load the query bins ------------------------------------------
        with open(query_bins_path, 'r') as query_file_handle:
            bin_to_query_map = json.load(query_file_handle)
            num_bins = len(bin_to_query_map)
            logger.info('Got %d bins from %s. First bin had %d queries.',
                num_bins, query_bins_path, len(list(bin_to_query_map.values())[0]))
            query_file_handle.close()
        query_str_map = self.read_queries(queries_file_path)
        query_ids = bin_to_query_map[str(query_bin)].keys()
        self.query_strs = [query_str_map[q_id] for q_id in query_ids]

        super().__init__(*args, **kwargs)

    @classmethod
    def read_queries(cls, query_path):
        """ Reads queries. """
        # Read .sql files in a directory and load all queries into a dict of filename: query_str
        queries = {}
        for f_path in glob.glob(os.path.join(query_path, '*.sql')):
            file_queries = get_sql_commands(f_path)
            queries[os.path.basename(f_path)] = file_queries[0]
        return queries

    def get_reward_fn(self):
        """ reward function. """
        reward_fn = lambda metrics, round_state: 0
        return reward_fn

    def get_sigma_fn(self):
        """ sigma function. """
        sigma_fn = lambda metrics, round_state: -1
        return sigma_fn

    def get_load_generator(self):
        """ load generator. """
        load_gen = TraceDBQueryLoadGenerator(base_task=self.task,
                                             db_path=self.db_path,
                                             queries=self.query_strs,
                                             trace_path=self.trace_path,
                                             sleep_time=self.sleep_time,
                                             trace_scale_factor=self.trace_scalefactor,
                                            )
        return load_gen

    @staticmethod
    def add_args_to_parser(parser: ArgumentParser):
        """ add args to parser """
        parser.add_argument("--sleep-time", type=float,
                            help="Duration for sleep task to sleep.")
        parser.add_argument("--trace-path", type=str, default='twit-b1000-n88600.csv',
                            help="Path to trace for load generation.")
        parser.add_argument("--db-path", type=str,
                            help="Path to the data base.")
        parser.add_argument("--trace-scalefactor", type=float, default=1,
                            help="Factor to scale load with (load = trace x scalefactor)")
        parser.add_argument("--query-bins-path", type=str,
                            help="Query bins path.")
        parser.add_argument("--queries-file-path", type=str,
                            help="Queries file path.")
        parser.add_argument("--query-bin", type=str,
                            help="Query bin.")


# Add any new workloads here to register them with the run script.
WORKLOAD_REGISTRY = {
    'sleep_task': SleepTraceWorkloadDefinition,
    'predserv_task': PredServWorkloadDefinition,
    'mltrain_task': MLTrainWorkloadDefinition,
    'db_task': DBWorkloadDefinition,
}
