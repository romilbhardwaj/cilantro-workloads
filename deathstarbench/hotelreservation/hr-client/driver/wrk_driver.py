import json
import logging
import os
import subprocess
import threading
import time
from typing import Dict, List
import traceback

from kubernetes import client, config

logger = logging.getLogger(__name__)


class WrkDriver(object):
    """
    This is the driver that is called from shell and invokes wrk2 repeatedly.
    """
    K8S_NAMESPACE = 'default'
    MICROSERVICES = ['consul', 'frontend', 'geo', 'jaeger',
                     'memcached-profile', 'memcached-rate', 'memcached-reserve',
                     'mongodb-geo', 'mongodb-profile', 'mongodb-rate',
                     'mongodb-recommendation', 'mongodb-reservation',
                     'mongodb-user', 'profile', 'rate', 'recommendation',
                     'reservation', 'search', 'user']



    def __init__(self,
                 log_output_dir: str = None,
                 executable_path: str = None,
                 workload_script_path: str = None,
                 wrk_qps: int = None,
                 wrk_duration: int = 30,
                 wrk_num_threads: int = 32,
                 wrk_num_connections: int = 32,
                 wrk_url: str = 'http://frontend.default.svc.cluster.local:5000'
                 ):
        """
        :param log_output_dir: Output dir for the logs
        :param executable_path: Path to the wrk2 executable
        :param workload_script_path: Path to the workload script
        """
        self.log_output_dir = log_output_dir
        self.executable_path = executable_path
        self.workload_script_path = workload_script_path
        self.wrk_qps = wrk_qps
        self.wrk_duration = wrk_duration
        self.wrk_num_threads = wrk_num_threads
        self.wrk_num_connections = wrk_num_connections
        self.wrk_url = wrk_url


        # Init k8s clients
        self.load_k8s_config()
        self.appsapi = client.AppsV1Api()

        # These are the number of resources allocated to all microservices
        # This is externally controlled by k8s, we just need to poll it's state.
        self.resource_allocs = self.get_alloc()
        logger.info(f"Got an initial resource allocs: {self.resource_allocs}")

        # Start thread to update resource count in background.
        resource_update_thread = threading.Thread(
            target=self.update_resource_alloc_thread, args=())
        resource_update_thread.start()

    def construct_command(self,
                          qps: int,
                          num_threads: int,
                          num_connections: int,
                          duration: int,
                          url: str):
        command = f"{self.executable_path} -R {qps} -D exp -t {num_threads} -c {num_connections} -d {duration} -L -s {self.workload_script_path} {url}"
        return command

    def load_k8s_config(self):
        if os.getenv('KUBERNETES_SERVICE_HOST'):
            logger.debug(
                'Detected running inside cluster. Using incluster auth.')
            config.load_incluster_config()
        else:
            logger.debug('Using kube auth.')
            config.load_kube_config()

    def update_resource_alloc_thread(self, sleep_time=1):
        logger.info("Running resource alloc updater thread.")
        while True:
            new_resource_alloc = self.get_alloc()
            if new_resource_alloc != self.resource_allocs:
                logger.info(
                    f"Got new resource count! Setting resources to {new_resource_alloc}")
                self.resource_allocs = new_resource_alloc
            time.sleep(sleep_time)

    def get_alloc(self) -> Dict[str, int]:
        """
        Gets the number of resources allocated to each of the microservices.
        Uses kubernetes API to get the replica count.
        :return: Dict of microservice name to number of replicas (resources).
        """
        try:
            deps = self.appsapi.list_namespaced_deployment(namespace=self.K8S_NAMESPACE)
        except Exception as e:
            logger.error("Failed to get deployment list.")
            return {}
        deps = {d.metadata.name: d for d in deps.items if
                d.metadata.name.replace('root--', '') in self.MICROSERVICES}  # Filter only deployments which are actual workloads. This is to exclude cilantro client and app client deployments.

        # Use ready replicas instead of total replicas to get actual current resource allocation
        current_allocations = {dep_name: d.status.ready_replicas for
                               dep_name, d in deps.items()}
        return current_allocations

    @staticmethod
    def average_list_of_dictionaries(list_of_dicts: List[Dict[str, int]]) -> Dict[str, int]:
        """
        Given a list of dictionaries, returns a dictionary of the average values for each key.
        :param list_of_dicts: List of dictionaries.
        :return: Dictionary of average values for each key.
        """
        avg_dict = {}
        for key in list_of_dicts[0].keys():
            avg_dict[key] = sum([d[key] for d in list_of_dicts if d[key] is not None]) / len(list_of_dicts)
        return avg_dict

    def write_output_to_disk(self,
                             avg_allocs: Dict[str, float],
                             qps: int,
                             event_start_time: float,
                             event_end_time: float,
                             wrk_stdout: str
                             ):
        """
        Writes the utility message to a log file on disk.
        The utility message is written as a json.
        :return:
        """
        timestr = time.strftime("%Y%m%d-%H%M%S-%f")[:-3]
        log_filename = "output_%s.log" % timestr  # This will be the final name of the log
        log_filepath = os.path.join(self.log_output_dir, log_filename)

        with open(log_filepath, 'w') as f:
            f.write(json.dumps(avg_allocs) + '\n')
            f.write(str(qps) + '\n')
            f.write(f"event_start_time:{event_start_time}" + '\n')
            f.write(f"event_end_time:{event_end_time}" + '\n')
            f.write(wrk_stdout)


    def run_loop(self):
        while True:
            # Get the command
            command = self.construct_command(
                qps=self.wrk_qps,
                num_threads=self.wrk_num_threads,
                num_connections=self.wrk_num_connections,
                duration=self.wrk_duration,
                url=self.wrk_url)
            logger.info(f"Running command: {command}")
            start_time = time.time()
            allocs = []
            try:
                # Run subprocess in background and collect stdout
                proc = subprocess.Popen(command,
                                        shell=True,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
                while proc.poll() is None:
                    # Update and average allocations while the job is running.
                    time.sleep(0.5)
                    allocs.append(self.resource_allocs) # This is updated in the background thread.
                end_time = time.time()
                avg_allocs = self.average_list_of_dictionaries(allocs)
                stdout, stderr = proc.communicate()
                if stdout is not None:
                    stdout = stdout.decode('utf-8')
                if stderr is not None:
                    stderr = stderr.decode('utf-8')
                logger.info(f"Command finished with exit code {proc.returncode}")
                if proc.returncode != 0:
                    err_msg = stdout + "\n" + stderr
                    logger.error(f'Command failed with stderr: {err_msg}')
                    raise Exception(f"Command failed with stderr: {stderr}")
                # Check resource count in background and average it
            except Exception as e:
                logger.error(
                    f"Something went wrong but DONT PANIC. I'll not report any utility this window and "
                    f"reattempt in 1 second. Error: {e}")
                traceback.print_exc()
                time.sleep(1)
                continue

            logger.info(f"Round complete with allocation {avg_allocs}. Writing results to disk.")
            self.write_output_to_disk(avg_allocs, self.wrk_qps, start_time, end_time, stdout)
