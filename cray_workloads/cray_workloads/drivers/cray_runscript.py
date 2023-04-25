import argparse
import logging
import os
import time

import ray

from cray_workloads.drivers.cray_driver import CRayDriver
from cray_workloads.drivers.workload_definitions import WORKLOAD_REGISTRY

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(levelname)-6s | %(name)-40s || %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_env_name(k8s_svc_name: str) -> str:
    """
    Gets the envvar name for a k8s service by replacing - with _ and makign it all uppercase
    """
    env_name = k8s_svc_name.replace('-', '_')
    env_name = env_name.upper()
    return env_name


def init_ray(k8s_svc_name: str):
    """
    Initializes ray by joining the ray svc running on k8s or initing locally.
    :param k8s_svc_name:
    :return:
    """
    logger.info(f"Looking for k8s service {k8s_svc_name} envvars.")
    svc_env_name = get_env_name(k8s_svc_name)
    if svc_env_name.upper() == "NONE":
        logger.info("Initializing local ray.")
        ray.init()
    elif svc_env_name.upper() == "AUTO":
        logger.info("Auto connecting to ray.")
        # Retry connections till you succeed
        success = False
        while not success:
            try:
                ray.init(address='auto')
                success = True
                logger.info("Joined the ray cluster!")
            except Exception as e:
                logger.info(f"Unable to establish connection with auto, retrying. Erorr was {str(e)}")
                time.sleep(1)
    else:
        ip = os.getenv(svc_env_name + "_SERVICE_HOST")
        port = os.getenv(svc_env_name + "_SERVICE_PORT_CLIENT")
        if ip is None or port is None:
            raise EnvironmentError(
                f"Unable to find {svc_env_name} in envvars {os.environ}. Service name was {k8s_svc_name}")
        conn_str = f"ray://{ip}:{port}"
        logger.info(f"Trying to connect to {conn_str}")

        # Retry connections till you succeed
        success = False
        while not success:
            try:
                ray.init(conn_str)
                success = True
                logger.info("Joined the ray cluster!")
            except Exception as e:
                logger.info(f"Unable to establish connection with {conn_str}, retrying. Erorr was {str(e)}")
                time.sleep(1)




if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script to run the CilantroRay Workloads.')

    # ======= CRayDriver Args ===========
    parser.add_argument('--cray-workload-type', type=str, default="sleep_task", required=True,
                        help=f'Workload type. Must be either of f{list(WORKLOAD_REGISTRY.keys())}')
    parser.add_argument('--cray-logdir', type=str, default="/tmp/", help='Output log dir.')
    parser.add_argument('--cray-utilfreq', type=float, default=1, help='Utility reporting frequency.')

    # ====== Ray initialization args ========
    parser.add_argument('--ray-svc-name', type=str, required=True,
                        help="name of the ray service running on k8s. Specify \"none\" to init ray locally.")

    # ======= Parse args till now and infer the workload_type ========
    args, unknown_args = parser.parse_known_args()
    workload_def_cls = WORKLOAD_REGISTRY[args.cray_workload_type]

    # ======= Depending on workload_type, parse the remainder args ======
    workload_def_cls.add_args_to_parser(parser)
    args = parser.parse_args()
    logger.info(f"Command line args: {args}")

    workload_def = workload_def_cls(**vars(args))
    workload = {"reward_fn": workload_def.get_reward_fn(),
                "sigma_fn": workload_def.get_sigma_fn(),
                "load_generator": workload_def.get_load_generator()}

    # ======== Initialize Ray ==========
    init_ray(args.ray_svc_name)

    # ======== Initialize CRay Driver =========
    crdriver = CRayDriver(log_output_dir=args.cray_logdir,
                          workload=workload,
                          utility_report_frequency=args.cray_utilfreq,
                          task_watch_loop_sleep_time=0.1)

    # ======== Run CRay Driver =========
    crdriver.run_loop()
