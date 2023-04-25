import argparse
import logging
import os

from wrk_driver import WrkDriver

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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script to run the Wrk load generator.')

    # ======= WrkDriver Args ===========
    parser.add_argument('--wrk-logdir', type=str, default="/tmp/",
                        help='Output log dir.')
    parser.add_argument('--wrk-executable-path', type=str, default="./wrk2/wrk",
                        help='Path to wrk executable')
    parser.add_argument('--wrk-script-path', type=str,
                        default="/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua",
                        help='Path to wrk workload script')
    parser.add_argument('--wrk-qps', type=int, default=100, help='Target QPS')
    parser.add_argument('--wrk-duration', type=int, default=30,
                        help='Duration to run wrk for')
    parser.add_argument('--wrk-num-threads', type=int, default=32,
                        help='Number of threads to run wrk with')
    parser.add_argument('--wrk-num-connections', type=int, default=32,
                        help='Number of connections to run wrk with')
    parser.add_argument('--wrk-url', type=str,
                        default="http://frontend.default.svc.cluster.local:5000",
                        help='Target URL for wrk')
    args = parser.parse_args()

    # ======== Initialize Wrk Driver =========
    os.makedirs(args.wrk_logdir, exist_ok=True)
    driver = WrkDriver(log_output_dir=args.wrk_logdir,
                       executable_path=args.wrk_executable_path,
                       workload_script_path=args.wrk_script_path,
                       wrk_qps=args.wrk_qps,
                       wrk_duration=args.wrk_duration,
                       wrk_num_threads=args.wrk_num_threads,
                       wrk_num_connections=args.wrk_num_connections,
                       wrk_url=args.wrk_url)

    # ======== Run Driver =========
    driver.run_loop()
