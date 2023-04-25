from collections import Counter
import sys
import time
import ray
""" This script is meant to be run from a pod in the same Kubernetes namespace
as your Ray cluster.
"""

def main():
    while True:
        print("\n\n=======Ray Resources=========")
        res = ray.cluster_resources()
        print(res)
        print(ray.available_resources())
        print(f"CPUs: {res['CPU']}")
        time.sleep(3)
        sys.stdout.flush()


if __name__ == "__main__":
    ray.init("ray://ray-head:10001")
    main()