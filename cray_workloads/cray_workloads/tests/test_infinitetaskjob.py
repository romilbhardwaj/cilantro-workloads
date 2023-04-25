import time

from cray_workloads.classes.job_container import JobContainer

from cray_workloads.classes.infinite_task_job import InfiniteTaskJobActor
from cray_workloads.tasks.sleep_task import sleep_task
import numpy as np
import ray

from cray_workloads.load_generators.trace_sleep_load_generator import TraceTaskLoadGenerator
from cray_workloads.utils.compute_slos import compute_throughput_reward


def main():
    trace_path = '/mnt/d/Romil/Berkeley/Research/cilantro/cilantro-workloads/cray_workloads/traces/twit-b1000-n88600.csv'
    task = ray.remote(sleep_task)
    args = [0.8]
    kwargs = None
    reward_fn = lambda metrics, round_state: compute_throughput_reward(
            len(metrics), round_state['load_round_total'], throughput_divide_reward_by=1)
    sigma_fn = reward_fn

    load_gen = TraceTaskLoadGenerator(base_task=task,
                                      base_task_args=args,
                                      base_task_kwargs=kwargs,
                                      trace_path=trace_path)

    job_kwargs = {"reward_fn": reward_fn,
                  "sigma_fn": sigma_fn,
                  "load_generator": load_gen}

    container = JobContainer(job_actor=InfiniteTaskJobActor,
                             job_actor_kwargs=job_kwargs)
    job_handle = container.launch_job()

    job_handle.round_start_callback.remote(resource_allocation=1)

    print("Sleeping now..")
    time.sleep(10)

    result = ray.get(job_handle.round_completion_callback.remote())

    feedback = {'reward': result['reward'],
                  'sigma': result['sigma'],
                  # 'task_metrics': result['task_metrics'],  # Removing task metrics to avoid memory bloat
                  'load_round_total': result['load_round_total'],
                  'load_round_total_to_report': result['load_round_total_to_report'],
                  'load_round_end': result['load_round_end']}

    print(feedback)

if __name__ == '__main__':
    ray.init()
    main()