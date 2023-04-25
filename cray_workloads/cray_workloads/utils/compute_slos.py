"""
    A utility to commpute SLOs.
"""
# Some Heuristics for setting hyperparameters

# Suppose we have a maximum load parameter for each user. Call this max_load.

# unit_demand_max_bounds: If you are sure that the load would not be larger than max_load,
# then set unit_demand_max_bounds of the task group to be 1/max_load. If you are not sure, then
# set it to something like 5/max_load.

# Set lip_const_val as follows:
# lip_const_val = 10 * max_reward / (unit_demand_max_bounds)
# Here, max_reward is the maximum possible value for the reward. If using a latency based SLO,
# max_reward is 1. If its a throughput based SLO, set it to be the maximum possible throughput.

import numpy as np

def compute_throughput_reward(throughput_achieved, max_throughput, throughput_divide_reward_by):
    """ throughput_achieved is the throughput achieved in the current round.
        max_throughput is the maximum possible achievable throughput. If you know what this is,
            you can specify it. In any case, make sure its not larger than 2 x SLO, since we don't
            want it to be too conservative.
    """
    return min(throughput_achieved, max_throughput) / throughput_divide_reward_by

def compute_throughput_sigma(max_throughput, throughput_divide_reward_by, round_duration):
    """ max_throughput is the maximum possible achievable throughput. If you know what this is,
            you can specify it. In any case, make sure its not larger than 2 x SLO, since we don't
            want it to be too conservative.
    """
    norm_max_throughput = max_throughput / throughput_divide_reward_by
    return 1 / (np.sqrt(round_duration) * 20 * norm_max_throughput)

def compute_success_event_reward(num_succ_events, num_total_events):
    """ This computes rewards when we are looking at success events. One common example for this
        could be latency based SLOs where we need to complete a set of queries under a deadline.
        num_total_events: total number of events (e.g. #queries served)
        num_succ_events: number of successful events (e.g. #queries served on times)
    """
    return num_succ_events / num_total_events

def compute_success_event_sigma(num_total_events):
    """ This computes sigmas when we are looking at success events. One common example for this
        could be latency based SLOs where we need to complete a set of queries under a deadline.
        num_total_events: total number of events (e.g. #queries served)
    """
    return 1/(2 * np.sqrt(num_total_events))


def compute_latency_reward(latencies, latency_slo, num_total_events):
    """ This computes rewards when we are looking at success events. One common example for this
        could be latency based SLOs where we need to complete a set of queries under a deadline.
        latencies: list of latencies
        num_total_events: total number of events (e.g. #queries in the queue for that round)
        num_total_events: total number of events (e.g. #queries in the queue for that round)
    """
    num_success = len(np.where([lat < latency_slo for lat in latencies])[0])
    # num_fail = num_total_events - num_success
    return num_success / num_total_events


def compute_latency_sigma(latencies, latency_slo, num_total_events):
    """ This computes sigmas when we are looking at success events. One common example for this
        could be latency based SLOs where we need to complete a set of queries under a deadline.
        num_total_events: total number of events (e.g. #queries served)
    """
    return 1/(2 * np.sqrt(num_total_events))

