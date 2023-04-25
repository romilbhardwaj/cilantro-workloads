class NoWorkRemainingError(Exception):
    pass


class BaseJob(object):
    def __init__(self,
                 reward_fn: callable,
                 sigma_fn: callable):
        '''
        Runs tasks as resources become available for a given resource allocation.
        :param reward_fn: Reward function that translates a list of task_metrics to a reward value
        :param sigma_fn: Function to compute sigma
        '''
        self.resource_utilization = 0
        self.reward_fn = reward_fn
        self.sigma_fn = sigma_fn

    def get_work(self):
        '''
        Returns a TaskContainer to run. Returns none when work is done.
        :return: TaskContainer
        '''
        raise NotImplementedError

    def get_load(self):
        '''
        Returns the current load of the job.
        :return: load: float
        '''
        raise NotImplementedError

    def round_start_callback(self, resource_allocation):
        '''
        Called at the start of a task
        :param resource_allocation:
        :return:
        '''
        raise NotImplementedError

    def watch_loop(self):
        '''
        Async loop which watches status of current tasks and launches new ones when they complete.
        :return:
        '''
        raise NotImplementedError


    def round_completion_callback(self):
        '''
        Called when a scheduling round is complete. Must kill all running tasks and return the utility value.
        :return: reward, sigma (estimate of confidence)
        '''
        raise NotImplementedError

