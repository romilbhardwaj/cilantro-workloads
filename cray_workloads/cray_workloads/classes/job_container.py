from ray._raylet import ObjectID



class JobContainer(object):
    def __init__(self,
                 job_actor,
                 job_actor_kwargs,
                 ray_kwargs = {'num_cpus': 0,
                               'max_concurrency': 2}):
        self.job_actor = job_actor
        self.job_actor_kwargs = job_actor_kwargs
        self.ray_kwargs = ray_kwargs
        self.actor_handle = None

    def launch_job(self, options_kwargs=None):
        if options_kwargs is None:
            options_kwargs = {}
        self.actor_handle = self.job_actor.options(**options_kwargs, **self.ray_kwargs).remote(**self.job_actor_kwargs)
        self.actor_handle.watch_loop.remote()
        return self.actor_handle

    def empty_work(self) -> ObjectID:
        return self.actor_handle.empty_work.remote()