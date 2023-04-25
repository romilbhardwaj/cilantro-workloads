from cray_workloads.utils.data_structures import GeneratorLen


class InsufficientStateError(BaseException):
    pass


class BaseLoadGenerator(object):
    '''
    Base class for load generators
    '''
    def generate_start_load(self, load_size=0):
        '''
        Generates a list of work [[TaskContainer, TaskArgs, TaskKwargs]] with len(load_size)
        '''
        raise NotImplementedError

    def get_load(self, **kwargs):
        '''
        Optional to implement.
        Returns a list of work [[TaskContainer, TaskArgs, TaskKwargs]] whenever polled.
        '''
        return GeneratorLen(iter(()), 0)

    def get_load_estimate(self):
        '''
        Returns the load estimated for the round. This is usually estimated at the start of the round
        '''
        raise NotImplementedError

    def round_start_callback(self):
        '''
        Called when a round starts
        '''
        pass

    def round_end_callback(self):
        '''
        Called when a round ends
        '''
        pass
