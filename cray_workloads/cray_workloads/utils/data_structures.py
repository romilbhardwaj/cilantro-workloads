class GeneratorLen(object):
    '''Generator class with a specified length'''
    def __init__(self, gen, length):
        self.gen = gen
        self.length = length

    def __len__(self):
        return self.length

    def __iter__(self):
        return self.gen

    def __next__(self):
        try:
            result = next(self.gen)
            self.length -= 1
            return result
        except StopIteration as e:
            assert self.length == 0
            raise e