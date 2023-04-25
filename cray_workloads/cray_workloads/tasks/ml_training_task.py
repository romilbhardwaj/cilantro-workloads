"""
    Model serving task.
    -- kirthevasank
"""

# pylint: disable=import-error

import time
import warnings
from sklearn.neural_network import MLPRegressor
from sklearn.exceptions import ConvergenceWarning

warnings.filterwarnings(action='ignore', category=ConvergenceWarning)


def _train_model(train_data, train_labels, num_iters):
    '''
    Task that serves a model on a data point.
    '''
    try:
        model = MLPRegressor(solver='sgd', hidden_layer_sizes=(64, 64, 64, 64),
                             max_iter=num_iters, activation='logistic',
                             random_state=1, learning_rate_init=0.01,
                             batch_size=len(train_data))
        for _ in range(num_iters):
            model.partial_fit(train_data, train_labels)
    except Exception as exc:
        raise exc
    return True


def ml_training_task(train_data, train_labels, num_iters, sleep_time):
    '''
    Task that serves a model on a data point.
    '''
    print('Received %d, %d data'%(len(train_data), len(train_labels)))
    start_time = time.time()
    _train_model(train_data, train_labels, num_iters)
    time.sleep(sleep_time)
    end_time = time.time()
    print('Time taken = %0.4f, sleep for %0.3f'%(end_time - start_time, sleep_time))
    return True

