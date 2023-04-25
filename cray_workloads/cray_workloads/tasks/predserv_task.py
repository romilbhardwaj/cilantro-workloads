"""
    Model serving task.
    -- kirthevasank
"""

import pickle
import time
import warnings

warnings.filterwarnings(action='ignore', category=UserWarning)

def _serve_model(model, test_data):
    '''
    Task that serves a model on a data point.
    '''
    try:
        results = model.predict(test_data)
    except Exception as exc:
        raise exc
#     results = model.predict(test_data)
    return results


def prediction_serving_task(model, test_data, sleep_time):
    '''
    Task that serves a model on a data point.
    '''
    if isinstance(model, str):
        with open(model, 'rb') as model_file:
            new_model = pickle.load(model_file)
            model_file.close()
        model = new_model
#         print('Loaded model %s.'%(model))
#     print('Received %d data'%(len(test_data)))
#     start_time = time.time()
    all_results = []
    for elem in test_data:
        curr_result = _serve_model(model, [elem])
        all_results.append(curr_result)
    time.sleep(sleep_time)
#     end_time = time.time()
#     print('Time taken = %0.4f, sleep for %0.3f'%(end_time - start_time, sleep_time))
    return True

