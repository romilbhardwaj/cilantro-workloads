"""
    DB Query task.
    -- romilbhardwaj
    -- kirthevasank
"""

import time
import sqlite3


def _run_query(db_path, query):
    '''
    Task that executes a sqlite query on a db
    '''
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=1)
    c = conn.cursor()
    try:
        c.execute(query)
        result = c.fetchall()
    except Exception as e:
        raise e
    return result

def db_query_task(db_path, query, sleep_time):
    '''
    Task that executes a sqlite query on a db
    '''
#     start_time = time.time()
    _run_query(db_path, query)
    time.sleep(sleep_time)
#     end_time = time.time()
#     print('Received query: %s', query)
#     print('Time taken = %0.4f, sleep for %0.3f'%(end_time - start_time, sleep_time))
    return True

