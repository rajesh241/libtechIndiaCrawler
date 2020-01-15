"""
This module has Queueing Framework For Libtech Queue system
"""
from threading import Thread
import threading
from queue import Queue
import pandas as pd
import queue_functions

def libtech_queue_manager(logger, joblist, num_threads=100):
    """This is the Queue Manager Function. It takes the joblist creates a queue
    and executes it"""
    job_queue = Queue(maxsize=0)
    result_queue = Queue(maxsize=0) # This is result Queue
    i = 0
    for job in joblist:
        job_queue.put(job)
    for i in range(num_threads):
        name = f"libtechWorker{i}"
        worker = Thread(name=name, target=libtech_queue_worker, args=(logger,
                                                                      job_queue,
                                                                      result_queue))
        worker.setDaemon(True)
        worker.start()

    job_queue.join()
    for i in range(num_threads):
        job_queue.put(None)
    result_array = []
    while not result_queue.empty():
        result = result_queue.get()
        if result is not None:
            result_array.append(result)
    dataframe = pd.concat(result_array, ignore_index=True)
    dataframe = dataframe.reset_index(drop=True)
    return dataframe


def libtech_queue_worker(logger, job_queue, result_queue):
    """Worker Module for Libtech Queueing System, this executes each Task and
    appends results to the result Queue"""
    name = threading.currentThread().getName()
    while True:
        obj = job_queue.get()
        if obj is None:
            break
        func_name = obj['func_name']
        func_args = obj['func_args']
        logger.info(f"QueueSize {job_queue.qsize()} Thread {name} job{func_name}")
        try:
            #result = globals()[func_name](logger, func_args, threadName=name)
            method_to_call = getattr(queue_functions, func_name)
            result = method_to_call(logger, func_args, thread_name=name)
            result_queue.put(result)
        except Exception as e_text:
            logger.error(e_text, exc_info=True)
        job_queue.task_done()
