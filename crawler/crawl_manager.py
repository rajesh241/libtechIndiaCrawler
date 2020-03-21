"""Blank file which can server as starting point for writing any script file"""
import argparse
import pytz
import datetime
from libtech_lib.generic.commons import logger_fetch
from libtech_lib.generic.api_interface import get_task, update_task
from libtech_lib.nrega import models
from libtech_lib.nrega.nicnrega import nic_server_status
def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This is blank script',
                                                  'you can copy this base script '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-pn', '--processName', help='Process Name', required=False)
    parser.add_argument('-e', '--execute', help='Execute Task',
                        required=False, action='store_const', const=1)
    parser.add_argument('-t', '--test', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
    parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
    args = vars(parser.parse_args())
    return args

def get_current_datetime():
    """Returns date time as string and also as python object"""
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    india_now = utc_now.astimezone(pytz.timezone("Asia/Calcutta"))
    india_now_isoformat = india_now.isoformat()
    return india_now, india_now_isoformat
 
def execute_task(logger, task_id=None, process_name=None):
    """This function will execute the task in the queue base on priority. If
    task_id is provided it will execute that particular task"""
    if process_name is None:
      process_name = 'default'
    task_dict = get_task(logger, task_id=task_id)
    state_time_obj, start_time = get_current_datetime()
    if task_dict is None:
      logger.info("Queue is empty")
      return "No Tasks to be completed"
    logger.info(task_dict)
    location_class = task_dict.get("location_class", None)
    location_code = task_dict.get("location_code", None)
    func_name = task_dict.get("report_type", None)
    scheme = task_dict.get("scheme", None)
    task_id = task_dict.get("id", None)
    my_location = getattr(models, location_class)(logger=logger, location_code=location_code)
    status = "inProgress"
    patch_data = {
        'id' : task_id,
        'status' : status,
        'process_name': process_name,
        'start_time' : start_time,
        }
    update_task(logger,  patch_data)
    is_server_running = nic_server_status(logger, location_code)
    logger.info(is_server_running)
    if not is_server_running:
        status = 'parked'
        patch_data = {
            'id' : task_id,
            'status' : status,
            'priority': 20,
            }
        update_task(logger, patch_data)
        return "Task is parked"
    method_to_call = getattr(my_location, func_name)
    patch_data = {
        'id' : task_id
    }
    try:
        method_to_call(logger)
        patch_data['is_error'] = False
        patch_data['is_done'] = True
        patch_data['status'] = 'completed'
    except Exception as e:
        remarks = e
        patch_data['is_error'] = True
        patch_data['is_done'] = False
        patch_data['status'] = 'error'
        patch_data['priority'] = 20
         
    end_time_obj, end_time = get_current_datetime()
    duration = int(((end_time_obj-state_time_obj).total_seconds())/60)
    patch_data['end_time'] = end_time
    patch_data['duration'] = duration
    update_task(logger, patch_data)
    return None

def main():
    """Main Module of this program"""
    args = args_fetch()
    logger = logger_fetch(args.get('log_level'))
    if args['test']:
        logger.info("Testing phase")
    logger.info("...END PROCESSING")
    if args['execute']:
        logger.info("Executing task")
        process_name = args.get("processName", "default")
        execute_task(logger, process_name=process_name)

if __name__ == '__main__':
    main()
