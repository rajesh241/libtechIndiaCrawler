"""Blank file which can server as starting point for writing any script file"""
import argparse
import time
from pathlib import Path
from datetime import datetime, timedelta
import os
from libtech_lib.generic.commons import file_logger_fetch, logger_fetch
EN_BACKEND = os.environ.get('EN_BACKEND', False)
if(EN_BACKEND):
    import django
    from django.db.models import Q

    DJANGO_SETTINGS = "base.settings"
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", DJANGO_SETTINGS)
    django.setup()
    from nrega import models as djmodels #Importing backend Django Models


def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This is blank script',
                                                  'you can copy this base script '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-t', '--test', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-e', '--execute', help='Execute',
                        required=False, action='store_const', const=1)
    parser.add_argument('-rt', '--recordType', help='Record Type', required=False)
    parser.add_argument('-td', '--timeDelay', help='Time Delay at Beginning', required=False)
    parser.add_argument('-ld', '--loggingDir', help='Loggin Director', required=False)
    parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
    parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
    args = vars(parser.parse_args())
    return args


def main():
    """Main Module of this program"""
    args = args_fetch()
    logging_dir = args.get("loggingDir", None)
    record_type = args.get("recordType", None)
    if record_type is None:
        record_type = "worker_register"
    home_dir = str(Path.home())
    if logging_dir is None:
        logging_dir = f"{home_dir}/logs"
    if not os.path.exists(logging_dir):
        os.makedirs(logging_dir)

    log_file_path = f"{logging_dir}/{record_type}.log"
    logger = file_logger_fetch(args.get('log_level'), filepath=log_file_path)
    logger = logger_fetch(args.get('log_level'))
    if args['test']:
        logger.debug("Testing phase")
        for _ in range(40000):
            logger.debug("Hello, world!")
    if args['execute']:
        logger.debug("Executing ")
        while(True):
            obj = djmodels.Record.objects.filter(Q( Q(is_downloaded=False, record_type=record_type) |
                                                    Q(is_recurring_download=True,record_type=record_type,download_date__lte=datetime.now()-timedelta(days=threshold_days)))).first().order_by("updated")
            if obj is None:
                time.sleep(5)
                continue
            logger.info(obj.id);
            obj.in_progress=True
            obj.save()
            time.sleep(2)
            obj.in_progress=False
            obj.save()
             



    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
