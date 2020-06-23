"""Blank file which can server as starting point for writing any script file"""
import argparse

from libtech_lib.generic.commons import logger_fetch
from libtech_lib.nrega import models

def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This is blank script',
                                                  'you can copy this base script '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-t', '--test', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-d', '--download', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
    parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
    args = vars(parser.parse_args())
    return args


def main():
    """Main Module of this program"""
    args = args_fetch()
    logger = logger_fetch(args.get('log_level'))
    if args['test']:
        logger.info("Testing phase")
    if args['download']:
        logger.info("Dowloading Data")
        sample_name = "appi_ap_nrega_itda"
        force_download = True
        location_class = "APBlock"
        itda_blocks = ['0203006','0203005','0203012','0203004','0203011','0203013','0203003','0203014','0203001','0203010','0203002']
        report_types = ["ap_labour_report_r3_17", "ap_not_enrolled_r14_21A",
                        "ap_suspended_payments_r14_5", "ap_nefms_report_r14_37"]
        for report_type in report_types:
            for location_code in itda_blocks:
                my_location = getattr(models, location_class)(logger=logger,
                                                              location_code=location_code,
                                                              force_download=force_download,
                                                              sample_name=sample_name)
                method_to_call = getattr(my_location, report_type)
                method_to_call(logger)


    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
