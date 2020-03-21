"""This is the Debug Script for testing the Library"""
import argparse

from libtech_lib.generic.commons import logger_fetch
from libtech_lib.nrega.models import NREGAPanchayat, NREGABlock, APPanchayat
from libtech_lib.nrega import models
from libtech_lib.generic.api_interface import create_task
def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This is blank script',
                                                  'you can copy this base script '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-t', '--test', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-i', '--insert', help='Insert in Queue',
                        required=False, action='store_const', const=1)
    parser.add_argument('-notnic', '--notnic', help='Not an NIC',
                        required=False, action='store_const', const=1)
    parser.add_argument('-lc', '--locationCode', help='Location Code for input', required=False)
    parser.add_argument('-lt', '--locationType',
                        help='Location type that needs tobe instantiated', required=False)
    parser.add_argument('-fn', '--func_name', help='Name of the function', required=False)
    parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
    parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
    args = vars(parser.parse_args())
    return args


def main():
    """Main Module of this program"""
    args = args_fetch()
    logger = logger_fetch(args.get('log_level'))
    if args['insert']:
        logger.info("Inserting in Crawl Queue")
        location_code = args.get('locationCode', None)
        func_name = args.get('func_name', None)
        location_type = args.get('locationType', 'panchayat')
        location_codes = []
        LOCATION_CLASS = "Location"
        if args['notnic']:
            BLOCK_CLASS = "APBlock"
            PANCHAYAT_CLASS = "APPanchayat"
        else:
            BLOCK_CLASS = "NREGABlock"
            PANCHAYAT_CLASS = "NREGAPanchayat"

        if location_type == 'block':
            location_codes.append(location_code)
        elif location_type == 'panchayat':
            if len(location_code) == 7:
                block_location = getattr(models, BLOCK_CLASS)(logger=logger,
                                            location_code=location_code)
                location_codes = block_location.get_all_panchayats(logger)
            else:
                location_codes = [location_code]
        else:
            location_codes.append(location_code)
        data = {
                'report_type' : func_name,
        }
        for location_code in location_codes:
            data['location_code'] = location_code
            if location_type == 'block':
                location_class = BLOCK_CLASS
            elif location_type == 'panchayat':
                location_class = PANCHAYAT_CLASS
                logger.info("I am here")
                logger.info(PANCHAYAT_CLASS)
            else:
                location_class = LOCATION_CLASS
            data['location_class'] = location_class
            create_task(logger, data)
     
    if args['test']:
        logger.info("Testing phase")
        location_code = args.get('locationCode', None)
        func_name = args.get('func_name', None)
        location_type = args.get('locationType', 'panchayat')
        location_codes = []
        LOCATION_CLASS = "Location"
        if args['notnic']:
            BLOCK_CLASS = "APBlock"
            PANCHAYAT_CLASS = "APPanchayat"
        else:
            BLOCK_CLASS = "NREGABlock"
            PANCHAYAT_CLASS = "NREGAPanchayat"

        if location_type == 'block':
            location_codes.append(location_code)
        elif location_type == 'panchayat':
            if len(location_code) == 7:
                block_location = getattr(models, BLOCK_CLASS)(logger=logger,
                                            location_code=location_code)
                location_codes = block_location.get_all_panchayats(logger)
            else:
                location_codes = [location_code]
        else:
            location_codes.append(location_code)

        for location_code in location_codes:
            if location_type == 'block':
                my_location = getattr(models, BLOCK_CLASS)(logger=logger, location_code=location_code)
            elif location_type == 'panchayat':
                my_location = getattr(models, PANCHAYAT_CLASS)(logger=logger, location_code=location_code)
            else:
                my_location = getattr(models, LOCATION_CLASS)(logger=logger, location_code=location_code)
            logger.info(my_location.__dict__)
           # my_location.muster_list(logger)
            method_to_call = getattr(my_location, func_name)
            method_to_call(logger)

    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
