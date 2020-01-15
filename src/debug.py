"""This is the Debug Script for testing the Library"""
import argparse

from commons import logger_fetch
from models import NREGAPanchayat, NREGABlock

def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This is blank script',
                                                  'you can copy this base script '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-t', '--test', help='Test Loop',
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
    if args['test']:
        logger.info("Testing phase")
        location_code = args.get('locationCode', None)
        func_name = args.get('func_name', None)
        location_type = args.get('locationType', 'panchayat')
        location_codes = []
        if location_type == 'block':
            location_codes.append(location_code)
        else:
            if len(location_code) == 7:
                block_location = NREGABlock(logger=logger,
                                            location_code=location_code)
                location_codes = block_location.get_all_panchayats(logger)
            else:
                location_codes = [location_code]

        for location_code in location_codes:
            if location_type == 'block':
                my_location = NREGABlock(logger=logger, location_code=location_code)
            else:
                my_location = NREGAPanchayat(logger=logger, location_code=location_code)
            logger.info(my_location.__dict__)
           # my_location.muster_list(logger)
            method_to_call = getattr(my_location, func_name)
            method_to_call(logger)

    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
