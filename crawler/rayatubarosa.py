"""Blank file which can server as starting point for writing any script file"""
import argparse
import pandas as pd
from libtech_lib.lib.commons import logger_fetch
from libtech_lib.lib.aws import upload_s3
from libtech_lib.rayatubarosa.models import RBLocation, RBCrawler
def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This is blank script',
                                                  'you can copy this base script '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-t', '--test', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-c', '--crawl', help='Crawl Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-lc', '--locationCode', help='Location Code', required=False)
    parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
    args = vars(parser.parse_args())
    return args

#G Madugula locaiton code is 4848
def main():
    """Main Module of this program"""
    args = args_fetch()
    logger = logger_fetch(args.get('log_level'))
    if args['test']:
        logger.info("Testing")
        #village_df_url = "https://libtech-india-data.s3.ap-south-1.amazonaws.com/data/locations/rayatu_barosa/vskp_villages.csv"
        #village_df = pd.read_csv(village_df_url, index_col=0)
        filename = 'data/locations/rayatu_barosa/vskp_villages.csv'
        village_df = pd.read_csv("~/thrash/v.csv")
        upload_s3(logger, filename, village_df)
    if args['crawl']:
        location_code = args['locationCode']
        rb_crawler = RBCrawler(logger)
        dataframe = rb_crawler.get_crawl_df(logger, block_code=location_code)
        logger.info(dataframe.head())
    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
