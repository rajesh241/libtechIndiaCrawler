"""Blank file which can server as starting point for writing any script file"""
import argparse
import pandas as pd
from libtech_lib.generic.commons import logger_fetch
from libtech_lib.nrega.nicnrega import nic_server_status
from libtech_lib.generic.aws import upload_s3

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
    if args['execute']:
        logger.info("Executing state status")
        df = pd.read_csv("https://libtech-india-data.s3.ap-south-1.amazonaws.com/temp_archives/stateStatus.csv", dtype={'code' :object})
        for index, row in df.iterrows():
            state_code = row.get("code")
            success = row.get("success")
            fail = row.get("fail")
            total = row.get("total")
            logger.info(state_code)
            status = nic_server_status(logger, state_code)
            if status == True:
                success = success + 1
            else:
                fail = fail + 1
            total = total + 1
            df.loc[index, "success"] = success
            df.loc[index, "fail"] = fail
            df.loc[index, "total"] = total
        df.to_csv("stateStatus.csv", index=False)
        filename = f"temp_archives/stateStatus.csv"
        file_url = upload_s3(logger, filename, df)
        logger.info(file_url)
             
    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
