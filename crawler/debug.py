"""This is the Debug Script for testing the Library"""
import argparse
import pandas as pd
import datetime
from libtech_lib.generic.commons import logger_fetch
from libtech_lib.nrega.models import NREGAPanchayat, NREGABlock, APPanchayat
from libtech_lib.nrega.nicnrega import nic_server_status
from libtech_lib.nrega import models
from libtech_lib.samples import models as samplemodels
from libtech_lib.samples.models import LibtechSample, APITDABlockSample
from libtech_lib.generic.api_interface import create_task, api_get_child_locations, get_location_dict
from libtech_lib.generic.helpers import download_report
from libtech_lib.generic.html_functions import (get_dataframe_from_html,
                            get_dataframe_from_url,
                            get_urldataframe_from_url,
                            delete_divs_by_classes
                           )
def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This is blank script',
                                                  'you can copy this base script '))
    parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
    parser.add_argument('-d', '--debug', help='Debug Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-fd', '--forceDownload', help='Force Download',
                        required=False, action='store_const', const=1)
    parser.add_argument('-t', '--test', help='Test Loop',
                        required=False, action='store_const', const=1)
    parser.add_argument('-i', '--insert', help='Insert in Queue',
                        required=False, action='store_const', const=1)
    parser.add_argument('-p', '--populate', help='Populate Queue',
                        required=False, action='store_const', const=1)
    parser.add_argument('-v', '--verify', help='Verify if the state IP is working',
                        required=False, action='store_const', const=1)
    parser.add_argument('-notnic', '--notnic', help='Not an NIC',
                        required=False, action='store_const', const=1)
    parser.add_argument('-lc', '--locationCode', help='Location Code for input', required=False)
    parser.add_argument('-lt', '--locationType',
                        help='Location type that needs tobe instantiated', required=False)
    parser.add_argument('-fn', '--func_name', help='Name of the function', required=False)
    parser.add_argument('-pr', '--priority', help='Priority of the download', required=False)
    parser.add_argument('-sn', '--sample_name', help='Name of the function', required=False)
    parser.add_argument('-ls', '--location_sample', help='Location Sample Name', required=False)
    parser.add_argument('-zf', '--zipfilename', help='Zip File name', required=False)
    parser.add_argument('-td', '--tempDir', help='Temp Dir', required=False)
    parser.add_argument('-ti1', '--testInput1', help='Test Input 1', required=False)
    parser.add_argument('-ti2', '--testInput2', help='Test Input 2', required=False)
    args = vars(parser.parse_args())
    return args

def get_location_class(logger, location_type, is_not_nic):
     LOCATION_CLASS = "Location"
     if location_type == "panchayat":
         if is_not_nic:
             LOCATION_CLASS = "APPanchayat"
         else:
             LOCATION_CLASS = "NREGAPanchayat"
     if location_type == "block":
         if is_not_nic:
             LOCATION_CLASS = "APBlock"
         else:
             LOCATION_CLASS = "NREGABlock"
     if location_type == "district":
         if is_not_nic:
             LOCATION_CLASS = "APDistrict"
         else:
             LOCATION_CLASS = "NREGADistrict"
     return LOCATION_CLASS


def main():
    """Main Module of this program"""
    args = args_fetch()
    logger = logger_fetch(args.get('log_level'))
    if args['populate']:
        logger.info("Populating Crawl Queue")
        location_code = args.get('locationCode', None)
        priority = args.get('priority', 100)
        location_type = args.get('locationType', 'panchayat')
        func_name = args.get('func_name', None)
        is_not_nic = args.get('notnic', None)
        is_nic = not is_not_nic
        location_sample = args.get("location_sample", None)
        if args['forceDownload']:
            force_download = True
        else:
            force_download = False
        if location_sample is None:
            my_sample = LibtechSample(logger,parent_location_code=location_code,sample_type=location_type,
                                      force_download=force_download)
        else:
            my_sample = getattr(samplemodels, location_sample)(logger,
                                                               force_download=force_download)
      # elif location_sample == "APITDABlockSample":
      #     logger.info("I am here")
      #     my_sample = APITDABlockSample(logger,
      #                                   force_download=force_download)
      #     logger.info(my_sample.sample_location_codes)
        my_sample.populate_queue(logger, func_name, priority=priority)
    if args['insert']:
        logger.info("Inserting in Crawl Queue")
        location_code = args.get('locationCode', None)
        is_not_nic = args.get('notnic', None)
        func_name = args.get('func_name', None)
        location_type = args.get('locationType', 'panchayat')
        location_codes = []
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
            data['location_class'] = LOCATION_CLASS
            create_task(logger, data)
    if args['verify']:
        state_codes = api_get_child_locations(logger, 0)
        csv_array = []
        columns = ["state", "code", "server status"]
        for state_code in state_codes:
            logger.info(state_code)
            ldict = get_location_dict(logger, location_code=state_code)
            state_name = ldict.get("name", "")
            status = nic_server_status(logger, state_code)
            a = [state_name, state_code, status]
            csv_array.append(a)
        logger.info(csv_array)
        df = pd.DataFrame(csv_array, columns=columns)
        df.to_csv('/tmp/stateStatus.csv')
    if args['test']:
        location_sample = args.get("location_sample", None)
        zipfilename = args.get("zipfilename", "zzz")
        tempDir = args.get("tempDir", "/tmp")
        if location_sample is not None:
            my_sample = getattr(samplemodels, location_sample)(logger)
        else:
            my_sample = LibtechSample(logger, sample_type='panchayat',
                                  parent_location_code="3406007")
        report_types = ["worker_register","nic_stats", "work_payment",
                        "jobcard_transactions", "block_rejected_transactions"]
        report_types = ["nic_stats"]
        report_types = ["block_rejected_transactions", "nic_r4_1"]
        report_types = ["worker_register"]
        report_types = ["block_rejected_transactions"]
        logger.info(tempDir)
        logger.info(zipfilename)
        zip_file_name = tempDir + "/" + zipfilename
        download_dir  = tempDir + "/" + str(datetime.datetime.now().timestamp()) 
        file_url = my_sample.create_bundle(logger, report_types,
                                           download_dir=download_dir,
                                           zip_file_name=zip_file_name)
        logger.info(f"file_url is {file_url}")
       #        data={}
       #        data['location_code'] = district_code
       #        data['location_class'] = "NREGADistrict"
       #        data['report_type'] = "nic_stat_urls"
       #        create_task(logger, data)
        exit(0)
        url ="http://mnregaweb2.nic.in/Netnrega/placeHolder1/placeHolder2/../../citizen_html/musternew.aspx?lflag=&id=1&state_name=CHHATTISGARH&district_name=JASHPUR&block_name=Manora&panchayat_name=Alori&block_code=3307016&msrno=5603&finyear=2016-2017&workcode=3307016001%2fWC%2f81094155&dtfrm=27%2f02%2f2017&dtto=05%2f03%2f2017&wn=Laghu+Sichai+Talab+Nirman+Pushani+%2fRengashu+(1.60+Lakhs)&Digest=nTMkfSq3BkT80yXpUwcuFw"
        extract_dict = {}
        extract_dict['pattern'] = f"CH-"
        extract_dict['table_id_array'] = ["ctl00_ContentPlaceHolder1_grdShowRecords",
                                      "ContentPlaceHolder1_grdShowRecords"]
        extract_dict['split_cell_array'] = [1]
        cookies = None

        dataframe = get_dataframe_from_url(logger, url, mydict=extract_dict,
                                       cookies=cookies)
        logger.info(dataframe.head())
    if args['debug']:
        logger.info("Debug phase")
        if args['forceDownload']:
            force_download = True
        else:
            force_download = False
        location_code = args.get('locationCode', None)
        func_name = args.get('func_name', None)
        location_type = args.get('locationType', 'panchayat')
        if args['notnic']:
            is_nic = False
        else:
            is_nic = True
        report_name = func_name
        sample_name = args.get('sample_name', "on_demand")
        if sample_name is None:
            sample_name = "on_demand"
        result = download_report(logger, location_code, location_type, report_name,
                        is_nic=is_nic, force_download=force_download)
        exit(0)
        logger.info(f"in debug sample name is {sample_name}")
        location_codes = []
        location_class = get_location_class(logger, location_type,
                                            args['notnic'])
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
            my_location = getattr(models, location_class)(logger=logger,
                                                          location_code=location_code,
                                                          force_download=force_download,
                                                          sample_name=sample_name)
           # my_location.muster_list(logger)
            method_to_call = getattr(my_location, func_name)
            method_to_call(logger)

    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
