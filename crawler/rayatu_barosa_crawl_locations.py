"""This is the Debug Script for testing the Library"""
#pylint: disable-msg = too-many-locals
#pylint: disable-msg = too-many-branches
#pylint: disable-msg = too-many-statements
#pylint: disable-msg = broad-except
import argparse
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait

from libtech_lib.lib.commons import logger_fetch
from libtech_lib.wrappers.sn import driverInitialize, driverFinalize
from libtech_lib.lib.aws import get_aws_parquet, upload_s3
from libtech_lib.lib.html_functions import get_dataframe_from_html
from libtech_lib.rayatubarosa.models import RBCrawler
def args_fetch():
    '''
    Paser for the argument list that returns the args list
    '''

    parser = argparse.ArgumentParser(description=('This script will crawl',
                                                  'locations for ryatu barosa '))
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

class Crawler():
    """Selenium crawler class for crawling ryatu barosa website"""
    def __init__(self):
        #self.driver = driverInitialize(timeout=3)
        self.driver = ''
        self.vars = {}
        self.district_url = 'https://ysrrythubharosa.ap.gov.in/RBApp/Reports/RBDistrictPaymentAbstract'
        self.payment_url = 'https://ysrrythubharosa.ap.gov.in/RBApp/Reports/PaymentvillReport'
       # /data/locations/ap_census/village_all_ap_villages/
        self.census_parquet_village_filename = "data/locations/ap_census/all_ap_villages/part-00000-9270a97e-3293-45b6-b1fd-2fed8304fc12-c000.snappy.parquet"
    def teardown_method(self):
        """To tear down the class"""
        driverFinalize(self.driver)
    def read_census_parquet(self, logger):
        """Will read the census parquet file from Amazon S3"""
        dataframe = get_aws_parquet(self.census_parquet_village_filename)
        logger.info(dataframe.head())
    def wait_for_window(self, timeout=2):
        """This function will wait for the new window to open based on timeout
        and return the new window handle"""
        time.sleep(round(timeout / 1000))
        wh_now = self.driver.window_handles
        wh_then = self.vars["window_handles"]
        if len(wh_now) > len(wh_then):
            return set(wh_now).difference(set(wh_then)).pop()
        return None
    def login_portal(self, logger):
        """this function will log in to the website"""
        url = 'https://ysrrythubharosa.ap.gov.in/RBApp/RB/Login'
        logger.info('Fetching URL[%s]' % url)
        self.driver.get(url)
        time.sleep(3)

        user = '9959905843'
        elem = self.driver.find_element_by_xpath('//input[@type="text"]')
        logger.info('Entering User[%s]' % user)
        elem.send_keys(user)

        password = '9959905843'
        elem = self.driver.find_element_by_xpath('//input[@type="password"]')
        logger.info('Entering Password[%s]' % password)
        elem.send_keys(password)


        login_button = '(//button[@type="button"])[2]'

        elem = self.driver.find_element_by_xpath(login_button)
        logger.info('Clicking Login Button')
        #elem.click()
        time.sleep(15)
        input()
    def print_current_window_handles(self, logger, event_name=None):
        """Debug function to print all the window handles"""
        handles = self.driver.window_handles
        logger.info(f"Printing current window handles after {event_name}")
        for index, handle in enumerate(handles):
            logger.info(f"{index}-{handle}")
    def download_payment_report(self, logger, village_df):
        """This function will download the payment report from the villages"""
        district_list = village_df['district_name_telugu'].unique().tolist()
        
        logger.info(district_list)
        #district_list = [self.district]
        
    def crawl_villages(self, logger, filename):
        """This function will crawl all the villages given district and block
        names"""
        district_url = 'https://ysrrythubharosa.ap.gov.in/RBApp/Reports/RBDistrictPaymentAbstract'
        payment_url = 'https://ysrrythubharosa.ap.gov.in/RBApp/Reports/PaymentvillReport'
        ##Now lets get the unique districts from the input dataframe
        village_stat_df_array = []
        village_name_df_array = []
        extract_dict = {}
        extract_dict['pattern'] = "Village Name"
        ##CSV to be built for village names
        csv_array = []
        column_headers = ["district_name_telugu", "block_name_telugu",
                          "village_name_telugu", "village_code"]
        ##First we will Login in to the ryatu barosa website
        self.login_portal(logger)
        ##Next we will fetch the distrit page and load it in one of the windoes
        logger.info('Fetching URL[%s]' % district_url)
        self.driver.get(district_url)
        ##Lets get the current window and save it as district_window
        self.vars["window_handles"] = self.driver.window_handles
        self.vars['district_window'] = self.driver.current_window_handle
        self.print_current_window_handles(logger,
                                          event_name="After district page fetch")
        dataframe = pd.read_csv(filename)
        district_list = dataframe['district_name_telugu'].unique().tolist()
        #district_list = [self.district]
        logger.info(f"District list is {district_list}")
        time.sleep(5)
        for dist_no, district_name in enumerate(district_list):
            self.driver.switch_to.window(self.vars["district_window"])
            logger.info(f"Currently processling {dist_no}-{district_name}")
            mandal_df = dataframe[dataframe['district_name_telugu'] == district_name]
            mandal_list = mandal_df["block_name_telugu"].unique().tolist()
            logger.info(f"Mandal list for {district_name} is {mandal_list}")
            self.vars["window_handles"] = self.driver.window_handles
            WebDriverWait(self.driver,
                          5).until(EC.element_to_be_clickable((By.LINK_TEXT,
                                                               district_name))).click()

            logger.info("after district click")
            self.print_current_window_handles(logger)

            self.vars["mandal_window"] = self.wait_for_window(5)
            for mandal_no, mandal_name in enumerate(mandal_list):
                csv_array = []
                logger.info(f"processing {dist_no}-{district_name}-{mandal_no}-{mandal_name}")
                logger.info(self.vars)
                self.driver.switch_to.window(self.vars["mandal_window"])
                self.vars["window_handles"] = self.driver.window_handles
                logger.info("after waiting district click")
                self.print_current_window_handles(logger)

                logger.info("Will sleep for 5 seconds")
                time.sleep(5)


                WebDriverWait(self.driver,
                              5).until(EC.element_to_be_clickable((By.LINK_TEXT,
                                                                   mandal_name))).click()
                logger.info("after block click")
                self.print_current_window_handles(logger)

                self.vars["village_window"] = self.wait_for_window(5)
                self.driver.switch_to.window(self.vars["village_window"])
                self.vars["window_handles"] = self.driver.window_handles
                ### Here I should do all theprocessing of downloading the stats
                ### Also getting all the village codes.
                time.sleep(2)
                myhtml = self.driver.page_source
                village_stat_df = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
                logger.info('Fetching URL[%s]' % payment_url)
                self.driver.get(payment_url)
                time.sleep(3)
                self.print_current_window_handles(logger)
                village_x_path = "//select[1]"
                try:
                    village_select = Select(self.driver.find_element_by_xpath(village_x_path))
                except Exception as e:
                    logger.error(f'Exception during village_select ',
                                 f'for {village_x_path} - EXCEPT[{type(e)}, {e}]')
                    return 'FAILURE'
                for option in village_select.options:
                    if option.text != "Select Village":
                        row = [district_name, mandal_name, option.text,
                               option.get_attribute('value')]
                        csv_array.append(row)
                village_name_df = pd.DataFrame(csv_array, columns=column_headers)
                village_stat_df = village_stat_df.merge(village_name_df,
                                                        how='left',
                                                        left_on='Village Name',
                                                        right_on='village_name_telugu'
                                                       )
                #village_stat_df.to_csv("~/thrash/a.csv")
                village_stat_df_array.append(village_stat_df)
                village_name_df_array.append(village_name_df)
                logger.info(f"Now I am going to close the village Window")
                self.print_current_window_handles(logger,
                                                  event_name="beforevllageclose")
                self.driver.close()#This will close the village window
                self.print_current_window_handles(logger,
                                                  event_name="aftervllageclose")
            self.driver.switch_to.window(self.vars["mandal_window"])
            self.print_current_window_handles(logger,
                                              event_name="beforemandalclose")
            self.driver.close()#This will close the Mandal Window
            self.print_current_window_handles(logger,
                                              event_name="aftermandalclose")
            self.print_current_window_handles(logger)
        dataframe = pd.concat(village_stat_df_array, ignore_index=True)
        dataframe.to_csv("ryatu_barosa_village_stat.csv")
        dataframe = pd.concat(village_name_df_array, ignore_index=True)
        dataframe.to_csv("ryatu_barosa_villages.csv")
        self.driver.switch_to.window(self.vars["district_window"])
        self.print_current_window_handles(logger)
        return None
    def crawl_district_block_names(self, logger, filename):
        """This module will crawl all the districts and blocks from Ryatu
        Barosa website"""
        base_url = 'https://ysrrythubharosa.ap.gov.in/RBApp/Reports/RBDistrictPaymentAbstract'
        logger.info('Fetching URL[%s]' % base_url)
        self.driver.get(base_url)
        time.sleep(3)
        self.vars["window_handles"] = self.driver.window_handles
        myhtml = self.driver.page_source
        extract_dict = {}
        extract_dict['pattern'] = "District Name"
        dataframe = get_dataframe_from_html(logger, myhtml, mydict=extract_dict)
        logger.info(dataframe.head())
        district_list = dataframe["District Name"].tolist()
        district_list.remove('Total')
        logger.info(district_list)
        #input()
        extract_dict = {}
        extract_dict['pattern'] = "Mandal Name"
        csv_array = []
        column_headers = ["district_name_telugu", "block_name_telugu"]
        for district_name in district_list:
            self.driver.get(base_url)
            time.sleep(3)
            WebDriverWait(self.driver,
                          5).until(EC.element_to_be_clickable((By.LINK_TEXT,
                                                               district_name))).click()
            self.vars["win9760"] = self.wait_for_window(2000)
            self.driver.switch_to.window(self.vars["win9760"])
            self.vars["window_handles"] = self.driver.window_handles
            myhtml = self.driver.page_source
            dataframe = get_dataframe_from_html(logger, myhtml,
                                                mydict=extract_dict)

            mandal_list = dataframe["Mandal Name"].tolist()
            mandal_list.remove('Total')
            logger.info(mandal_list)
            for mandal_name in mandal_list:
                row = [district_name, mandal_name]
                csv_array.append(row)
        dataframe = pd.DataFrame(csv_array, columns=column_headers)
        dataframe.to_csv(filename)

def main():
    """Main Module of this program"""
    args = args_fetch()
    logger = logger_fetch(args.get('log_level'))
    if args['test']:
        logger.info("Testing phase")
        location_code = args['locationCode']
        rb_crawler = RBCrawler(logger)
       # village_df = rb_crawler.get_crawl_df(logger, block_code=location_code)
       # village_df.to_csv("~/thrash/village_df.csv") 
        village_df = pd.read_csv("~/thrash/village_df.csv")
        my_crawler = Crawler()
        #filename = "rayatu_barosa_district_block.csv"
        my_crawler.download_payment_report(logger, village_df)
        #my_crawler.read_census_parquet(logger)
        #my_crawler.teardown_method()

    logger.info("...END PROCESSING")

if __name__ == '__main__':
    main()
