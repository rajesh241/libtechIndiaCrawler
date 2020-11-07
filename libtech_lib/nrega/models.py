"""This has different classes instantiated.
there is a class for each location type, and associated methods related to that
"""
#pylint: disable-msg = no-member
#pylint: disable-msg = too-few-public-methods
#File Path for on Demand
#data/samples/on_demand/<scheme>/<report_type>/district/block/panchayat/
##Archive data
import datetime
import pandas as pd
from libtech_lib.generic.commons import (get_full_finyear,
                                         get_default_start_fin_year,
                                         get_current_finyear
                                         )
from libtech_lib.generic.api_interface import (get_location_dict,
                                               create_update_report,
                                               api_get_report_dataframe,
                                               api_get_report_urls,
                                               api_get_report_last_updated,
                                               api_get_child_locations,
                                               api_get_child_location_ids,
                                               api_update_crawl_accuracy
                                              )
from libtech_lib.nrega.nicnrega import (get_jobcard_register,
                                        get_nic_block_urls,
                                        get_jobcard_register_mis,
                                        get_worker_register,
                                        get_muster_list,
                                        fetch_muster_list,
                                        update_muster_list,
                                        get_jobcard_transactions,
                                        get_block_rejected_transactions_v2,
                                        get_block_rejected_transactions,
                                        get_muster_transactions,
                                        update_muster_transactions,
                                        get_fto_status_urls,
                                        get_block_rejected_stats,
                                        get_nic_r4_1_urls,
                                        get_nic_r4_1,
                                        get_nic_stats,
                                        get_data_accuracy,
                                        create_work_payment_report,
                                        get_nic_stat_urls, 
                                        get_ap_worker_register,
                                        get_worker_stats,
                                        get_worker_register_mis,
                                        get_nic_urls,
                                        update_muster_transactions_v2,
                                        get_nrega_locations,
                                        get_dynamic_work_report_r6_18,
                                        get_nic_locations,
                                        get_jobcard_stats
                                       )
from libtech_lib.nrega.apnrega import (get_ap_jobcard_register,
                                       get_ap_muster_transactions,
                                       get_ap_not_enrolled_r14_21A,
                                       get_ap_labour_report_r3_17,
                                       get_ap_suspended_payments_r14_5,
                                       get_ap_nefms_report_r14_37,
                                       get_ap_rejected_transactions,
                                       get_ap_employment_generation_r2_2,
                                       get_ap_jobcard_updation_report_r24_43
                                      )
from libtech_lib.generic.aws import days_since_modified_s3
AP_STATE_CODE = "02"
REPORT_THRESHOLD_DICT = {
    "jobcard_register" : 15,
    "worker_register" : 1,
    "nic_urls" : 365,
    "nic_stat_urls" : 365
}
DEFAULT_REPORT_THRESHOLD = 20
class Location():
    """This is the base Location Class"""
    def __init__(self, logger, location_code, force_download='false',
                 scheme='nrega', sample_name="on_demand"):
        self.code = location_code
        self.scheme = scheme
        self.force_download = force_download
        self.sample_name = sample_name
        location_dict = get_location_dict(logger, self.code, scheme=self.scheme)
        self.logger = logger
        for key, value in location_dict.items():
            setattr(self, key, value)
    def get_child_locations(self, logger):
        """Will fetch all the child location codes"""
        location_array = api_get_child_locations(logger, self.code,
                                                  scheme=self.scheme)
        return location_array
    def fetch_report_urls(self, logger, report_type, finyear=None):
        """This will fetch the report urls"""
        report_urls = api_get_report_urls(logger, self.id, report_type,
                                          finyear=finyear)
        return report_urls
    def fetch_report_dataframe(self, logger, report_type, finyear=None):
        """Fetches the report dataframe from amazon S3"""
        dataframe = api_get_report_dataframe(logger, self.id, report_type,
                                             index_col=None, finyear=finyear)
        return dataframe
    def update_accuracy(self, logger, accuracy):
        """This will update the data accuracy in the Location database"""
        api_update_crawl_accuracy(logger, self.id, accuracy)
        
    def get_file_path(self, logger):
        """Will get file path directory for the given location"""
        if self.sample_name == "on_demand":
            filepath = f"data/samples/on_demand/{self.scheme}/reportType"
        else:
            today_date = datetime.date.today().strftime("%d_%m_%Y")
            filepath = f"data/samples/{self.sample_name}/{self.scheme}/{today_date}/reportType"
            return filepath
        if self.location_type == "country":
            return filepath
        filepath = f"{filepath}/{self.state_code}"
        if self.location_type == "state":
            return filepath
        filepath = f"{filepath}/{self.district_code}"
        if self.location_type == "district":
            return filepath
        filepath = f"{filepath}/{self.block_code}"
        if self.location_type == "block":
            return filepath
        filepath = f"{filepath}/{self.panchayat_code}"
        return filepath

    def get_report_filepath(self, logger, report_type, finyear=None):
        """Standard function to get report_filename"""
        if finyear is None:
            report_filename = f"{self.slug}_{self.code}_{report_type}.csv"
        else:
            report_filename = f"{self.slug}_{self.code}_{report_type}_{finyear}.csv"
        filepath = self.get_file_path(logger)
        filepath = filepath.replace("reportType", report_type)
        filename = f"{filepath}/{report_filename}"
        return filename
    def is_report_updated(self, logger, report_type, finyear=None):
        """Checks if report is updated"""
        if self.force_download:
            return False
        try:
            last_updated = api_get_report_last_updated(logger, self.code,
                                                       report_type,
                                                       finyear=finyear)
            
            time_diff = datetime.datetime.now(datetime.timezone.utc) - last_updated
            days_diff = time_diff.days
        except:
            days_diff = None
        if days_diff is None:
            return False
        threshold = REPORT_THRESHOLD_DICT.get(report_type,
                                              DEFAULT_REPORT_THRESHOLD)
        if days_diff > threshold:
            return False
        return True

    def save_report(self, logger, data, report_type, health="unknown",
                    finyear=None, remarks=''):
        """Standard function to save report to the location"""
        today = datetime.datetime.now().strftime('%d%m%Y')
        if data is None:
            return
        if finyear is None:
            report_filename = f"{self.slug}_{self.code}_{report_type}_{today}.csv"
        else:
            report_filename = f"{self.slug}_{self.code}_{report_type}_{finyear}_{today}.csv"
        filepath = self.get_file_path(logger)
        filepath = filepath.replace("reportType", report_type)
        filename = f"{filepath}/{report_filename}"
        logger.debug(f"Report will be saved at {filename}")
        create_update_report(logger, self.id, report_type,
                             data, filename, finyear=finyear, health=health,
                             remarks=remarks)
    def fto_status_urls(self, logger):
        """This function will get and save FTO Stat"""
        dataframe = get_fto_status_urls(self, logger)
        report_type = "fto_status_urls"
        self.save_report(logger, dataframe, report_type)
    def block_rejected_stats(self, logger):
        """This function will get block Rejected Statistics"""
        report_type = "fto_status_urls"
        fto_status_df = self.fetch_report_dataframe(logger, report_type)
        dataframe = get_block_rejected_stats(self, logger, fto_status_df)
        report_type = "block_rejected_stats"
        self.save_report(logger, dataframe, report_type)

class APPanchayat(Location):
    """This is the AP Panchayat subclass for Location Class"""
    def __init__(self, logger, location_code, force_download=False, sample_name="on_demand"):
        self.scheme = 'nrega'
        self.code = location_code
        self.force_download = force_download
        self.sample_name = sample_name
        Location.__init__(self, logger, self.code, scheme=self.scheme,
                          force_download=self.force_download,
                          sample_name=self.sample_name)
        if self.state_code == AP_STATE_CODE:
            self.home_url = "http://www.nrega.ap.gov.in/Nregs/FrontServlet"
        else:
            self.home_url = "http://www.nrega.telangana.gov.in/Nregs/FrontServlet"
    def worker_register(self, logger):
        """This will fetch the worker register of AP from nic site"""
        report_type = "worker_register"
        dataframe = get_ap_worker_register(self, logger)
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)
    def ap_rejected_transactions(self, logger):
        """This will download Rejected Transactions for AP"""
        logger.info("this method has been depreceated and instead use block level report")
    def ap_jobcard_register(self, logger):
        """Will Fetch the Jobcard Register"""
        logger.info(f"Going to fetch Jobcard register for {self.code}")
        dataframe = get_ap_jobcard_register(self, logger)
        report_type = "ap_jobcard_register"
        self.save_report(logger, dataframe, report_type)
    def ap_muster_transactions(self, logger):
        """Will Fetch the AP Muster Transactions"""
        logger.info(f"Going to fetch Jobcard register for {self.code}")
        dataframe = get_ap_muster_transactions(self, logger)
        report_type = "ap_muster_transactions"
        self.save_report(logger, dataframe, report_type)
    def ap_suspended_payments_r14_5(self, logger):
        "Will fetch R14.5 suspended payment reprot"
        dataframe = get_ap_suspended_payments_r14_5(self, logger)
        report_type = "ap_suspended_payments_r14_5"
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)

class NREGAPanchayat(Location):
    """This is the Panchayat subclass for Location Class"""
    def __init__(self, logger, location_code, force_download=False, sample_name="on_demand"):
        self.scheme = 'nrega'
        self.code = location_code
        self.force_download = force_download
        self.sample_name = sample_name
        Location.__init__(self, logger, self.code, scheme=self.scheme,
                          force_download=self.force_download,
                          sample_name=self.sample_name)
        self.nic_state_url = f"https://{self.crawl_ip}/netnrega/homestciti.aspx?state_code={self.state_code}&state_name={self.state_name}&lflag=eng"
        self.mis_state_url = f"https://mnregaweb4.nic.in/netnrega/homestciti.aspx?state_code={self.state_code}&state_name={self.state_name}&lflag=eng"
        full_finyear = get_full_finyear(get_current_finyear())
        self.panchayat_page_url = (f"http://{self.crawl_ip}/netnrega/IndexFrame.aspx?"
                                   f"lflag=eng&District_Code={self.state_code}&"
                                   f"district_name={self.district_name}"
                                   f"&state_name={self.state_name}"
                                   f"&state_Code={self.state_code}&block_name={self.block_name}"
                                   f"&block_code={self.block_code}&fin_year={full_finyear}"
                                   f"&check=1&Panchayat_name={self.panchayat_name}"
                                   f"&Panchayat_Code={self.panchayat_code}")

    def nic_urls(self, logger):
        report_type = "nic_urls"
        dataframe = get_nic_urls(self, logger)
        return dataframe
    def jobcard_register(self, logger):
        """Will Fetch the Jobcard Register"""
        logger.info(f"Going to fetch Jobcard register for {self.code}")
        report_type = "jobcard_register"
        is_updated = self.is_report_updated(logger, report_type)
        dataframe = None
        if (is_updated):
            dataframe = self.fetch_report_dataframe(logger, report_type)
            return dataframe
        my_location = NREGABlock(logger, self.block_code)
        nic_urls_df = my_location.fetch_report_dataframe(logger, "nic_block_urls")
        dataframe = get_jobcard_register_mis(self, logger, nic_urls_df)
        health = 'green'
        remarks = ''
        if len(dataframe) == 0:
            health = 'red'
            remarks = 'not able to find any table'
        self.save_report(logger, dataframe, report_type, health=health,
                         remarks=remarks)
        return dataframe
    def worker_register(self, logger):
        """Will Fetch the Jobcard Register"""
        logger.info(f"Going to fetch Jobcard register for {self.code}")
        report_type = "worker_register"
        is_updated = self.is_report_updated(logger, report_type)
        #if (is_updated) and (not self.force_download):
        if (is_updated):
            dataframe = self.fetch_report_dataframe(logger, report_type)
            return dataframe
        my_location = NREGABlock(logger, self.block_code)
        my_location.nic_urls(logger)
        nic_urls_df = my_location.fetch_report_dataframe(logger, "nic_block_urls")
        dataframe = get_worker_register_mis(self, logger, nic_urls_df)
        self.save_report(logger, dataframe, report_type)
        return dataframe
    def jobcard_transactions(self, logger):
        """This will fetch all jobcard transactions for the panchayat"""
        report_type = "jobcard_transactions"
        is_updated = self.is_report_updated(logger, report_type)
        #if (is_updated) and (not self.force_download):
        if (is_updated):
            dataframe = self.fetch_report_dataframe(logger, report_type)
            return dataframe
        logger.info(f"Going to fetch Jobcard Transactions for {self.code}")
        #self.jobcard_register(logger)
        #self.worker_register(logger)
        report_type = "jobcard_register"
        jobcard_register_df = self.fetch_report_dataframe(logger, report_type)
        dataframe = get_jobcard_transactions(self, logger, jobcard_register_df)
        report_type = "jobcard_transactions"
        self.save_report(logger, dataframe, report_type)
        return dataframe
    def muster_list_v2(self, logger):
        report_type = "muster_list"
        my_location = NREGABlock(logger, self.block_code)
        my_location.nic_urls(logger)
        nic_urls_df = my_location.fetch_report_dataframe(logger, "nic_urls")
        dataframe = fetch_muster_list(self, logger, nic_urls_df)
        return dataframe
    def muster_list(self, logger):
        """This will fetch all jobcard transactions for the panchayat"""
        logger.info(f"Going to fetch Muster list for {self.code}")
        report_type = "muster_list"
        is_updated = self.is_report_updated(logger, report_type)
        #if (is_updated) and (not self.force_download):
        if (is_updated):
            return
        self.jobcard_transactions(logger)
        report_type = "jobcard_transactions"
        jobcard_transaction_df = self.fetch_report_dataframe(logger, report_type)
        report_type = "muster_list"
        muster_list_df = self.fetch_report_dataframe(logger, report_type)
        dataframe = update_muster_list(self, logger, jobcard_transaction_df,
                                       muster_list_df)
        report_type = "muster_list"
        self.save_report(logger, dataframe, report_type)
    def correct(self, logger):
        report_type = "muster_transactions"
        filtered_transactions_df = self.fetch_report_dataframe(logger, report_type)
        filtered_transactions_df = filtered_transactions_df[filtered_transactions_df['panchayat_code'] == int(self.code)]
        report_type = "muster_transactions"
        self.save_report(logger, filtered_transactions_df, report_type)
    

    def muster_transactions(self, logger):
        """This will fetch all muster transactions for the panchayat"""
        logger.info(f"Going to fetch Muster transactions for {self.code}")
        report_type = "muster_transactions"
        is_updated = self.is_report_updated(logger, report_type)
        if (is_updated) and (not self.force_download):
            return
        self.muster_list(logger)
        dataframe = update_muster_transactions(self, logger)
        report_type = "muster_transactions"
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)

    def work_payment(self, logger):
        """this will create work payment report based on all other reports"""
        report_type = "work_payment"
        is_updated = self.is_report_updated(logger, report_type)
        if (is_updated):
            return
        self.muster_transactions(logger)
        dataframe = create_work_payment_report(self, logger)
        report_type = "work_payment"
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)
    def validate_data(self, logger):
        """This function will validate downloaded data with nic stats"""
        self.muster_transactions(logger)
        self.nic_stats(logger)
        report_type="muster_transactions"
        muster_transactions_df = self.fetch_report_dataframe(logger, report_type)
        report_type="nic_stats"
        nic_stats_df = self.fetch_report_dataframe(logger, report_type)
        accuracy = get_data_accuracy(self, logger, muster_transactions_df, nic_stats_df)
        self.update_accuracy(logger, accuracy)
        


    def nic_stats(self, logger):
        """This function will fetch NIC Stats"""
        report_type = "nic_stats"
        is_updated = self.is_report_updated(logger, report_type)
        logger.info(f"Is report updated {is_updated}")
        if is_updated:
            dataframe = self.fetch_report_dataframe(logger, report_type)
            return dataframe
        my_location = NREGADistrict(logger, self.district_code,
                                    force_download=False)
        my_location.nic_stat_urls(logger)
        report_type = "nic_stat_urls"
        nic_stat_urls_df = my_location.fetch_report_dataframe(logger, report_type)
        dataframe = get_nic_stats(self, logger, nic_stat_urls_df)
        report_type = "nic_stats"
        self.save_report(logger, dataframe, report_type)
        return dataframe


class APBlock(Location):
    """This is the AP Block subclass for Location Class"""
    def __init__(self, logger, location_code, sample_name="on_demand",
                 force_download=False):
        self.scheme = 'nrega'
        self.code = location_code
        self.force_download = force_download
        self.sample_name = sample_name
        Location.__init__(self, logger, self.code, scheme=self.scheme,
                          force_download=self.force_download,
                          sample_name=self.sample_name)
    def get_all_panchayats(self, logger):
        """Getting all child Locations, in this case getting all panchayat
        locations"""
        panchayat_array = api_get_child_locations(logger, self.code,
                                                  scheme='nrega')
        return panchayat_array
    def nic_stats(self, logger):
        """This will create NIC Location object and execute ni stats"""
        my_location = NREGABlock(logger, self.code)
        my_location.nic_stats(logger)
    def ap_jobcard_register(self, logger):
        """This function will fetch the jobcar dregister of AP"""
        report_type = "ap_jobcard_register"
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        for each_panchayat_code in panchayat_array:
            my_location = APPanchayat(logger, each_panchayat_code)
            dataframe = get_ap_jobcard_register(my_location, logger)
            if dataframe is not None:
                df_array.append(dataframe)
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type)
    def worker_register(self, logger):
        """this will fetch the worker register for entire block"""
        report_type = "worker_register"
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        for each_panchayat_code in panchayat_array:
            my_location = APPanchayat(logger, each_panchayat_code)
            dataframe = get_ap_worker_register(my_location, logger)
            if dataframe is not None:
                df_array.append(dataframe)
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type)

    def get_all_panchayat_ids(self, logger):
        """Getting all child Locations, in this case getting all panchayat
        locations IDs"""
        panchayat_array = api_get_child_location_ids(logger, self.code,
                                                     scheme='nrega')
        return panchayat_array
    def ap_suspended_payments_r14_5(self, logger):
        "Will fetch R14.5 suspended payment reprot"

        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        for each_panchayat_code in panchayat_array:
            logger.info(f"Currently Processing panchayat code {each_panchayat_code}")
            my_location = APPanchayat(logger, each_panchayat_code)
            dataframe = get_ap_suspended_payments_r14_5(my_location, logger)
            if dataframe is not None:
                df_array.append(dataframe)
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            report_type = "ap_suspended_payments_r14_5"
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type)
    def ap_not_enrolled_r14_21A(self, logger):
        "Will fetch R14.21A not enrolled reprot"
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        for each_panchayat_code in panchayat_array:
            logger.info(f"Currently Processing panchayat code {each_panchayat_code}")
            my_location = APPanchayat(logger, each_panchayat_code)
            dataframe = get_ap_not_enrolled_r14_21A(my_location, logger)
            if dataframe is not None:
                df_array.append(dataframe)
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            report_type = "ap_not_enrolled_r14_21A"
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type)

    def ap_nefms_report_r14_37(self, logger):
        "Will fetch R14.37 NEFMS reprot"
        report_type = "ap_nefms_report_r14_37"
        # FIXME df_prev = self.fetch_report_dataframe(logger, report_type) 
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        for each_panchayat_code in panchayat_array:
            logger.info(f"Currently Processing panchayat code {each_panchayat_code}")
            my_location = APPanchayat(logger, each_panchayat_code)
            dataframe = get_ap_nefms_report_r14_37(my_location, logger)
            if dataframe is not None:
                df_array.append(dataframe)
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            if dataframe is not None:
                #FIXME TBD merge here with Ranu's help
                self.save_report(logger, dataframe, report_type)
    
    
    def ap_labour_report_r3_17(self, logger):
        "Will fetch R3.17"
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        dataframe = get_ap_labour_report_r3_17(self, logger)
        report_type = "ap_labour_report_r3_17"
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)

    def ap_rejected_transactions(self, logger):
        """Will download individual level rejected transactions"""
        self.ap_nefms_report_r14_37(logger)
        #self.ap_jobcard_register(logger)
        #self.worker_register(logger)
        report_type = "ap_nefms_report_r14_37"
        fto_report_df = self.fetch_report_dataframe(logger, report_type)
        dataframe = get_ap_rejected_transactions(self, logger, fto_report_df)
        report_type = "ap_rejected_transactions"
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)

    def ap_employment_generation_r2_2(self, logger):

        report_type = "ap_employment_generation_r2_2"
        dataframe = get_ap_employment_generation_r2_2(self, logger) # this gives a csv file
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)

    def ap_jobcard_updation_report_r24_43(self, logger):

        report_type = 'ap_jobcard_updation_report_r24_43'
        dataframe = get_ap_jobcard_updation_report_r24_43(self, logger)
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)





class NREGAState(Location):
    """This is the District class for NREGA"""
    def __init__(self, logger, location_code, force_download=False, sample_name="on_demand"):
        self.scheme = 'nrega'
        self.force_download = force_download
        self.code = location_code
        self.sample_name = sample_name
        Location.__init__(self, logger, self.code, scheme=self.scheme,
                          force_download=self.force_download,
                          sample_name=self.sample_name)
        self.mis_state_url = f"https://mnregaweb4.nic.in/netnrega/homestciti.aspx?state_code={self.state_code}&state_name={self.state_name}&lflag=eng"
    def nrega_locations(self, logger):
        """This loop will crawl all the nrega Locations"""
        report_type = "nrega_locations"
        dataframe = get_nrega_locations(self, logger)
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)
    def nic_r4_1_urls(self, logger):
        """This will fetch MIS URLs based on pattern"""
        report_type = 'nic_r4_1_urls'
        url_text = 'Fortnight_rep3.aspx'
        url_prefix = "http://mnregaweb4.nic.in/netnrega/state_html/"
        dataframe = get_nic_r4_1_urls(self, logger, report_type=report_type,
                                 url_text=url_text, url_prefix=url_prefix)
        self.save_report(logger, dataframe, report_type)
    def nic_r14_5_urls(self, logger):
        """This will fetch MIS URLs based on pattern"""
        report_type = 'nic_r14_5_urls'
        url_text = 'delayed_payment.aspx'
        url_prefix = "http://mnregaweb4.nic.in/netnrega/state_html/"
        dataframe = get_nic_r4_1_urls(self, logger, report_type=report_type,
                                 url_text=url_text, url_prefix=url_prefix)
        self.save_report(logger, dataframe, report_type)

class NREGADistrict(Location):
    """This is the District class for NREGA"""
    def __init__(self, logger, location_code, force_download=False, sample_name="on_demand"):
        self.scheme = 'nrega'
        self.force_download = force_download
        self.code = location_code
        self.sample_name = sample_name
        self.mis_state_url = "https://mnregaweb4.nic.in/netnrega/homestciti.aspx?state_code={self.state_code}&state_name={self.state_name}&lflag=eng"
        Location.__init__(self, logger, self.code, scheme=self.scheme,
                          force_download=self.force_download,
                          sample_name=self.sample_name)
    def get_all_blocks(self, logger):
        """Getting all child Locations, in this case getting all panchayat
        locations"""
        block_array = api_get_child_locations(logger, self.code,
                                                  scheme='nrega')
        return block_array
    def nic_stat_urls(self, logger):
        """This function will get the nic stat URLs for all the panchayats and
        blocks"""
        report_type = "nic_stat_urls"
        logger.debug(f"force download is {self.force_download}")
        is_updated = self.is_report_updated(logger, report_type)
        #logger.info(f"CHecking if nic stat urls is updated{is_updated}")
        if is_updated:
            return
        dataframe_array = []
        block_array = self.get_all_blocks(logger)
        logger.info(block_array)
        for block_code in block_array:
            my_location = NREGABlock(logger, block_code)
            dataframe = my_location.nic_stat_urls(logger)
            if dataframe is not None:
                dataframe_array.append(dataframe)
        if len(dataframe_array) > 0:
            dataframe = pd.concat(dataframe_array)
            report_type = "nic_stat_urls"
            self.save_report(logger, dataframe, report_type)
    def nic_stats(self, logger):
        """This will fetch NIC Stats for all the blocks and panchayats under
        this district"""
        block_array = self.get_all_blocks(logger)
        for block_code in block_array:
            my_location = NREGABlock(logger, block_code,
                                     force_download=self.force_download)
            my_location.nic_stats(logger)

        


class NREGABlock(Location):
    """This is the Panchayat subclass for Location Class"""
    def __init__(self, logger, location_code, force_download=False, sample_name="on_demand"):
        self.scheme = 'nrega'
        self.force_download = force_download
        self.code = location_code
        self.sample_name = sample_name
        Location.__init__(self, logger, self.code, scheme=self.scheme,
                          force_download=self.force_download,
                          sample_name=self.sample_name)
        self.mis_state_url = f"https://mnregaweb4.nic.in/netnrega/homestciti.aspx?state_code={self.state_code}&state_name={self.state_name}&lflag=eng"
        self.mis_block_url = f"https://mnregaweb4.nic.in/netnrega/Progofficer/PoIndexFrame.aspx?flag_debited=S&lflag=eng&District_Code={self.district_code}&district_name={self.district_name}&state_name={self.state_name}&state_Code={self.state_code}&finyear=fullFinYear&check=1&block_name={self.block_name}&Block_Code={self.block_code}"
    def get_all_panchayats(self, logger):
        """Getting all child Locations, in this case getting all panchayat
        locations"""
        panchayat_array = api_get_child_locations(logger, self.code,
                                                  scheme='nrega')
        return panchayat_array
    def get_all_panchayat_ids(self, logger):
        """Getting all child Locations, in this case getting all panchayat
        locations IDs"""
        panchayat_array = api_get_child_location_ids(logger, self.code,
                                                     scheme='nrega')
        return panchayat_array

    def get_all_panchayat_objs(self, logger):
        """Getting all child Locations, in this case getting all panchayat
        locations"""
        pobj_array = []
        panchayat_array = api_get_child_locations(logger, self.code,
                                                  scheme='nrega')
        for panchayat_code in panchayat_array:
            pobj_array.append(NREGAPanchayat(logger, panchayat_code))

        return pobj_array

    def nic_locations(self, logger):
        report_type = "nic_locations"
        dataframe = get_nic_locations(self, logger)
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)
    def nic_block_urls(self, logger):
        report_type = "nic_block_urls"
        dataframe = get_nic_block_urls(self, logger)
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)
    def nic_urls(self, logger):
        report_type = "nic_urls"
        is_updated = self.is_report_updated(logger, report_type)
        if is_updated:
            return
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        count = 0
        remarks = ''
        health = 'green'
        for each_panchayat_code in panchayat_array:
            count = count + 1
            my_location = NREGAPanchayat(logger, each_panchayat_code)
            dataframe = my_location.nic_urls(logger)
            if dataframe is not None:
                logger.info(f"Processed {count} {my_location.code}")
                df_array.append(dataframe)
            else:
                health = "red"
                remarks = remarks + "unable to download for " + each_panchayat_code
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type, health,
                                 remarks)
    def jobcard_transactions(self, logger):
        report_type = "jobcard_transactions"
        is_updated = self.is_report_updated(logger, report_type)
        if is_updated:
            return
        logger.info("Jobcard Transactions is not updated")
        input()
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        count = 0
        remarks = ''
        health = 'green'
        for each_panchayat_code in panchayat_array:
            logger.debug(f"Currently processing {each_panchayat_code}")
            count = count + 1
            my_location = NREGAPanchayat(logger, each_panchayat_code)
            dataframe = my_location.jobcard_transactions(logger)
            if dataframe is not None:
                logger.info(f"Processed {count} {my_location.code}")
                df_array.append(dataframe)
            else:
                health = "red"
                remarks = remarks + "unable to download for " + each_panchayat_code
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type, health,
                                 remarks)
    
    def muster_list(self, logger):
        """This will fetch all jobcard transactions for the panchayat"""
        logger.info(f"Going to fetch Muster list for {self.code}")
        report_type = "muster_list"
        is_updated = self.is_report_updated(logger, report_type)
        #if (is_updated) and (not self.force_download):
        if (is_updated):
            return
        self.jobcard_transactions(logger)
        report_type = "jobcard_transactions"
        jobcard_transaction_df = self.fetch_report_dataframe(logger, report_type)
        report_type = "muster_list"
        muster_list_df = self.fetch_report_dataframe(logger, report_type)
        dataframe = update_muster_list(self, logger, jobcard_transaction_df,
                                       muster_list_df)
        report_type = "muster_list"
        self.save_report(logger, dataframe, report_type)

    def muster_transactions(self, logger):
        """This will fetch all muster transactions for the panchayat"""
        logger.info(f"Going to fetch Muster transactions for {self.code}")
        report_type = "muster_transactions"
        is_updated = self.is_report_updated(logger, report_type)
       #if (is_updated) and (not self.force_download):
       #    return
        self.muster_list(logger)
        dataframe = update_muster_transactions(self, logger)
        report_type = "muster_transactions"
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)


    def dynamic_work_report_r6_18(self, logger):
        """This will fetch the dynamic work report from MIS reports"""
        report_type = "dynamic_work_report_r6_18"
        dataframe = get_dynamic_work_report_r6_18(self, logger)
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)
    def muster_transactions_v2(self, logger):
        """This will download muster transactions based on muster list"""
        report_type = "muster_transactions_v2"
        dataframe = update_muster_transactions_v2(self, logger)
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)
    def nic_r4_1(self, logger):
        '''Download MIS NIC 4_1 report'''
        report_type = "nic_r4_1"
        state_obj = Location(logger, self.state_code)
        report_name = "nic_r4_1_urls"
        url_df = state_obj.fetch_report_dataframe(logger, report_name)
        if url_df is None:
            state_obj.nic_r4_1_urls(logger)
            url_df = state_obj.fetch_report_dataframe(logger, report_name)
        start_fin_year = get_default_start_fin_year()
        end_fin_year = get_current_finyear()
        for finyear in range(start_fin_year, end_fin_year+1):
            finyear = str(finyear)
            dataframe = get_nic_r4_1(self, logger, url_df, finyear)
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type,
                                 finyear=finyear)
    def jobcard_stats(self, logger):
        '''
        Get jobcard stats based on S4.15
        '''
        report_type = 'jobcard_stats'
        df = get_jobcard_stats(self, logger)
        if df is not None:
            self.save_report(logger, df, report_type)

    def nic_stats(self, logger):
        """This function will fetch NIC Stats"""
        report_type = "nic_stats"
        is_updated = self.is_report_updated(logger, report_type)
        if is_updated:
            return
        my_location = NREGADistrict(logger, self.district_code,
                                    force_download=False)
        my_location.nic_stat_urls(logger)
        report_type = "nic_stat_urls"
        nic_stat_urls_df = my_location.fetch_report_dataframe(logger, report_type)
        health = "green"
        remarks = ''
        df_array = []
        report_type = "nic_stats"
        panchayat_array = self.get_all_panchayats(logger)
        dataframe = get_nic_stats(self, logger, nic_stat_urls_df)
        if len(dataframe) > 0:
            df_array.append(dataframe)
        else:
            health = "red"
            remarks = f"Unable to download for {self.code}"
        for each_panchayat_code in panchayat_array:
            my_location = NREGAPanchayat(logger, each_panchayat_code,
                                         force_download=self.force_download)
            dataframe = my_location.nic_stats(logger)
            if ( (dataframe is None) or (len(dataframe) == 0)):
                health = "red"
                remarks = remarks + f"Unable to download for {each_panchayat_code}\n"
            else:
                df_array.append(dataframe)

        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type, health=health,
                                remarks=remarks)

    def jobcard_register(self, logger):
        """This will fetch jobcard register for each panchayat in the block"""
        report_type = "jobcard_register"
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        remarks = ''
        health = 'green'
        for each_panchayat_code in panchayat_array:
            my_location = NREGAPanchayat(logger, each_panchayat_code)
            dataframe = my_location.jobcard_register(logger)
            if ( (dataframe is None) or (len(dataframe) == 0)):
                health = "red"
                remarks = remarks + f"Unable to download for {each_panchayat_code}\n"
            else:
                df_array.append(dataframe)
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type, health=health,
                                remarks=remarks)
    def muster_list_v2(self, logger):
        """This will fetch jobcard register for each panchayat in the block"""
        report_type = "muster_list_v2"
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        for each_panchayat_code in panchayat_array:
            my_location = NREGAPanchayat(logger, each_panchayat_code)
            dataframe = my_location.muster_list_v2(logger)
            if dataframe is not None:
                df_array.append(dataframe)
            if len(df_array) > 0:
                break
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type)
    def worker_register(self, logger):
        """This will fetch worker register for each panchayat in the block"""
        report_type = "worker_register"
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        for each_panchayat_code in panchayat_array:
            my_location = NREGAPanchayat(logger, each_panchayat_code)
            dataframe = my_location.worker_register(logger)
            if dataframe is not None:
                df_array.append(dataframe)
        if len(df_array) > 0:
            dataframe = pd.concat(df_array)
            if dataframe is not None:
                self.save_report(logger, dataframe, report_type)
    def block_rejected_transactions_v2(self, logger):
        """This will fetch all the rejected transactions of the block"""
        report_name = "block_rejected_stats"
        india_obj = Location(logger, '0')
        rej_stat_df = india_obj.fetch_report_dataframe(logger, report_name)
        dataframe = get_block_rejected_transactions_v2(self, logger, rej_stat_df)
        if dataframe is not None:
            report_type = "block_rejected_transactions_v2"
            self.save_report(logger, dataframe, report_type)
    def worker_stats(self, logger):
        """This will fetch the worker stats"""
        report_type = "worker_stats"
        nic_urls_df = self.fetch_report_dataframe(logger, "nic_block_urls")
        dataframe = get_worker_stats(self, logger, nic_urls_df)
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)

    def block_rejected_transactions(self, logger):
        """This will fetch all the rejected transactions of the block"""
        report_name = "block_rejected_stats"
        india_obj = Location(logger, '0')
        rej_stat_df = india_obj.fetch_report_dataframe(logger, report_name)
        dataframe = get_block_rejected_transactions(self, logger, rej_stat_df)
        if dataframe is not None:
            report_type = "block_rejected_transactions"
            self.save_report(logger, dataframe, report_type)
        
    def block_reference_document(self, logger):
        """This will crawl data for the entire block"""
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        transactions_df_array = []
        for each_panchayat_code in panchayat_array:
            logger.info(f"Currently Processing panchayat code {each_panchayat_code}")
            my_location = NREGAPanchayat(logger, each_panchayat_code)
            my_location.correct(logger)
            my_location.validate_data(logger)
            report_type="muster_transactions"
            muster_transactions_df = my_location.fetch_report_dataframe(logger, report_type)
            transactions_df_array.append(muster_transactions_df)
        muster_transactions_df = pd.concat(transactions_df_array)
        report_type="nic_stats"
        nic_stats_df = self.fetch_report_dataframe(logger, report_type)
        accuracy = get_data_accuracy(self, logger, muster_transactions_df, nic_stats_df)
        self.update_accuracy(logger, accuracy)
        logger.info(f"Shape of  is {muster_transactions_df.shape}")
        
        #self.block_rejected_transactions(logger)
    def nic_stat_urls(self, logger):
        """This function will get the nic stat URLs for all the panchayats and
        blocks"""
        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        dataframe = get_nic_stat_urls(self, logger, panchayat_array)
        return dataframe

