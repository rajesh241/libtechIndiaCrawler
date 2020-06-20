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
                                         get_current_finyear
                                         )
from libtech_lib.generic.api_interface import (get_location_dict,
                                               create_update_report,
                                               api_get_report_dataframe,
                                               api_get_child_locations,
                                               api_get_child_location_ids,
                                               api_update_crawl_accuracy
                                              )
from libtech_lib.nrega.nicnrega import (get_jobcard_register,
                                        get_worker_register,
                                        get_muster_list,
                                        get_jobcard_transactions,
                                        get_block_rejected_transactions,
                                        get_muster_transactions,
                                        get_fto_status_urls,
                                        get_block_rejected_stats,
                                        get_nic_stats,
                                        get_data_accuracy,
                                        get_nic_stat_urls
                                       )
from libtech_lib.nrega.apnrega import (get_ap_jobcard_register,
                                       get_ap_muster_transactions,
                                       get_ap_suspended_payments_r14_5
                                      )
from libtech_lib.generic.aws import days_since_modified_s3
AP_STATE_CODE = "02"
REPORT_THRESHOLD_DICT = {
    "jobcard_register" : 15,
    "worker_register" : 15,
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
        for key, value in location_dict.items():
            setattr(self, key, value)
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
            filepath = f"data/samples/{self.sample_name}/{self.scheme}/reportType/{today_date}"
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
        logger.info("filepath is ")
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
        filename = self.get_report_filepath(logger, report_type, finyear=finyear)
        logger.info(f"report name is {filename}")
        days_diff = days_since_modified_s3(logger, filename)
        logger.info(f"days diff is {days_diff}")
        if days_diff is None:
            return False
        threshold = REPORT_THRESHOLD_DICT.get(report_type,
                                              DEFAULT_REPORT_THRESHOLD)
        if days_diff > threshold:
            return False
        return True

    def save_report(self, logger, data, report_type, finyear=None):
        """Standard function to save report to the location"""
        if data is None:
            return
        if finyear is None:
            report_filename = f"{self.slug}_{self.code}_{report_type}.csv"
        else:
            report_filename = f"{self.slug}_{self.code}_{report_type}_{finyear}.csv"
        filepath = self.get_file_path(logger)
        filepath = filepath.replace("reportType", report_type)
        filename = f"{filepath}/{report_filename}"
        logger.info(f"file name is {filename}")
        create_update_report(logger, self.id, report_type,
                             data, filename, finyear=finyear)
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
        "Will fetch R15.5 suspended payment reprot"
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
        full_finyear = get_full_finyear(get_current_finyear())
        self.panchayat_page_url = (f"http://{self.crawl_ip}/netnrega/IndexFrame.aspx?"
                                   f"lflag=eng&District_Code={self.state_code}&"
                                   f"district_name={self.district_name}"
                                   f"&state_name={self.state_name}"
                                   f"&state_Code={self.state_code}&block_name={self.block_name}"
                                   f"&block_code={self.block_code}&fin_year={full_finyear}"
                                   f"&check=1&Panchayat_name={self.panchayat_name}"
                                   f"&Panchayat_Code={self.panchayat_code}")

    def jobcard_register(self, logger):
        """Will Fetch the Jobcard Register"""
        logger.info(f"Going to fetch Jobcard register for {self.code}")
        report_type = "jobcard_register"
        is_updated = self.is_report_updated(logger, report_type)
        if not is_updated:
            dataframe = get_jobcard_register(self, logger)
            self.save_report(logger, dataframe, report_type)
    def worker_register(self, logger):
        """Will Fetch the Jobcard Register"""
        logger.info(f"Going to fetch Jobcard register for {self.code}")
        report_type = "worker_register"
        is_updated = self.is_report_updated(logger, report_type)
        #if (is_updated) and (not self.force_download):
        if (is_updated):
            return
        dataframe = get_worker_register(self, logger)
        self.save_report(logger, dataframe, report_type)
    def jobcard_transactions(self, logger):
        """This will fetch all jobcard transactions for the panchayat"""
        report_type = "jobcard_transactions"
        is_updated = self.is_report_updated(logger, report_type)
        #if (is_updated) and (not self.force_download):
        if (is_updated):
            return
        logger.info(f"Going to fetch Jobcard Transactions for {self.code}")
        self.jobcard_register(logger)
        self.worker_register(logger)
        report_type = "jobcard_register"
        jobcard_register_df = self.fetch_report_dataframe(logger, report_type)
        dataframe = get_jobcard_transactions(self, logger, jobcard_register_df)
        report_type = "jobcard_transactions"
        self.save_report(logger, dataframe, report_type)
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
        dataframe = get_muster_list(self, logger, jobcard_transaction_df)
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
        report_type = "muster_list"
        muster_list_df = self.fetch_report_dataframe(logger, report_type)
        report_type = "muster_transactions"
        muster_transactions_df = self.fetch_report_dataframe(logger, report_type)
        dataframe = get_muster_transactions(self, logger, muster_list_df,
                                            muster_transactions_df)
        report_type = "muster_transactions"
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
        if is_updated:
            return
        logger.info(f"District code is {self.district_code}")
        my_location = NREGADistrict(logger, self.district_code,
                                    force_download=False)
        my_location.nic_stat_urls(logger)
        report_type = "nic_stat_urls"
        nic_stat_urls_df = my_location.fetch_report_dataframe(logger, report_type)
        dataframe = get_nic_stats(self, logger, nic_stat_urls_df)
        report_type = "nic_stats"
        self.save_report(logger, dataframe, report_type)


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
    def get_all_panchayat_ids(self, logger):
        """Getting all child Locations, in this case getting all panchayat
        locations IDs"""
        panchayat_array = api_get_child_location_ids(logger, self.code,
                                                     scheme='nrega')
        return panchayat_array
    def ap_suspended_payments_r14_5(self, logger):
        "Will fetch R15.5 suspended payment reprot"

        panchayat_array = self.get_all_panchayats(logger)
        logger.info(panchayat_array)
        df_array = []
        for each_panchayat_code in panchayat_array:
            logger.info(f"Currently Processing panchayat code {each_panchayat_code}")
            my_location = APPanchayat(logger, each_panchayat_code)
            dataframe = get_ap_suspended_payments_r14_5(my_location, logger)
            if dataframe is not None:
                df_array.append(dataframe)
        dataframe = pd.concat(df_array)
        report_type = "ap_suspended_payments_r14_5"
        if dataframe is not None:
            self.save_report(logger, dataframe, report_type)

class NREGADistrict(Location):
    """This is the District class for NREGA"""
    def __init__(self, logger, location_code, force_download=False, sample_name="on_demand"):
        self.scheme = 'nrega'
        self.force_download = force_download
        self.code = location_code
        self.sample_name = sample_name
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
        logger.info(f"force download is {self.force_download}")
        is_updated = self.is_report_updated(logger, report_type)
        logger.info(f"CHecking if nic stat urls is updated{is_updated}")
        if is_updated:
            return
        exit(0)
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
    def nic_stats(self, logger):
        """This function will fetch NIC Stats"""
        report_type = "nic_stats"
        is_updated = self.is_report_updated(logger, report_type)
        if is_updated:
            return
        logger.info(f"District code is {self.district_code}")
        my_location = NREGADistrict(logger, self.district_code,
                                    force_download=False)
        my_location.nic_stat_urls(logger)
        report_type = "nic_stat_urls"
        nic_stat_urls_df = my_location.fetch_report_dataframe(logger, report_type)
        dataframe = get_nic_stats(self, logger, nic_stat_urls_df)
        report_type = "nic_stats"
        self.save_report(logger, dataframe, report_type)
    def block_rejected_transactions(self, logger):
        """This will fetch all the rejected transactions of the block"""
        get_block_rejected_transactions(self, logger)
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

