"""This has different classes instantiated.
there is a class for each location type, and associated methods related to that
"""
#pylint: disable-msg = no-member
#pylint: disable-msg = too-few-public-methods
#File Path for on Demand
#data/samples/on_demand/<scheme>/<report_type>/district/block/panchayat/
##Archive data
AP_STATE_CODE = "02"
from libtech_lib.generic.commons import (get_full_finyear,
                     get_current_finyear
                    )
from libtech_lib.generic.api_interface import (get_location_dict,
                           create_update_report,
                           api_get_report_dataframe,
                           api_get_child_locations,
                           api_get_child_location_ids
                          )
from libtech_lib.nrega.nicnrega import (get_jobcard_register,
                      get_worker_register,
                      get_muster_list,
                      get_jobcard_transactions,
                      get_block_rejected_transactions,
                      get_muster_transactions,
                      get_fto_status_urls,
                      get_block_rejected_stats
                     )
from libtech_lib.nrega.apnrega import (get_ap_jobcard_register,
                                       get_ap_muster_transactions
                    )
from libtech_lib.generic.aws import days_since_modified_s3
REPORT_THRESHOLD_DICT = {
    "jobcard_register" : 15,
    "worker_register" : 15,
}
class Location():
    """This is the base Location Class"""
    def __init__(self, logger, location_code, scheme='nrega'):
        self.code = location_code
        self.scheme = scheme
        location_dict = get_location_dict(logger, self.code, scheme=self.scheme)
        for key, value in location_dict.items():
            setattr(self, key, value)
    def fetch_report_dataframe(self, logger, report_type, finyear=None):
        """Fetches the report dataframe from amazon S3"""
        dataframe = api_get_report_dataframe(logger, self.id, report_type, finyear=finyear)
        return dataframe
    def get_file_path(self, logger):
        filepath = f"data/samples/on_demand/{self.scheme}/reportType"
        if self.location_type == "country":
            return filepath
        filepath = f"data/samples/on_demand/{self.scheme}/reportType/{self.state_code}"
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
        logger.info(f"file name is {filename}")
        return filename
    def is_report_updated(self, logger, report_type, finyear=None):
        """Checks if report is updated"""
        filename = self.get_report_filepath(logger, report_type, finyear=finyear)
        logger.info(f"report name is {filename}")
        days_diff = days_since_modified_s3(logger, filename)
        if days_diff is None:
            return False
        threshold = REPORT_THRESHOLD_DICT.get(report_type, 7)
        if days_diff > threshold:
            return False
        return False

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
    def __init__(self, logger, location_code):
        self.scheme = 'nrega'
        self.code = location_code
        Location.__init__(self, logger, self.code, self.scheme)
        full_finyear = get_full_finyear(get_current_finyear())
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

class NREGAPanchayat(Location):
    """This is the Panchayat subclass for Location Class"""
    def __init__(self, logger, location_code):
        self.scheme = 'nrega'
        self.code = location_code
        Location.__init__(self, logger, self.code, self.scheme)
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
        if not is_updated:
            dataframe = get_worker_register(self, logger)
            self.save_report(logger, dataframe, report_type)
    def jobcard_transactions(self, logger):
        """This will fetch all jobcard transactions for the panchayat"""
        report_type = "jobcard_transactions"
        is_updated = self.is_report_updated(logger, report_type)
        if is_updated:
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
        if is_updated:
            return
        self.jobcard_transactions(logger)
        report_type = "jobcard_transactions"
        jobcard_transaction_df = self.fetch_report_dataframe(logger, report_type)
        dataframe = get_muster_list(self, logger, jobcard_transaction_df)
        report_type = "muster_list"
        self.save_report(logger, dataframe, report_type)
    def muster_transactions(self, logger):
        """This will fetch all muster transactions for the panchayat"""
        logger.info(f"Going to fetch Muster transactions for {self.code}")
        report_type = "muster_transactions"
        is_updated = self.is_report_updated(logger, report_type)
        if is_updated:
            return
        self.muster_list(logger)
        report_type = "muster_list"
        muster_list_df = self.fetch_report_dataframe(logger, report_type)
        dataframe = get_muster_transactions(self, logger, muster_list_df)
        report_type = "muster_transactions"
        self.save_report(logger, dataframe, report_type)

class APBlock(Location):
    """This is the AP Block subclass for Location Class"""
    def __init__(self, logger, location_code):
        self.scheme = 'nrega'
        self.code = location_code
        Location.__init__(self, logger, self.code, self.scheme)
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

class NREGABlock(Location):
    """This is the Panchayat subclass for Location Class"""
    def __init__(self, logger, location_code):
        self.scheme = 'nrega'
        self.code = location_code
        Location.__init__(self, logger, self.code, self.scheme)
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
    def block_rejected_transactions(self, logger):
        """This will fetch all the rejected transactions of the block"""
        get_block_rejected_transactions(self, logger)
