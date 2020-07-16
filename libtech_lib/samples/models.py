from libtech_lib.nrega.models import Location
from libtech_lib.nrega import models
from libtech_lib.generic.api_interface import (create_task
                                              )

class LibtechSample():
    """This is the base Location Class"""
    def __init__(self, logger, parent_location_code=None, sample_type="block", force_download='false',
                 scheme='nrega', name="on_demand", is_nic=True):
        self.parent_location_code = parent_location_code
        self.sample_type = sample_type
        self.name = name
        self.scheme = scheme
        self.is_nic = is_nic
        self.force_download = force_download
        self.location_class = self.get_location_class(logger)
    def get_location_class(self, logger):
        """This fuction will return the location class based on
        location_type"""
        location_class = "Location"
        if self.sample_type == "panchayat":
            if not self.is_nic:
                location_class = "APPanchayat"
            else:
                location_class = "NREGAPanchayat"
        if self.sample_type == "block":
            if not self.is_nic:
                location_class = "APBlock"
            else:
                location_class = "NREGABlock"
        if self.sample_type == "district":
            if not self.is_nic:
                location_class = "APDistrict"
            else:
                location_class = "NREGADistrict"
        return location_class
    def populate_queue(self, logger, report_type, finyear=None):
        """This function will populate the Queue"""
        logger.info("Populating queue")
        self.get_all_locations(logger)
        for each_code in self.sample_location_codes:
            logger.info(each_code)
            data = {
                'report_type' : report_type,
            }
            data['location_code'] = each_code
            data['location_class'] = self.location_class
            data['force_download'] = self.force_download
            logger.info(data)
            create_task(logger, data)
    def get_all_locations(self, logger):
        """This function will populate the Queue"""
        sample_location_codes = [self.parent_location_code]
        lobj = Location(logger, location_code=self.parent_location_code)
        current_location_type = lobj.location_type
        while (current_location_type != self.sample_type):
            child_location_codes = []
            for each_code in sample_location_codes:
                lobj = Location(logger, location_code=each_code)
                location_array = lobj.get_child_locations(logger) 
                child_location_codes.extend(location_array)
            sample_location_codes = child_location_codes
            one_sample_code = sample_location_codes[0]
            lobj = Location(logger, location_code=one_sample_code)
            current_location_type = lobj.location_type
        logger.info(f"Total samples selected is {len(sample_location_codes)}")
        self.sample_location_codes = sample_location_codes 
    def create_bundle(self, logger, report_types):
        """This would create the zip bundle of all the reports"""
        report_urls = []
        for each_code in self.sample_location_codes:
            lobj = Location(logger, location_code=each_code)
            for report_type in report_types:
                urls = lobj.fetch_report_urls(lobj, report_types)
                report_urls = report_urls + urls
        logger.info(report_urls)



        
class APITDABlockSample(LibtechSample):
    def __init__(self, logger, force_download='false',
                 name="on_demand"):
        self.parent_location_code = None
        self.sample_type = "block"
        self.name = name
        self.scheme = "nrega"
        self.is_nic = False
        self.force_download = force_download
        self.location_class = self.get_location_class(logger)
        self.sample_location_codes = ['0203006','0203005','0203012','0203004','0203011','0203013','0203003','0203014','0203001','0203010','0203002']
    def get_all_locations(self, logger):
        """This function will populate the Queue"""
        return
