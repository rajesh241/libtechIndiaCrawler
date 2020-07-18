"""This has bunch of helper functions which can beused to initiate crawl"""
from libtech_lib.nrega import models
def get_location_class(logger, location_type, is_nic=True):
     """This function will determine the class for NIC"""
     LOCATION_CLASS = "Location"
     if location_type == "panchayat":
         if is_nic:
             LOCATION_CLASS = "NREGAPanchayat"
         else:
             LOCATION_CLASS = "APPanchayat"
     if location_type == "block":
         if is_nic:
             LOCATION_CLASS = "NREGABlock"
         else:
             LOCATION_CLASS = "APBlock"
     if location_type == "district":
         if is_nic:
             LOCATION_CLASS = "NREGADistrict"
         else:
             LOCATION_CLASS = "APDistrict"
     return LOCATION_CLASS



def download_report(logger, location_code, location_type, report_name,
                    force_download=False, is_nic=True, sample_name='on_demand'):
    """This function will download the Report"""
    location_class = get_location_class(logger, location_type,
                                            is_nic=is_nic)
    my_location = getattr(models, location_class)(logger=logger,
                                                  location_code=location_code,
                                                  force_download=force_download,
                                                  sample_name=sample_name)
    method_to_call = getattr(my_location, report_name)
    method_to_call(logger)
