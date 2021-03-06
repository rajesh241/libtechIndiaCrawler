import datetime
import os
import shutil
from slugify import slugify
from libtech_lib.nrega.models import Location
from libtech_lib.nrega import models
from libtech_lib.generic.api_interface import (create_task,
                                              api_get_locations_by_params,
                                              api_create_bundle
                                              )
from libtech_lib.generic.commons import download_save_file
from libtech_lib.generic.aws import upload_s3
class LibtechSample():
    """This is the base Location Class"""
    def __init__(self, logger, tag_name=None, parent_location_code=None, sample_type="block", force_download='false',
                 scheme='nrega', name="on_demand", is_nic=True):
        self.parent_location_code = parent_location_code
        self.sample_type = sample_type
        self.name = name
        self.scheme = scheme
        self.is_nic = is_nic
        self.tag_name = tag_name
        self.force_download = force_download
        self.location_class = self.get_location_class(logger)
        self.get_sample_locations(logger) 
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
        if self.sample_type == "state":
            if not self.is_nic:
                location_class = "NREGAState"
            else:
                location_class = "NREGAState"
        return location_class
    def populate_queue(self, logger, report_type, finyear=None, priority=None):
        """This function will populate the Queue"""
        logger.info("Populating queue")
        for each_code in self.sample_location_codes:
            logger.info(each_code)
            data = {
                'report_type' : report_type,
            }
            data['location_code'] = each_code
            data['location_class'] = self.location_class
            data['force_download'] = self.force_download
            if priority is not None:
                data['priority'] = priority
            logger.info(data)
            create_task(logger, data)
    def get_all_locations(self, logger):
        self.get_sample_locations(logger)
    def get_sample_locations(self, logger):
        """This function will populate the Queue"""
        if self.tag_name is not None:
            params = { "libtech_tag__name" : self.tag_name,
                       "location_type" : self.sample_type,
                       "scheme" : "nrega",
                       "limit" : 10000
                     }
            self.sample_location_codes = api_get_locations_by_params(logger, params)
        else:
            sample_location_codes = [self.parent_location_code]
            child_location_codes = [self.parent_location_code]
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
            self.sample_location_codes = child_location_codes
    def create_bundle(self, logger, report_types, filename=None,
                      report_format="both", title=None):
        today_str_date = datetime.datetime.today().strftime('%d%B%Y')
        if title is None:
            if self.tag_name is not None:
                title = f"{self.tag_name}_{today_str_date}"
            else:
                title = f"{self.parent_location_code}_{today_str_date}"
        if filename is None:
            filename = slugify(title)
        data = {
                'title' : title,
                'location_type' : self.sample_type,
                'report_types' : report_types,
                'filename' : filename,
                'report_format' : report_format
        }
        if self.tag_name is not None:
            data["libtech_tags"] = self.tag_name
        else:
            data["location_code"] = self.parent_location_code

        url = api_create_bundle(logger, data=data)
        return url
         
        
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
        self.get_all_locations(logger)
    def get_all_locations(self, logger):
        """Getting all the panchayat codes"""
        sample_location_codes = self.sample_location_codes
        for each_code in self.sample_location_codes:
            lobj = Location(logger, location_code=each_code)
            location_array = lobj.get_child_locations(logger) 
            sample_location_codes = sample_location_codes + location_array
        self.all_location_codes = sample_location_codes

class APNatureBlockSample(LibtechSample):
    def __init__(self, logger, force_download='false',
                 name="on_demand"):
        self.parent_location_code = None
        self.sample_type = "block"
        self.name = name
        self.scheme = "nrega"
        self.is_nic = False
        self.force_download = force_download
        self.location_class = self.get_location_class(logger)
        self.sample_location_codes = ['0203004','0203005','0203006']
        self.get_all_locations(logger)
    def get_all_locations(self, logger):
        """Getting all the panchayat codes"""
        sample_location_codes = self.sample_location_codes
        for each_code in self.sample_location_codes:
            lobj = Location(logger, location_code=each_code)
            location_array = lobj.get_child_locations(logger) 
            sample_location_codes = sample_location_codes + location_array
        self.all_location_codes = sample_location_codes


class FESAPSample(LibtechSample):
    def __init__(self, logger, force_download='false',
             name="on_demand"):
       self.parent_location_code = None
       self.sample_type = "block"
       self.name = name
       self.scheme = "nrega"
       self.is_nic = False
       self.force_download = force_download
       self.location_class = self.get_location_class(logger)
       self.sample_location_codes = ['0210002', # Andhra Pradesh-Chittoor-Thamballapalle
                                     '0210001', # Andhra Pradesh-Chittoor-Peddamandyam
                                     '0212040' # Andhra Pradesh-Anantapur-Nambulipulikunta
                                    ]
    def get_all_locations(self, logger):
        """This function will populate the Queue"""
        return 
    def get_sample_locations(self, logger):
        return

class FESNICSample(LibtechSample):
    def __init__(self, logger, force_download='false',
                 name="on_demand"):
        self.parent_location_code = None
        self.sample_type = "block"
        self.name = name
        self.scheme = "nrega"
        self.is_nic = True
        self.force_download = force_download
        self.location_class = self.get_location_class(logger)
        self.sample_location_codes = [
                                      '3403009', # jharkhand-gumla-basia
                                      '3403002', #jharkhand-gumla-ghaghra
                                      '3401006', # jharkhand-khunti-arki
                                      '3402002', # jharkhand-lohardaga-kisko
                                      '2724007', # Rajasthan-Bhilwara-Sahada
                                      '2724006', # Rajasthan-Bhilwara-Raipur
                                      '2724010', # Rajasthan-Bhilwara-Jahajpur
                                      '2724003', # Rajasthan-Bhilwara-Shahpura
                                      '2724009', # Rajasthan-Bhilwara-Kotri
                                      '2724005', # Rajasthan-Bhilwara-Mandal
                                      '2724001', # Rajasthan-Bhilwara-Asind
                                      '2724011', # Rajasthan-Bhilwara-Mandalagarh
                                      '2726003', # Rajasthan-Udaipur-Gogunda
                                      '2729012', # Rajasthan-Pratapgarh-Pratapgarh
                                      '2728002', # Rajasthan-Pratapgarh-Peepalakhunt
                                      '2729003', # Rajasthan-Chittorgarh-Begun
                                      '2725002', # Rajasthan-Rajsamand-Devgarh
                                      '2411003', # Odisha-Koraput-Pottangi
                                      '2411002', # Odisha-Koraput-Semiliguda
                                      '2421003', # Odisha-Angul-Athmallik
                                      '2407005', # Odisha-Dhenkanal-Kankada Had
                                      '2403007', # Odisha-Kendujhar-Bansapal
                                      '1114006', # Gujarat-Mahisagar-Santrampur
                                      '1114013', # Gujarat-Mahisagar-Kadana
                                      '1528001', # Karnataka-Chikkaballapura-Bagepalli
                                      '1528006', # Karnataka-Chikkaballapura-Sidlaghatta
                                      '1825008' # Maharashtra-Yavatmal-Ghatanji
                                     ]
    def get_all_locations(self, logger):
        """This function will populate the Queue"""
        return 
    def get_sample_locations(self, logger):
        return

class FESSample(LibtechSample):
    def __init__(self, logger, force_download='false',
                 name="on_demand"):
        self.parent_location_code = None
        self.sample_type = "block"
        self.name = name
        self.scheme = "nrega"
        self.is_nic = True
        self.force_download = force_download
        self.location_class = self.get_location_class(logger)
        self.sample_location_codes = ['0210002', # Andhra Pradesh-Chittoor-Thamballapalle
                                     '0210001', # Andhra Pradesh-Chittoor-Peddamandyam
                                     '0212040', # Andhra Pradesh-Anantapur-Nambulipulikunta
                                      '2724007', # Rajasthan-Bhilwara-Sahada
                                      '2724006', # Rajasthan-Bhilwara-Raipur
                                      '2724010', # Rajasthan-Bhilwara-Jahajpur
                                      '2724003', # Rajasthan-Bhilwara-Shahpura
                                      '2724009', # Rajasthan-Bhilwara-Kotri
                                      '2724005', # Rajasthan-Bhilwara-Mandal
                                      '2724001', # Rajasthan-Bhilwara-Asind
                                      '2724011', # Rajasthan-Bhilwara-Mandalagarh
                                      '2726003', # Rajasthan-Udaipur-Gogunda
                                      '2729012', # Rajasthan-Pratapgarh-Pratapgarh
                                      '2728002', # Rajasthan-Pratapgarh-Peepalakhunt
                                      '2729003', # Rajasthan-Chittorgarh-Begun
                                      '2725002', # Rajasthan-Rajsamand-Devgarh
                                      '2411003', # Odisha-Koraput-Pottangi
                                      '2411002', # Odisha-Koraput-Semiliguda
                                      '2421003', # Odisha-Angul-Athmallik
                                      '2407005', # Odisha-Dhenkanal-Kankada Had
                                      '2403007', # Odisha-Kendujhar-Bansapal
                                      '1114006', # Gujarat-Mahisagar-Santrampur
                                      '1114013', # Gujarat-Mahisagar-Kadana
                                      '1528001', # Karnataka-Chikkaballapura-Bagepalli
                                      '1528006', # Karnataka-Chikkaballapura-Sidlaghatta
                                      '1825008' # Maharashtra-Yavatmal-Ghatanji
                                     ]
        self.get_all_locations(logger)
    def get_all_locations(self, logger):
        """Getting all the panchayat codes"""
        sample_location_codes = self.sample_location_codes
        for each_code in self.sample_location_codes:
            lobj = Location(logger, location_code=each_code)
            location_array = lobj.get_child_locations(logger) 
            sample_location_codes = sample_location_codes + location_array
        self.all_location_codes = sample_location_codes
            
        return
    def get_sample_locations(self, logger):
        return

class MKSS_RAJASTHAN_REJ1(LibtechSample):
    def __init__(self, logger, force_download='false',
                 name="on_demand"):
        self.parent_location_code = None
        self.sample_type = "block"
        self.name = name
        self.scheme = "nrega"
        self.is_nic = True
        self.force_download = force_download
        self.location_class = self.get_location_class(logger)
        self.sample_location_codes = [
                                   '2725001',# Bhim, Rajsamand
                                   '2725002',# Devgarh, Rajsamad
                                   '2721003',# Jawaja, Ajmer
                                   '2720002',# Raipur, Pali
                                   '2724005',# Mandal, Bhilwada
                                   '2724001',#Asind, Bhilwada
                                   '2721005' #Masuda, Ajmer
                                     ]
        self.get_all_locations(logger)
    def get_all_locations(self, logger):
        """Getting all the panchayat codes"""
        sample_location_codes = self.sample_location_codes
        for each_code in self.sample_location_codes:
            lobj = Location(logger, location_code=each_code)
            location_array = lobj.get_child_locations(logger) 
            sample_location_codes = sample_location_codes + location_array
        self.all_location_codes = sample_location_codes
            
        return 
    def get_sample_locations(self, logger):
        return
