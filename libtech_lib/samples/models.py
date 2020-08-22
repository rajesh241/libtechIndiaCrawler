import datetime
import os
import shutil
from libtech_lib.nrega.models import Location
from libtech_lib.nrega import models
from libtech_lib.generic.api_interface import (create_task
                                              )
from libtech_lib.generic.commons import download_save_file
from libtech_lib.generic.aws import upload_s3
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
    def populate_queue(self, logger, report_type, finyear=None, priority=None):
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
            if priority is not None:
                data['priority'] = priority
            logger.info(data)
            create_task(logger, data)
    def get_all_locations(self, logger):
        self.get_sample_locations(logger)
    def get_sample_locations(self, logger):
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
        sample_location_codes.append(self.parent_location_code)
        self.sample_location_codes = sample_location_codes
        self.all_location_codes = sample_location_codes 
    def create_bundle(self, logger, report_types, download_dir=None, zip_file_name=None, save_to_s3=True, only_csv=False):
        """This would create the zip bundle of all the reports"""
        report_urls = []
        for each_code in self.all_location_codes:
            lobj = Location(logger, location_code=each_code)
            logger.info(f"Currently processing {each_code}")
            for report_type in report_types:
                urls = lobj.fetch_report_urls(lobj, report_type)
                report_urls = report_urls + urls
        logger.info(report_urls)
        if download_dir is None:
            current_timestamp = str(datetime.datetime.now().timestamp())
            download_dir = f"/tmp/{current_timestamp}"
        if zip_file_name is None:
            zip_file_name = f"/tmp/{current_timestamp}"
        for url in report_urls:
            if only_csv == True:
                if url.endswith('.csv'):
                    download_save_file(logger, url, dest_folder=download_dir)
            else:
                download_save_file(logger, url, dest_folder=download_dir)
        shutil.make_archive(zip_file_name, 'zip', download_dir)
        if save_to_s3 == True:
            with open(f"{zip_file_name}.zip", "rb") as f:
                filedata = f.read()
            content_type = 'binary/octet-stream'
            filename = zip_file_name.split('/')[-1].replace(" ", "_")
            filename = f"temp_archives/{filename}.zip"
            file_url = upload_s3(logger, filename, filedata, content_type=content_type)
            return file_url
        else:
            return f"{zip_file_name}.zip"
         
        
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
