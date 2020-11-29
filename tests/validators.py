from glob import glob
import unittest
import requests
import pandas as pd
import sys
import os
from libtech_lib.generic.commons import  (get_current_finyear,
    get_default_start_fin_year,
    get_current_finyear
)

def block_rejected_transactions_v2_validator(lobj, logger, data, report_type, finyear):
    logger.info(f'Running validator: {report_type}_validator({data.shape})')
    obj = RejectedPaymentReportValidator(lobj, logger, data, report_type, finyear)
    result = obj.validate_report()
    del obj
    return result


def dynamic_work_report_r6_18_validator(lobj, logger, data, report_type, finyear):
    logger.info(f'Running validator: {report_type}_validator({data.shape})')
    # Place validating logic here
    # obj = Rejectedpaymentvalidator(logger, data, report_type)
    # obj.validate_report()

    return True


validator_lookup = {
    'block_rejected_transactions_v2': block_rejected_transactions_v2_validator,
    'dynamic_work_report_r6_18': dynamic_work_report_r6_18_validator
}


class ReportValidator():
    def __init__(self, lobj, logger, data, report_type, finyear=None):
        self.logger = logger
        self.data = data
        self.report_type = report_type
        self.finyear = finyear
        self.lobj = lobj
        self.health = "green"
        self.remarks = ""

    def __del__(self):
        pass

    def test_nan(self, column):
        logger = self.logger
        data = self.data
        result = data[column].isnull().values.any()
        assert result != True , f'Found null value in column[{column}]' 
        return result

    def test_empty_values(self, columns):
        logger = self.logger
        data = self.data

        for column in columns:
            if not self.test_nan(column):
                return False

        return True

    def test_finyear(self, expected_values, column_name):
        logger = self.logger
        data = self.data
        finyear = self.finyear

        logger.info('Running finyear test')

        finyears = data[column_name].unique()
        unexpected = [year for year in finyears if year not in expected_values]

        assert not len(
            unexpected), f'Found unexpected values for finyear: {unexpected}'
        return True 
    
    def test_empty_df(self):
         logger = self.logger
         data = self.data
         if data is None:
             assert True, f"Empty Dataframe"
         assert data.shape[0] != 0,  f'It is a empty dataframe with {data.shape}'
         return True

    def test_child_locations(self):
        logger = self.logger
        dataframe = self.data
        lobj = self.lobj
        # We do not check child locations for panchayat as it is lowerst level
        if (lobj.location_type == "panchayat"):
            return True
        child_location_column_name = f"{lobj.child_location_type}_code"
        if child_location_column_name not in dataframe.columns:
            message = f"{child_location_column_name} column does not exists"
            assert False, message
        dataframe = dataframe.astype({child_location_column_name : int})
        unique_child_locations = dataframe[child_location_column_name].unique().tolist()
        unique_child_location = unique_child_locations[0]
        logger.info(f"type of location is {type(unique_child_location)}")
        logger.debug(f"uniquer child locations {unique_child_locations}")
        expected_child_locations = lobj.get_child_locations(logger)
        for i in range(0,len(expected_child_locations)):
            expected_child_locations[i] = int(expected_child_locations[i])
        logger.debug(f"expected child {expected_child_locations}")
        expected_child_location = expected_child_locations[0]
        logger.info(f"type of expectedlocation is {type(expected_child_location)}")
        absent_locations = [];
        for location_code in expected_child_locations:
            if int(location_code) not in unique_child_locations:
                absent_locations.append(location_code)
        if len(absent_locations) > 0:
            message = f"following locations are not present {absent_locations}"
            assert False, message
        return True



         
  
class NicBlockUrlsValidator(ReportValidator):
    def __init__(self, lobj, logger, data, report_type, finyear=None):
        super().__init__(lobj, logger, data, report_type, finyear)
    def validate_report(self):
        logger = self.logger
        self.test_empty_df()
        columns = [
            'state_code', 'district_code', 'block_code', 'panchayat_code']
        self.test_empty_values(columns)
        self.test_child_locations()
        return True, self.health, self.remarks

class WorkerRegisterValidator(ReportValidator):
    def __init__(self, lobj, logger, data, report_type, finyear=None):
        super().__init__(lobj, logger, data, report_type, finyear)
    def validate_report(self):
        logger = self.logger
        self.test_empty_df()
        self.test_child_locations()
        return True, self.health, self.remarks




class RejectedPaymentReportValidator(ReportValidator):
    def __init__(self, lobj, logger, data, report_type, finyear=None):
        super().__init__(lobj, logger, data, report_type, finyear)

    def validate_report(self):
        logger = self.logger
        logger.debug(f"location type {self.lobj.location_type}")
        logger.debug(f"is NIC {self.lobj.is_nic}")
        logger.debug(f"panchayats for blocks {self.lobj.get_all_panchayats(logger)}")
        logger.debug('Validating Report')

        start_finyear = get_default_start_fin_year()
        end_finyear = get_current_finyear()
        expected_values = []
        for finyear in range(start_finyear, end_finyear+1):
            expected_values.append(finyear)
        if self.finyear is not None:
            expected_values = [finyear]
        logger.debug(expected_values)
        columns = [
            'state_code', 'district_code', 'block_code', 'panchayat_code', 'fto_no',
            'final_status', 'fto_amount', 'fto_amount', 'fto_fin_year', 'final_rejection_reason'
        ]
        self.test_empty_values(columns)
        #expected_values = [19, 20, 21]
        self.test_finyear(expected_values, 'fto_fin_year')
        self.test_empty_df()
        message = ''
        health = ''
        return True, health, message


class DynamiceReportValidator(ReportValidator):
    def __init__(self, lobj, logger, data, report_type, finyear=None):
        super().__init__(lobj, logger, data, report_type, finyear)

    def validate_report(self):
        logger = self.logger
        logger.info('Validating Report')

        columns = [
            'state_code', 'district_code', 'block_code', 'panchayat_code', 'fto_no',
            'final_status', 'fto_amount', 'final_rejection_reason', 'fto_amount', 'fto_fin_year'
        ]
        self.test_empty_values(columns)

        expected_values = [19, 20, 21]
        self.test_finyear(expected_values, 'fto_fin_year')
