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
         assert data.shape[0] != 0,  f'It is a empty dataframe with {data.shape}'
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
