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

panchayats_with_no_workers = [
    2728002102,
    2728002104,
    2728002105,
    2728002101,
    2728002100,
    2728002106,
    2728002108,
    2728002099,
    2728002103,
    2728002107,
    2728002071,
    2729012381,
    2729012380,
    2729012379,
    2729012395,
    2729012386,
    2729012398,
    2729012397,
    2729012390,
    2729012393,
    2729012382,
    2729012394,
    2729012396,
    2729012391,
    2729012387,
    2729012378,
    2729012388,
    2729012383,
    2729012392,
    2729012377,
    2729012384,
    2729012385,
    2729012389,

]
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

    def get_expected_finyears(self):
        logger = self.logger
        start_finyear = get_default_start_fin_year()
        end_finyear = get_current_finyear()
        expected_values = []
        for finyear in range(start_finyear, end_finyear+1):
            expected_values.append(finyear)
        if self.finyear is not None:
            expected_values = [finyear]
        logger.debug(expected_values)
        return expected_values

    def test_finyear_unexpected(self, expected_values, column_name):
        logger = self.logger
        data = self.data
        finyear = self.finyear

        logger.info('Running finyear test')

        finyears = data[column_name].unique()
        unexpected = [year for year in finyears if year not in expected_values]

        assert not len(unexpected), f'Found unexpected values for finyear: {unexpected}'
        return True
    
    def test_finyears_exist(self, expected_values, column_name):
        logger = self.logger
        data = self.data
        finyear = self.finyear

        logger.info('Running finyear test')

        finyears = data[column_name].unique()
        missing = [year for year in expected_values if year not in finyears]

        # We don't want halt the validator
        # assert not len(missing), f'Missing finyear are: {missing}'
        if len(missing) != 0:
            self.health = 'yellow'
            self.remark = f'Missing finyear are: {missing}'
            return False
        return True

    def test_empty_df(self):
         logger = self.logger
         data = self.data
         print("I am in test empty df")
         if data is None:
             assert False, f"Empty Dataframe"
             return True
         assert data.shape[0] != 0,  f'It is a empty dataframe with {data.shape}'
         return True
     
    def test_columns_exist(self, columns):
        data = self.data
        column_list = data.columns.tolist()

        missing = []
        for column in columns:
            if column not in column_list:
                missing.append(column)
        assert len(missing) == 0, f'{missing} are the column(s) missing in the data'
        return True

    def test_child_location(self):
        logger = self.logger
        data = self.data 
        lobj = self.lobj  
        
        location_type =  lobj.location_type
        location_list = ['state','district','block','panchayat'] 
        location_dict = {'state': ['state_code', 'state_name'],
                 'district': ['district_code','district_name'],
                 'block': ['block_code','block_name'],
                 'panchayat': ['panchayat_code','panchayat_name']}
        try:
            get_location = location_list[location_list.index(location_type)+1]
            location_nan = location_dict.get(get_location)
        except IndexError:
            print('This is the lowest level') 

        #if get_location == 'district':
         #   ref_df = self.lobj.get_all_districts(logger)
        #elif get_location == 'block':
         #   ref_df = self.lobj.get_all_blocks(logger)
        #elif get_location == 'panchayat':
           # ref_df = self.lobj.get_all_panchayats(logger)   

        ref_df = self.lobj.get_all_panchayats(logger)              
        display_name = [self.lobj.display_name] * len(ref_df)           
        panchayat_dict = dict(zip(pd.Series(ref_df), display_name))         
        uni_df = data[location_nan[0]].unique().tolist()
        uni_df = [str(i) for i in uni_df]
        missing_loc = [(panchayat, panchayat_dict[panchayat]) for panchayat in ref_df if panchayat not in uni_df]
        assert len(missing_loc) == 0,  f'these are the missing {location_nan[0]} and ids {missing_loc}'
        return True


    def test_location(self):
        logger = self.logger
        dataframe = self.data
        lobj = self.lobj
        location_column_name = f"{lobj.location_type}_code"
        dataframe = dataframe.astype({location_column_name : int})
        unique_locations = dataframe[location_column_name].unique().tolist()
        if int(lobj.code) not in unique_locations:
            assert False, "location code not found"
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
            if int(location_code) in panchayats_with_no_workers:
                continue
            if int(location_code) not in unique_child_locations:
                absent_locations.append(location_code)
        if len(absent_locations) > 0:
            message = f"following locations are not present {absent_locations}"
            assert False, message
        return True

    def run_validators(self, columns, finyear_column):
        pass

class NicBlockUrlsValidator(ReportValidator):
    def __init__(self, lobj, logger, data, report_type, finyear=None):
        super().__init__(lobj, logger, data, report_type, finyear)
    def validate_report(self):
        logger = self.logger
        self.test_empty_df()
        columns = [
            'state_code', 'district_code', 'block_code', 'panchayat_code']
        self.test_columns_exist(columns)
        self.test_empty_values(columns)
        #self.test_child_locations()
        return True, self.health, self.remarks


class WorkerRegisterValidator(ReportValidator):
    def __init__(self, lobj, logger, data, report_type, finyear=None):
        super().__init__(lobj, logger, data, report_type, finyear)
    def validate_report(self):
        if (self.lobj.location_type == "panchayat"):
            if int(self.lobj.code) in panchayats_with_no_workers:
                return False, self.health, self.remarks
            self.test_empty_df()
            self.test_location()
            return True, self.health, self.remarks
        self.test_empty_df()
        logger = self.logger
        self.test_child_locations()
        return True, self.health, self.remarks


class BlockRejectedTransactionsV2Validator(ReportValidator):
    def __init__(self, lobj, logger, data, report_type, finyear=None):
        super().__init__(lobj, logger, data, report_type, finyear)

    def validate_report(self):
        logger = self.logger
        logger.debug(f"location type {self.lobj.location_type}")
        logger.info(f'{self.lobj.state_name}-{self.lobj.district_name}-{self.lobj.block_name} {self.lobj.display_name}') 
        logger.info(f'{self.lobj.location_type}')
        logger.debug(f"is NIC {self.lobj.is_nic}")
        logger.debug(f"panchayats for blocks {self.lobj.get_all_panchayats(logger)}")
        logger.debug('Validating Report')

        expected_finyears = self.get_expected_finyears()
        columns = [
            'state_code', 'district_code', 'block_code', 'panchayat_code', 'fto_no',
            'final_status', 'fto_amount', 'fto_amount', 'fto_fin_year', 'final_rejection_reason'
        ]
        #expected_values = [19,-2,22,21]
      # self.test_finyear_unexpected(expected_finyears, 'fto_fin_year')
      # self.test_empty_values(columns)
      # # self.test_child_location()  # FIXME
      # #expected_values = [19, 20, 21]
      # self.test_finyear_unexpected(expected_finyears, 'fto_fin_year')
      # self.test_empty_df()
      # if not self.test_finyears_exist(expected_finyears, 'fto_fin_year'):
      #     return False, self.health, self.remark
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
        self.test_finyear_unexpected(expected_values, 'fto_fin_year')
        
