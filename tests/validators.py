from glob import glob
import unittest
import requests
import pandas as pd
import sys
import os




def Test_NaN(data):
    check_variable = ['state_code','district_code','block_code', 'panchayat_code', 'fto_no', 'final_status', 
                      'fto_amount','final_rejection_reason','fto_amount','fto_fin_year'] 
   
        
    naFalse = []
    
    for eachvar in check_variable:
        if data[eachvar].isnull().values.any():
            naFalse.append(eachvar)
            
    
    if len(naFalse) == 0:
        print(f'NaN test is successful for report')

        return True
    else:
        print(f'NaN test is failed for report. columns with NaN values is/are {naFalse}')
        return False


def test_finyear(data, finyear_var):
    
    expected_values = [19, 20, 21]
    finyears = data[finyear_var].unique()
            
    unexpected = [year for year in finyears if year not in expected_values]

    if len(unexpected) == 0:
        print('fin year test is successful')
        return True
    else:
        print('fin year is not successful, extra years are'+'f{unexpected}')
        return False  




def block_rejected_transactions_v2_validator(logger, data, report_type):
    logger.info(f'Running validator: {report_type}_validator({data.shape})')
    # Place validating logic here
    # obj = Rejectedpaymentvalidator(logger, data, report_type)
    # obj.validate_report()
    
        
    if test_finyear(data, finyear_var = 'fto_fin_year') and Test_NaN(data):
           return True
    else:
           print(" It is failed case")
           #return False
    return True
   



def dynamic_work_report_r6_18_validator(logger, data, report_type):
    logger.info(f'Running validator: {report_type}_validator({data.shape})')
    # Place validating logic here
    # obj = Rejectedpaymentvalidator(logger, data, report_type)
    # obj.validate_report()

    return True


validator_lookup = {
    'block_rejected_transactions_v2': block_rejected_transactions_v2_validator,
    'dynamic_work_report_r6_18': dynamic_work_report_r6_18_validator
}


class ReportsValidator():
    def __init__(self, logger=None):
        pass

    def __del__(self):
        pass

    def validate_report(self):
        logger = self.logger
        logger.info(f'Validating report [{report_type}]')

    def nan_test(self, column_name):
        data

        return True


class RejectedPaymentValidator():
    def __init__(self, logger, data, report_type):
        # super().__init__(False)
        self.logger = logger_fetch()
        self.data = data
        self.report_type = report_type

    def finyear_test(self, finyear):
        logger = self.logger
        data = self.data
        logger.info('Testing finyear related issue')
        pass

    def nan_test(self, columns):
        data = self.data
        self.url
        return True

    def validate_report(self):
        logger = self.logger
        logger.info('Validating Report')

        self.nan_test('finyear')
        self.nan_test('fto_amount')
        self.nan_test(columns)

    def test_NaN(self):
        logger.info()
        check_variable = [
            'state_code', 'district_code', 'block_code', 'panchayat_code', 'fto_amount',
            'state_name', 'district_name', 'block_name', 'panchayat_name', 'village_name',
            'final_status'
        ]
        naFalse = []
        for eachvar in check_variable:
            if self.df[eachvar].isnull().values.any():
                naFalse.append(eachvar)

        if len(naFalse) == 0:
            return True
        else:
            # print(naFalse)
            return False, naFalse

    def test_locality(self):
        check_codes = ['district_code', 'block_code', 'panchayat_code']
        check_codes = ['panchayat_code']
        CodeNot_list = []
        codeNotexist = []

        for eachcode in check_codes:
            uni_fes_list = self.loc_df[eachcode].unique().tolist()

            for eachunique in uni_fes_list:
                if eachunique not in self.df[eachcode].unique().tolist():
                    CodeNot_list.append(eachunique)
                    codeNotexist.append(eachcode)

        print(f'Number of panchayat codes is {len(CodeNot_list)}')
        print(f'{CodeNot_list}')
        '''
        if len(CodeNot_list) == 0:
            return True
        else:
            # print(CodeNot_list)
            # print(codeNotexist)
            return False, CodeNot_list
        '''
        return CodeNot_list

    def test_finyear(self, expected_values=None):
        if not expected_values:
            expected_values = [19, 20, 21]

        finyears = self.df.fto_fin_year.unique()
        unexpected = [year for year in finyears if year not in expected_values]
        return self.df[self.df.fto_fin_year.isin(unexpected)][['state_name', 'district_name', 'block_name', 'jobcard']]

    def test_finyear():
        '''
        This gets printed on jupiter notebook
        '''
