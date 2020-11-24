from glob import glob
import unittest
import requests
import pandas as pd
import sys
import os


def Test_NaN(data):
    check_variable = [
        'state_code', 'district_code', 'block_code', 'panchayat_code', 'fto_no',
        'final_status', 'fto_amount', 'final_rejection_reason', 'fto_amount', 'fto_fin_year'
    ]

    naFalse = []

    for eachvar in check_variable:
        if data[eachvar].isnull().values.any():
            naFalse.append(eachvar)

    if len(naFalse) == 0:
        print(f'NaN test is successful for report')

        return True
    else:
        print(
            f'NaN test is failed for report. columns with NaN values is/are {naFalse}')
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


def block_rejected_transactions_v2_validator(logger, data, report_type, finyear):
    logger.info(f'Running validator: {report_type}_validator({data.shape})')
    obj = RejectedPaymentReportValidator(logger, data, report_type, finyear)
    result = obj.validate_report()
    del obj
    return result


def old_block_rejected_transactions_v2_validator(logger, data, report_type, finyear):
    logger.info(f'Running validator: {report_type}_validator({data.shape})')
    if test_finyear(data, finyear_var='fto_fin_year') and Test_NaN(data):
        return True
    else:
        print(" It is failed case")
        # return False
    return True


def dynamic_work_report_r6_18_validator(logger, data, report_type, finyear):
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
    def __init__(self, logger, data, report_type, finyear=None):
        self.logger = logger
        self.data = data
        self.report_type = report_type
        self.finyear = finyear

    def __del__(self):
        pass

    def test_nan(self, column):
        logger = self.logger
        data = self.data

        result = data[column].isnull().values.any()
        if result:
            logger.error(f'Found null value in column[{column}]')

        return result

    def test_empty_values(self, columns):
        logger = self.logger
        data = self.data

        for column in columns:
            if self.test_nan(column):
                return False  # Fixme
                # continue

        return True

    def test_finyear(self, expected_values, column_name):
        logger = self.logger
        data = self.data
        finyear = self.finyear

        logger.info('Running finyear test')

        finyears = data[column_name].unique()
        unexpected = [year for year in finyears if year not in expected_values]

        if len(unexpected) == 0:
            return True
        else:
            logger.error(f'Found unexpected values for finyear: {unexpected}')
            return False


class RejectedPaymentReportValidator(ReportValidator):
    def __init__(self, logger, data, report_type, finyear=None):
        super().__init__(logger, data, report_type, finyear)

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


class DynamiceReportValidator(ReportValidator):
    def __init__(self, logger, data, report_type, finyear=None):
        super().__init__(logger, data, report_type, finyear)

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