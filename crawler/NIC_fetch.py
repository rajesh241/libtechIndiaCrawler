import os
CUR_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.dirname(CUR_DIR)
REPO_DIR = os.path.dirname(ROOT_DIR)

import sys
sys.path.insert(0, ROOT_DIR)

import errno

#import argparse

from bs4 import BeautifulSoup, NavigableString, Tag

import requests
# import time
import unittest
# import datetime

from libtech_lib.generic.commons import logger_fetch
# from libtech_lib.wrappers.sn import driverInitialize, driverFinalize, displayInitialize, displayFinalize
#from libtech_lib.generic.aws import get_aws_parquet, upload_s3
#from libtech_lib.generic.html_functions import get_dataframe_from_html
#from libtech_lib.rayatubarosa.models import RBCrawler, RBLocation
#from libtech_lib.generic.api_interface import api_get_tag_id

#import psutil
import pandas as pd
#import json

# For crawler.py

from slugify import slugify
#import csv
#import urllib.parse as urlparse


#######################
# Global Declarations
#######################

timeout = 3

is_mynk = True


#############
# Functions
#############


#############
# Classes
#############

class NIC():
    def __init__(self, logger=None, directory=None):
        if logger:
            self.logger = logger
        else:
            logger = self.logger = logger_fetch('info')
        logger.info(f'Constructor({type(self).__name__})')

        self.dir = '../Data/NIC'
        if directory:
            self.dir = directory
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

        self.base_url = 'http://nregasp2.nic.in/netnrega/Citizen_html/Musternew.aspx'
        self.session = requests.Session()
        response = self.session.get(self.base_url)
        self.cookies = self.session.cookies
        self.view_state = ''
        self.event_validation = ''

    def __del__(self):
        self.session.close()
        self.logger.info(f'Destructor({type(self).__name__})')
    
    def fetch_work_codes(self, url=None):
        logger = self.logger
        if not url:
            url = self.base_url + 'ROR.aspx'
        logger.info(f'fetch_work_codes(url={url})')

        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.142 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        params = (
            ('id', '2'),
            ('lflag', 'eng'),
            ('ExeL', 'GP'),
            ('fin_year', '2020-2021'),
            ('state_code', '34'),
            ('district_code', '3424'),
            ('block_code', '3401020'),
            ('panchayat_code', '3401020002'),
            ('State_name', 'JHARKHAND'),
            ('District_name', 'KHUNTI'),
            ('Block_name', 'TORPA'),
            ('panchayat_name', 'BARKULI'),
            ('Digest', 'Q CEdRMQ8m7Jfvp6sBSfzg'),
        )
        
        filename = f'{self.dir}/base.html'
        logger.info(f'Fetching URL[{url}]')
        try:
            response = self.session.get('http://nregasp2.nic.in/netnrega/Citizen_html/Musternew.aspx',
                                        headers=headers,
                                        params=params,
                                        cookies=self.cookies,
                                        verify=False)
        except Exception as e:
            logger.critical('Exception on get() - EXCEPT[%s:%s]' % (type(e), e))
            return []
                
        html_source = response.text
        logger.debug("HTML Fetched [%s]" % html_source)
        
        with open(filename, 'w') as html_file:
            logger.info('Writing [%s]' % filename)
            html_file.write(html_source)
            
        soup = BeautifulSoup(html_source, 'lxml')
        self.view_state = soup.find(id="__VIEWSTATE")['value']
        self.event_validation = soup.find(id="__EVENTVALIDATION")['value']

        work_names_html = soup.find(id='ctl00_ContentPlaceHolder1_ddlwork')
        work_codes = []
        for body_child in work_names_html:
            if isinstance(body_child, NavigableString):
                #logger.info(f'NavigableString[{body_child}]')
                continue
            if isinstance(body_child, Tag):
                work_code = body_child['value']
                logger.info(work_code)
                if('---select---' in work_code):
                    logger.warning(f'Skipping muster_no[{work_code}]')
                    continue
                work_codes.append(work_code)
                break
            #logger.info(work_name_html)
        return work_codes

    def fetch_muster_numbers(self, work_code):
        logger = self.logger
        logger.info(f'fetch_work_codes(work_code={work_code})')

        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'X-MicrosoftAjax': 'Delta=true',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.142 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': '*/*',
            'Origin': 'http://nregasp2.nic.in',
            'Referer': 'http://nregasp2.nic.in/netnrega/Citizen_html/Musternew.aspx?id=2&lflag=eng&ExeL=GP&fin_year=2020-2021&state_code=34&district_code=3424&block_code=3401020&panchayat_code=3401020002&State_name=JHARKHAND&District_name=KHUNTI&Block_name=TORPA&panchayat_name=BARKULI&Digest=Q+CEdRMQ8m7Jfvp6sBSfzg',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        params = (
            ('id', '2'),
            ('lflag', 'eng'),
            ('ExeL', 'GP'),
            ('fin_year', '2020-2021'),
            ('state_code', '34'),
            ('district_code', '3424'),
            ('block_code', '3401020'),
            ('panchayat_code', '3401020002'),
            ('State_name', 'JHARKHAND'),
            ('District_name', 'KHUNTI'),
            ('Block_name', 'TORPA'),
            ('panchayat_name', 'BARKULI'),
            ('Digest', 'Q CEdRMQ8m7Jfvp6sBSfzg'),
        )

        data = {
          'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$ScriptManager1|ctl00$ContentPlaceHolder1$ddlwork',
          '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlwork',
          '__EVENTARGUMENT': '',
          '__LASTFOCUS': '',
          '__VIEWSTATE': self.view_state,
          '__EVENTVALIDATION': self.event_validation,
          'ctl00$ContentPlaceHolder1$ddlFinYear': '2020-2021',
          'ctl00$ContentPlaceHolder1$btnfill': 'btnfill',
          'ctl00$ContentPlaceHolder1$txtSearch': '',
          'ctl00$ContentPlaceHolder1$ddlwork': work_code,
          'ctl00$ContentPlaceHolder1$ddlMsrno': '---select---',
          '__ASYNCPOST': 'true',
          '': ''
        }

        response = self.session.post('http://nregasp2.nic.in/netnrega/Citizen_html/Musternew.aspx', headers=headers, params=params, cookies=self.cookies, data=data, verify=False)
        logger.debug(response)
        filename = self.dir + slugify(f'{work_code}.html')
        with open(filename, 'wb') as output:
            print(f'Writing file[{filename}]')
            output.write(response.content)

        soup = BeautifulSoup(response.text, 'lxml')
        logger.debug(soup)
        muster_nos_dd = soup.find(id='ctl00_ContentPlaceHolder1_ddlMsrno')
        logger.info(muster_nos_dd)
        muster_nos = []
        for body_child in muster_nos_dd:
            if isinstance(body_child, NavigableString):
                continue
            if isinstance(body_child, Tag):
                muster_no = body_child['value']
                logger.info(muster_no)
                if('---Select---' in muster_no):
                    logger.warning(f'Skipping muster_no[{muster_no}]')
                    continue
                muster_nos.append(muster_no)
                break


        return muster_nos

    def fetch_muster_roll(self, work_code, muster_no):
        logger = self.logger
        logger.info(f'fetch_muster_roll(work_code={work_code}, muster_no={muster_no})')
        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'X-MicrosoftAjax': 'Delta=true',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.142 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': '*/*',
            'Origin': 'http://nregasp2.nic.in',
            'Referer': 'http://nregasp2.nic.in/netnrega/Citizen_html/Musternew.aspx?id=2&lflag=eng&ExeL=GP&fin_year=2020-2021&state_code=34&district_code=3424&block_code=3401020&panchayat_code=3401020002&State_name=JHARKHAND&District_name=KHUNTI&Block_name=TORPA&panchayat_name=BARKULI&Digest=Q+CEdRMQ8m7Jfvp6sBSfzg',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        params = (
            ('id', '2'),
            ('lflag', 'eng'),
            ('ExeL', 'GP'),
            ('fin_year', '2020-2021'),
            ('state_code', '34'),
            ('district_code', '3424'),
            ('block_code', '3401020'),
            ('panchayat_code', '3401020002'),
            ('State_name', 'JHARKHAND'),
            ('District_name', 'KHUNTI'),
            ('Block_name', 'TORPA'),
            ('panchayat_name', 'BARKULI'),
            ('Digest', 'Q CEdRMQ8m7Jfvp6sBSfzg'),
        )

        data = {
          'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel2|ctl00$ContentPlaceHolder1$ddlMsrno',
          'ctl00$ContentPlaceHolder1$ddlFinYear': '2020-2021',
          'ctl00$ContentPlaceHolder1$btnfill': 'btnfill',
          'ctl00$ContentPlaceHolder1$txtSearch': '',
          'ctl00$ContentPlaceHolder1$ddlwork': work_code,
          'ctl00$ContentPlaceHolder1$ddlMsrno': muster_no,
          '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlMsrno',
          '__EVENTARGUMENT': '',
          '__LASTFOCUS': '',
          '__VIEWSTATE': self.view_state,
          '__EVENTVALIDATION': self.event_validation,
          '__ASYNCPOST': 'true',
          '': ''
        }

        response = self.session.post('http://nregasp2.nic.in/netnrega/Citizen_html/Musternew.aspx', headers=headers, params=params, cookies=self.cookies, data=data, verify=False)

        filename = self.dir + slugify(f'{work_code}_{muster_no}.html')
        with open(filename, 'wb') as output:
            print(f'Writing file[{filename}]')
            output.write(response.content)

        return response.content

class TestSuite(unittest.TestCase):
    def setUp(self):
        self.logger = logger_fetch('info')
        self.logger.info('BEGIN PROCESSING...')

    def tearDown(self):
        self.logger.info('...END PROCESSING')

    def test_fetch_work_codes(self):
        self.logger.info('TestCase: fetch_work_codes()')
        nic = NIC(logger=self.logger)
        work_codes = nic.fetch_work_codes()
        if len(work_codes) != 0:
            result = 'SUCCESS'
        self.assertEqual(result, 'SUCCESS')
        del nic

    def test_fetch_muster_numbers(self):
        self.logger.info('TestCase: fetch_work_codes()')
        nic = NIC(logger=self.logger)
        work_codes = nic.fetch_work_codes()
        for work_code in work_codes:
            muster_numbers = nic.fetch_muster_numbers(work_codes)
            break
        if len(muster_numbers) != 0:
            result = 'SUCCESS'
        self.assertEqual(result, 'SUCCESS')
        del nic

    def test_fetch_muster_roll(self):
        self.logger.info('TestCase: fetch_work_codes()')
        nic = NIC(logger=self.logger)
        work_codes = nic.fetch_work_codes()
        for work_code in work_codes:
            muster_numbers = nic.fetch_muster_numbers(work_codes)
            for muster_number in muster_numbers:
                html = nic.fetch_muster_roll(work_code, muster_number)

        print(html)
        if len(muster_numbers) != 0:
            result = 'SUCCESS'
        self.assertEqual(result, 'SUCCESS')
        del nic

if __name__ == '__main__':
    unittest.main()
