from bs4 import BeautifulSoup
from PIL import Image
from subprocess import check_output

import os
CUR_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.dirname(CUR_DIR)
REPO_DIR = os.path.dirname(ROOT_DIR)

import sys
sys.path.insert(0, ROOT_DIR)

import errno
import pytesseract
import cv2

import argparse
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, SessionNotCreatedException, TimeoutException, WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys

import requests
import time
import unittest
import datetime
from libtech_lib.generic.commons import logger_fetch
from libtech_lib.wrappers.sn import driverInitialize, driverFinalize, displayInitialize, displayFinalize

class TestSuite(unittest.TestCase):
    def setUp(self):
        self.logger = logger_fetch('info')
        self.logger.info('BEGIN PROCESSING...')
        self.display = displayInitialize(isDisabled = True, isVisible = True)
        self.driver = driverInitialize(timeout=3)
        self.baseURL = "https://fadeb121.ngrok.io"
        self.listURL = f"{self.baseURL}/apartments"
    def tearDown(self):
        driverFinalize(self.driver) 
        displayFinalize(self.display)
        self.logger.info('...END PROCESSING')
    def login(self, username, password):
        """This function is the login in to the system"""
        self.driver.get(self.baseURL)
        elem = self.driver.find_element_by_xpath('//input[@type="text"]')
        self.logger.info('Entering User[%s]' % username)
        elem.send_keys(username)
        elem = self.driver.find_element_by_xpath('//input[@type="password"]')
        self.logger.info('Entering Password[%s]' % password)
        elem.send_keys(password)
        button_xpath = "/html/body/app-root/div/app-login/div/div[1]/div/button"
        elem = self.driver.find_element_by_xpath(button_xpath)
        elem.click()
        time.sleep(3)
    def logout(self):
        logout_xpath = "/html/body/app-root/nav/div/div[2]/ul[2]/li[3]/a"
        elem = self.driver.find_element_by_xpath(logout_xpath)
        elem.click()
    def test_realtor_create_apartment(self):
        """This will test create apartment functionality"""
        username = 'relator1@gmail.com'
        password = 'worldpeace'
        self.login(username, password)
        self.logger.info("Testing realtor  Login")
        create_xpath = "/html/body/app-root/nav/div/div[2]/ul[1]/li[3]/a"
        elem = self.driver.find_element_by_xpath(create_xpath)
        elem.click()
        time.sleep(2)
        name_xpath = '//*[@id="name"]'
        number_or_rooms_xpath = "/html/body/app-root/div/app-apartment-create/div/div/div/form/div/div[1]/div[2]/input"
        description_xpath = '//*[@id="description"]'
        is_available_xpath = '/html/body/app-root/div/app-apartment-create/div/div/div/form/div/div[3]/div/select'
        price_per_month_xpath = '//*[@id="price_per_month"]'
        floor_area_size_xpath = '/html/body/app-root/div/app-apartment-create/div/div/div/form/div/div[4]/div[2]/input'
        button_xpath = '/html/body/app-root/div/app-apartment-create/div/div/div/form/div/div[8]/button'
        elem = self.driver.find_element_by_xpath(name_xpath)
        elem.send_keys("test apartment")
        elem = self.driver.find_element_by_xpath(description_xpath)
        elem.send_keys("lovely view")
        elem = self.driver.find_element_by_xpath(number_or_rooms_xpath)
        elem.send_keys("3")
        elem = self.driver.find_element_by_xpath(price_per_month_xpath)
        elem.send_keys("87456")
        elem = self.driver.find_element_by_xpath(floor_area_size_xpath)
        elem.send_keys("17000")
        elem = self.driver.find_element_by_xpath(is_available_xpath)
        elem.send_keys("yes")
        elem = self.driver.find_element_by_xpath(button_xpath)
        elem.click()
        current_url = self.driver.current_url
        self.logger.info(f"{current_url}")
        self.assertEqual(current_url, self.listURL)
        self.logout()


    def test_client_login_profile_email(self):
        """This will test client login"""
        username = 'client1@gmail.com'
        password = 'worldpeace'
        self.login(username, password)
        self.logger.info("Testing client Login")
        profile_xpath = "/html/body/app-root/nav/div/div[2]/ul[2]/li[1]/a"
        elem = self.driver.find_element_by_xpath(profile_xpath)
        self.logger.info(elem.text)
        elem.click()
        profile_email_xpath = "//h3[2]"
        elem = self.driver.find_element_by_xpath(profile_email_xpath)
        profile_email = elem.text
        self.logger.info(f"Profile email is {profile_email}")
        self.assertEqual(profile_email, username)
        self.logout()
        


if __name__ == '__main__':
    unittest.main()
