"""This has different classes instantiated for Ryatu Barosa Crawling
"""
#pylint: disable-msg = no-member
#pylint: disable-msg = too-few-public-methods
import pandas as pd
import json
from libtech_lib.lib.aws import (
    get_aws_parquet,
    get_aws_file_url
)
from libtech_lib.lib.api_interface import (
    get_location_dict,
    api_get_child_locations
)
import os 
dir_path = os.path.dirname(os.path.realpath(__file__))

class RBLocation():
    """This is the base Location Class"""
    def __init__(self, logger, location_code, scheme='rayatubarosa'):
        self.code = location_code
        self.scheme = scheme
        location_dict = get_location_dict(logger, self.code, scheme=self.scheme)
        for key, value in location_dict.items():
            setattr(self, key, value)
    def get_child_locations(self, logger):
        """Will return all the child location codes in an array"""
        location_array = api_get_child_locations(logger, self.code,
                                                  scheme=self.scheme)
        return location_array


class RBBlock(RBLocation):
    """This is the Block subclass for RB Location Class"""
    def __init__(self, logger, location_code):
        self.scheme = 'rayatubarosa'
        self.code = location_code
        RBLocation.__init__(self, logger, self.code, self.scheme)
        
class RBCrawler():
    """This is the Block subclass for RB Location Class"""
    def __init__(self, logger):
        self.scheme = 'rayatubarosa'
    def get_crawl_df(self, logger, block_code = None,
                    tag_name = None):
        """This is to get the crawer df with all the crawl parameters and file
        path parameters"""
        dataframe = None
        csv_array = []
        col_headers = ["district_name_telugu", "block_name_telugu",
                       "village_name", "district_code",
                       "block_code", "village_code"]
        if block_code is not None:
            rb_block = RBBlock(logger, block_code)
            village_code_array = rb_block.get_child_locations(logger)
            for village_code in village_code_array:
                village_loc = RBLocation(logger, village_code)
                row = [village_loc.district_name,
                       village_loc.block_name,
                       village_loc.name,
                       village_loc.district_code,
                       village_loc.block_code,
                       village_loc.code
                      ]
                csv_array.append(row)
            dataframe = pd.DataFrame(csv_array, columns=col_headers)
        return dataframe

class RBLocationInit():
    """This class has all the functions for the intial crawl of Locations"""
    def __init__(self):
        self.census_parquet_village_filename = (f"data/locations/ap_census/"
                                                f"all_ap_villages/"
                                                f"part-00000-9270a97e-3293-45b6-b1fd-2fed8304fc12-c000.snappy.parquet")
        self.district_block_raw = 'data/locations/rayatu_barosa/rayatu_barosa_district_block_raw.csv'
    def merge_census_data(self, logger):
        """This will merge the raw district block data with census data"""
        census_df = get_aws_parquet(self.census_parquet_village_filename)
        logger.info(census_df.columns)
        col_list = ['district_code', 'district_name_eng', 'district_name_tel',
                    'mandal_code', 'mandal_name_tel', 'mandal_name_eng']

        census_df = census_df[col_list]
        logger.info(f"shape of census df is {census_df.shape}")
        census_df = census_df.drop_duplicates()
        logger.info(f"shape of census df is {census_df.shape}")
        logger.info(census_df.head())
        logger.info(census_df.columns)
        census_df.to_csv("~/thrash/c.csv")
        raw_df_url = get_aws_file_url(self.district_block_raw)
        COLUMN_CONFIG_FILE = f"{dir_path}/data/scheme_to_census.json"
        logger.info(COLUMN_CONFIG_FILE)
        with open(COLUMN_CONFIG_FILE) as config_file:
            raw_column_rename_dict = json.load(config_file)
        raw_df = pd.read_csv(raw_df_url, index_col=0)
        raw_df['block_name_telugu'] = raw_df['block_name_telugu'].map(raw_column_rename_dict).fillna(raw_df['block_name_telugu'])

        logger.info(raw_df.head())
        logger.info(raw_df.columns)
        block_district_df = raw_df.merge(census_df, how="left",
                                         left_on=["district_name_telugu",
                                                  "block_name_telugu"],
                                         right_on=["district_name_tel",
                                                   "mandal_name_tel"]
                                        )
        block_district_df.to_csv("~/thrash/a.csv")

