"""This has different classes instantiated for Ryatu Barosa Crawling
"""
#pylint: disable-msg = no-member
#pylint: disable-msg = too-few-public-methods
import pandas as pd
from aws import (
    get_aws_parquet,
    get_aws_file_url
)
class RayatuBarosaLocationInit():
    """This class has all the functions for the intial crawl of Locations"""
    def __init__(self):
        self.census_parquet_village_filename = (f"data/locations/ap_census/"
                                                f"all_ap_villages/"
                                                f"part-00000-9270a97e-3293-45b6-b1fd-2fed8304fc12-c000.snappy.parquet")
        self.district_block_raw = 'data/locations/rayatu_barosa/rayatu_barosa_district_block_raw.csv'
    def merge_census_data(self, logger):
        """This will merge the raw district block data with census data"""
        census_df = get_aws_parquet(self.census_parquet_village_filename)
        logger.info(census_df.head())
        logger.info(census.columns)
        raw_df_url = get_aws_file_url(sele.district_block_raw)
        raw_df = pd.read_csv(raw_df_url)
        logger.info(raw_df.head())
        logger.info(raw_df.columns)

