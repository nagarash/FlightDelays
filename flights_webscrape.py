
"""
class and methods to webscrape historical flights data in the US airspace network
from https://www.transtats.bts.gov using Selenium.
"""


import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
import time


class flights_webscrape:

    def __init__(self,url,years):
        self.url  = url
        self.years = years
        
    def get_chrome_driver(self,driver_path):
        """
        create a chrome driver object for Selenium from the specified driver file
        """
        driver = webdriver.Chrome(driver_path)
        return driver
    
    def create_param_list(self):
        """
        create list of parameters to pull from the url 
        """
        # Time Period Parameters
        Tparam = ['Year','Month','DayofMonth','DayOfWeek','FlightDate']
        # Airline Parameters
        Aparam = ['Reporting_Airline','Tail_Number','Flight_Number_Reporting_Airline']
        # Origin Parameters
        Oparam = ['OriginAirportID','Origin','OriginCityName','OriginStateName','OriginWac']
        # Destination Parameters
        Dparam = ['DestAirportID','Dest','DestCityName','DestStateName','DestWac']
        # Departure Performance
        DPfparam = ['CRSDepTime','DepTime','DepDelay','DepartureDelayGroups','TaxiOut','WheelsOff']
        # Arrival Performance
        APfparam = ['CRSArrTime','ArrTime','ArrDelay','ArrivalDelayGroups','TaxiIn','WheelsOn']
        # Cancellations
        Cparam = ['Cancelled','CancellationCode','Diverted']
        # Flight Summary
        Fparam = ['AirTime','Flights','Distance']
        # Cause of Delay
        CDparam = ['CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay']
        # Gate Return Delay
        Gparam = ['FirstDepTime','TotalAddGTime','LongestAddGTime']
        # Diverted Landings
        Dvparam = ['DivAirportLandings']
        
        param_list = Tparam + Aparam + Oparam + Dparam + DPfparam + APfparam + Cparam + Fparam + CDparam + Gparam + Dvparam
        return param_list
    
    def download_datasets(self,driver,param_list):
        """
        Select the list of parameters and year on the url and download the flights data for given year
        """
        ## go to specified web url
        driver.get(self.url)

        ## select all the parameters of interest on the css selector
        for param in param_list:
            schk = driver.find_element_by_css_selector('input[Title='+param+']')
            webdriver.ActionChains(driver).move_to_element(schk).click(schk).perform()
        
        ## scroll up to top of window
        driver.execute_script("window.scrollTo(0, 0)")

        ## Pull Data by Year
        for year in self.years:
            for month in np.arange(1,13):
                selectY = Select(driver.find_element_by_id('XYEAR'))
                selectY.select_by_value(year)
                selectM = Select(driver.find_element_by_id('FREQUENCY'))
                selectM.select_by_value(str(month))
                ## click the download button (files go to browser downloads folder)
                sbtn = driver.find_element_by_css_selector('button[onclick="tryDownload()"]')
                sbtn.click()
                time.sleep(45)
        
        ## close the driver once download is complete
        driver.close()

    def write_parquet(self,read_path,write_path):
        """
        convert csvs in zip files to parquet for faster spark queries
        """
        import pandas as pd
        import os
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        filelist = [f for f in os.listdir(read_path) if 'ONTIME_REPORTING' in f]
        
        for file in filelist:
            temp = pd.read_csv(read_path + file,compression='zip')
            table = pa.Table.from_pandas(temp)
            # Local dataset write
            pq.write_to_dataset(table, root_path=write_path,partition_cols=['YEAR', 'MONTH'])
        return


    

