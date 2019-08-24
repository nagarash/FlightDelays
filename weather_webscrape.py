"""
Functions to scrapes data from the IEM ASOS download service
adopted from https://github.com/akrherz/iem/blob/master/scripts/asos/iem_scraper_example.py
"""

from __future__ import print_function
import json
import time
import datetime
# Python 2 and 3: alternative 4
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

def download_data(complete_url,MAX_ATTEMPTS):
    """Fetch the data from the IEM
    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.
    Args:
      uri (string): URL to fetch
    Returns:
      string data
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            data = urlopen(complete_url, timeout=300).read().decode('utf-8')
            if data is not None and not data.startswith('ERROR'):
                return data
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return data

def create_year_str(year):
	"""
	create url string to get data for entire year
	"""
	startts = datetime.datetime(int(year), 1, 1)
    endts = datetime.datetime(int(year), 12, 31)

    year_str += startts.strftime('year1=%Y&month1=%m&day1=%d&')
    year_str += endts.strftime('year2=%Y&month2=%m&day2=%d')

    return year_str

def get_stations_from_filelist(filename,param):
    """Build a listing of stations from a csv file listing the stations.
    The file should simply have one station per line.
    """
    import pandas as pd

    df = pd.read_csv(filename)
    stations = df[param].tolist()

    return stations

def create_variable_str(var_list,param):
	"""
	create url string concatenating list of variables of interest
	"""
	var_str = ""
	for var in var_list:
		var_str+= "%s=%s&" % (param,var)

	return var_str[:-1] # remove last &

def main():
    """Our main method"""

    # Number of attempts to download data
	MAX_ATTEMPTS = 6
	
	# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
	SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"

	# specify years to request data for
	years =[2016,2017,2018,2019]

	# specify parameters to query and create parameters string
	params = ['tmpf','dwpf', 'relh', 'feel', 'sknt', 'alti', 'mslp', 'p01m', 'vsby', 'gust', 'wxcode', 'ice_accretion1hr',
	'ice_accretion_3hr', 'ice_accretion_6hr', 'peak_wind_gust']
	param_str = create_variable_str(params,'data')

	# get list of stations and create stations string
	stations = get_stations_from_filelist('airports.csv','ORIGIN')
	stations_str = create_variable_str(stations,'station')

	# create generic url string without year specification
    service = SERVICE + "tz=Etc%2FUTC&format=onlycomma&latlon=no&missing=M&trace=T&direct=no&report_type=1&report_type=2&"
    service+= param_str + "&" + stations_str + "&"

    for year in years:
    	year_str = create_year_str(year)
    	service+= year_str
    	data = download_data(service)
    	outfn = 'airports_weather_%s.txt' % (year)
        out = open(outfn, 'w')
        out.write(data)
        out.close()


if __name__ == '__main__':
    main()