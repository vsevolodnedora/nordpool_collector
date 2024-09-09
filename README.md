# nordpool_collector

This repository contains regularly updated data from [NordPool](https://data.nordpoolgroup.com/) 
Data collection started 2024-09-09.

The data is collected separately for auctions, and specifically for 
- day-ahead auction (hourly interval)
- intraday auction 1,2,3 (15-min interval)
and for intraday trading (hourly statsitics) (hourly interval) for all european countries (excluding UK for simplicity)

In order to assure that the data was updated (especially relevant for intraday trading), each file contains data for the last four days.

The data is updated dayly.  

The data will be used for personal project related to the electricity market analysis and forecasting 

The data is scraped using [selenium](https://pypi.org/project/selenium/) and [beautifulsoup](https://pypi.org/project/beautifulsoup4/). 

The code is inspired by this [repo](https://github.com/uit-sok-1003-h24/notebooks/blob/9684fd1b29624e22be66705cbf249a148cfe30c4/res/scraping_nordpool.py) and this [repo](https://github.com/elgohr/EPEX-DE-History)
