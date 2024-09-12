"""
    Collect public data from Nord Pool for the last N days
    of the trading auctions and intraday tradings
    for all European countries (except UK)
"""

from bs4 import BeautifulSoup
import pandas as pd
import time
import gc
import os
import re

from datetime import datetime,timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

def format_cell(cell):
    """Returns the text of cell"""

    if cell.content is None:
        return cell.text
    if len(cell.content)==0:
        return ''
    s = ''
    s = ' '.join([str(c) for c in cell.content])

    #if there is undesired contents, replace it here:
    s = s.replace('\xa0','')
    s = s.replace('\n','')
    return s

def html_to_table(tbl):
    """Extracts the table from a table found with BS"""

    #initiates the list object that will be returned:
    a=[]
    #iterates over all table rows:
    for row in tbl.find_all('tr'):
        # print(row)
        #initiates the current row to be added to a:
        r=[]

        #identifies all cells in row:
        cells=row.find_all('td')

        #if there were no normal cells, there might be header cells:
        if len(cells)==0:
            cells=row.find_all('th')

        #iterate over cells
        for cell in cells:
            cell = format_cell(cell)
            r.append(cell)
        a.append(r)
    return a

def find_string_between(string,a,b,n):
    "returns the substring of string between expressions a and b, occurence n"
    a = re.findall(a, string)
    if len(a)==0:
        return ''
    a = string.index(a[n])
    b = re.findall(b, string[a:])[0]
    b = string[a:].index(b)
    return string[a:a+b]

def safe_webdriver_initialization()-> webdriver.Chrome:
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')  # Add a default window size
    options.add_argument('--disable-gpu')  # Disable GPU hardware acceleration
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
    except Exception as e:
        print(f"Error initializing WebDriver: {e}")
        driver = None
    return driver

def load_page_headless(url:str, restarts:int, delay:int) -> str or None:

    driver = safe_webdriver_initialization()
    if not driver is None:
    # with safe_webdriver_initialization() as driver:
    #     if driver is None:
    #         raise BaseException("WebDriver not initialized")
        # driver.implicitly_wait(3)
        driver.get(url)
        # driver.implicitly_wait(3)
        # driver.implicitly_wait(10)  # Wait for up to 20 seconds for elements to appear
        try:
            wait = WebDriverWait(driver, delay)  # Wait for up to 20 seconds
            table_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.dx-datagrid-table-fixed")))
            table_html = table_element.get_attribute('outerHTML')
            del table_html

            # -------------------------------------------------
            for i in range(1000):
                page_source = driver.page_source
                table_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.dx-datagrid-table-fixed")))
                table_html = table_element.get_attribute('outerHTML')
                del table_html
                # extract data
                s = find_string_between(
                    page_source,
                    '<table class="dx-datagrid-table dx-datagrid-table-fixed" .*?>',
                    '</table>',
                    1
                )
                # if data is loaded return
                if len(s) > 0:
                    print(f'Fetching successful i={i} len(s)={len(s)}')
                    return s

            raise IOError("After 1000 attempts failed to extract table from the string")

        except Exception as e:
            print(f"Error '{e}' during scraping: {url} (Restarts {restarts})")
            return None
    else:
        BaseException("WebDriver not initialized")
        # --------------------------------------------------
        # time.sleep(2)
    # except TimeoutException:
    #     print("Timed out waiting for table to load.")

def get_page_headless_restarts(url:str)->str:
    restarts = 5

    for i in range(restarts):
        try:
            s = load_page_headless(
                url, restarts, delay=10*(i+1)
            )
        except Exception as e:
            print(f"Attemt to fetch the data failed. Attempt {i+1} / {restarts} with wait {10*(i+1)} "
                  f"gave error Error '{e}' during scraping: {url}")
            continue

        if s is not None:
            return s

    raise IOError(f"Failed to load page:{url} N={i}/{restarts} times")

''' ------------------------------------ '''
# GitHub for some reason fetches data for wrong timesteps (starting at 23.00 instead of 00:00, possible time-zone issue)
def adjust_df_for_timeshifts(df: pd.DataFrame) -> pd.DataFrame:
    # Check if the first datetime entry is at 23:00
    if df.loc[0, 'date'].hour == 23:
        print(f"Warning: Wrong initial datetime {df.iloc[0]['date']}. Adding one hour.")

    def adjust_hour(row):
        if row.hour == 23:
            return row.replace(hour=0)
        else:
            return row + pd.Timedelta(hours=1)

    # Apply the function to the datetime column
    df['date'] = df['date'].apply(adjust_hour)

    # Check if the first and last row have the same 'date'
    if df.iloc[0]['date'] == df.iloc[-1]['date'] or df.iloc[-1]['date'].hour != 23:
        print(f"Warning: Initial datetime {df.iloc[0]['date']} is the same as the last one {df.iloc[-1]['date']}. Removing the last one.")
        df = df.iloc[:-1]

    # print(f"After parsing first date {df.iloc[0]['date']}")

    return df

def scrape_auction(delivery_date_str, category, sub_category, areas)->pd.DataFrame:

    areas_str = ''
    for area in areas:
        if area!=areas[-1]:
            areas_str += area + ','
        else:
            areas_str += area
    url = f'https://data.nordpoolgroup.com/auction/{category}/{sub_category}?deliveryDate={delivery_date_str}&currency=EUR&aggregation=Hourly&deliveryAreas={areas_str}'
    #get the html from the url
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    # Fetch the webpage
    # response = requests.get(url, headers=headers)
    # response.raise_for_status()  # Raises an HTTPError for bad responses
    # html=response.text
    html=get_page_headless_restarts(url)

    #read it with BS
    bs=BeautifulSoup(html, features="lxml")

    #extract all tables and put in array t
    tables=bs.find_all('table')
    # print(f"Detected {len(tables)} tables")
    t=[]
    for tbl in tables:
        t.extend(html_to_table(tbl))
        # print('\t', t)

    df = pd.DataFrame(t)

    print(f"Initial collected data first step {df.iloc[0][0]} df.shape={df.shape}")


    # Function to convert strings with non-breaking spaces and commas as decimals to float
    def convert_to_float(value):
        if isinstance(value, str) and value.strip():  # Check if the string is not empty and not just whitespace
            # Replace non-breaking spaces and change comma to dot
            value = value.replace('\xa0', '').replace(',', '.')
            return float(value)
        return None  # Return NaN for empty or invalid input strings

    for col in df.columns[1:]:
        df[col] = df[col].apply(convert_to_float)

    if sub_category == 'volumes':
        areas_ = []
        for area in areas:
            areas_.append(area+'_buy')
            areas_.append(area+'_sell')
        areas = areas_

    # Ensure the list length matches the number of columns to be renamed
    if (len(df.columns) - 1) == len(areas):
        # Creating a dictionary to map old column names to new ones
        rename_dict = {i+1: name for i, name in enumerate(areas)}
        df.rename(columns=rename_dict, inplace=True)
    else:
        raise ValueError(f"Error: The number of names {len(areas)} provided does not match the number of columns ({len(df.columns) - 1}) to rename .")

        # Function to convert time range to datetime
    def convert_to_datetime(time_range, date):
        start_hour = time_range.split(' - ')[0]  # Split the string and take the first part
        datetime_str = f"{date} {start_hour}"    # Form the datetime string
        return pd.to_datetime(datetime_str)      # Convert to datetime object

    # Apply the conversion function to the dataframe
    df['date'] = df[0].apply(lambda x: convert_to_datetime(x, delivery_date_str))
    df.drop(0, axis=1, inplace=True)

    print(f"Collected for starting datetime of {df.iloc[0]['date']} last two: df.shape={df.shape}")

    # reorder
    cols = ['date'] + [col for col in df.columns if col != 'date']
    df = df[cols]

    df = adjust_df_for_timeshifts(df)

    return df
    #save the result:
    # f=open('res/table.csv','w')
    # for row in t:
    #     f.write(';'.join(row)+'\n')
    # f.close()
    # a=0

def scrape_intraday(delivery_date_str, category, delivery_ara)->pd.DataFrame:

    # url = f'https://data.nordpoolgroup.com/intraday/{category}/{sub_category}?deliveryDate={delivery_date_str}&currency=EUR&aggregation=Hourly&deliveryAreas={areas_str}'
    #get the html from the url
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    url = f'https://data.nordpoolgroup.com/intraday/{category}?deliveryDate={delivery_date_str}&deliveryArea={delivery_ara}'

    # Fetch the webpage
    # response = requests.get(url, headers=headers)
    # response.raise_for_status()  # Raises an HTTPError for bad responses
    # html=response.text
    html = get_page_headless_restarts(url)

    #read it with BS
    bs=BeautifulSoup(html)

    #extract all tables and put in array t
    tables = bs.find_all('table')

    t=[]
    for tbl in tables:
        t.extend(html_to_table(tbl))

    df = pd.DataFrame(t)

    # Function to convert strings with non-breaking spaces and commas as decimals to float
    def convert_to_float(value):
        if isinstance(value, str) and value.strip():  # Check if the string is not empty and not just whitespace
            # Replace non-breaking spaces and change comma to dot
            value = value.replace('\xa0', '').replace(',', '.')
            return float(value)
        return None  # Return NaN for empty or invalid input strings
    for col in df.columns[1:-2]: #last to columns are date-time
        df[col] = df[col].apply(convert_to_float)


    # print("LAST COLUMNS: ")
    # print(df[df.columns[-2]])
    # if the data is absent
    try:
        df[df.columns[-2]] = pd.to_datetime(df[df.columns[-2]], errors='coerce', format='%d.%M.%Y %H:%M:%S')
    except Exception as e:
        print("Failed to parts the first trading date:", e)
    try:
        df[df.columns[-1]] = pd.to_datetime(df[df.columns[-1]], errors='coerce', format='%d.%M.%Y %H:%M:%S')
    except Exception as e:
        print("Failed to parts the last trading date:", e)

    columns = ["high", "low", "VWAP", "open", "close", "VWAP1H", "VWAP3H", "buy_volume", "sell_volume", "transaction_volume",
               "first_trade_date", "last_trade_date"] # Note, Trading dates are in CET time not UTC!
    rename_dict = {i+1: name for i, name in enumerate(columns)}
    df.rename(columns=rename_dict, inplace=True)

    # Function to convert time range to datetime
    def convert_to_datetime(time_range, date):
        start_hour = time_range.split(' - ')[0]  # Split the string and take the first part
        datetime_str = f"{date} {start_hour}"   # Form the datetime string
        return pd.to_datetime(datetime_str)     # Convert to datetime object

    # Apply the conversion function to the dataframe
    df['date'] = df[0].apply(lambda x: convert_to_datetime(x, delivery_date_str))
    df.drop(0, axis=1, inplace=True)

    # reorder
    cols = ['date'] + [col for col in df.columns if col != 'date']
    df = df[cols]

    df = adjust_df_for_timeshifts(df)

    # df.to_csv(f'data/day_ahead_prices_{delivery_date_str}.csv', index=False)
    return df
    #save the result:
    # f=open('res/table.csv','w')
    # for row in t:
    #     f.write(';'.join(row)+'\n')
    # f.close()
    # a=

''' ------------------------------------ '''

def collect_auction_data(start_date, end_date)->None:

    market = 'auctions'

    regions = [
        "EE","LT","LV",
        "AT","BE","FR","GER","NL","PL",
        "DK1","DK2","FI","NO1","NO2","NO3","NO4","NO5","SE1","SE2","SE3","SE4","SYS"
    ]

    for sub_market in [
        'day_ahead',
        'intraday_auction_1',
        'intraday_auction_2',
        'intraday_auction_3'
    ]:
        for data_type in ['prices','volumes']:

            if not os.path.isdir(f"./data/{market}/"):
                os.mkdir(f"./data/{market}/")
            if not os.path.isdir(f"./data/{market}/{sub_market}"):
                os.mkdir(f"./data/{market}/{sub_market}")
            if not os.path.isdir(f"./data/{market}/{sub_market}/{data_type}"):
                os.mkdir(f"./data/{market}/{sub_market}/{data_type}")

            df = pd.DataFrame()
            for date in pd.date_range(start=start_date, end=end_date):
                date_str = date.strftime("%Y-%m-%d")
                print(f"Fetching {data_type} for {market} ({sub_market}) data for {date_str}")
                df_i = scrape_auction(
                    date_str,
                    sub_market.replace('_','-'),
                    data_type,
                    regions if (data_type == 'prices' and sub_market == 'day_ahead') else regions[:-1] # SYS is not in volumes
                )
                if not df_i is None:
                    df = pd.concat([df,df_i])

            temp_index = pd.DatetimeIndex(df['date'])
            frequency = temp_index.inferred_freq

            fname = f'./data/{market}/{sub_market}/{data_type}/{datetime.today().strftime("%Y-%m-%d")}_{frequency}.csv'

            df.to_csv(fname, index=False)
            print(f"Saved data to {fname}")
            print("\n")

def collect_intraday_data(start_date, end_date)->None:
    market = 'intraday'

    if not os.path.isdir(f"./data/{market}/"):
        os.mkdir(f"./data/{market}/")

        for area in [
            # "50HZ",
            "EE","LT","LV", # Baltic
            "50HZ","AMP","AT","BE","FR","GER","NL","PL","TBW","TTG", # CWE
            "DK1","DK2","FI","NO1","NO2","NO3","NO4","NO5","SE1","SE2","SE3","SE4" # Nordic
        ]:
            time.sleep(10) # to prevent NordPool from blocking the request based on frequency

            df = pd.DataFrame()

            for date in pd.date_range(start=start_date, end=end_date):
                date_str = date.strftime("%Y-%m-%d")
                print(f"Fetching {market} data for {area} for {date_str}")

                df_i = scrape_intraday(
                    date_str,'intraday-hourly-statistics',area
                )
                if not df_i is None:
                    df = pd.concat([df,df_i])

            if not os.path.isdir(f'./data/{market}/{area}/'):
                os.mkdir(f'./data/{market}/{area}/')

            temp_index = pd.DatetimeIndex(df['date'])
            frequency = temp_index.inferred_freq

            fname = f'./data/{market}/{area}/{area}_{datetime.today().strftime("%Y-%m-%d")}_{frequency}.csv'
            df.to_csv(fname, index=False)

            print(f"Saved data to {fname}")
            print("\n")


if __name__ == '__main__':

    # to assume that data was updated we always fetch the last 4 days
    end_date = pd.Timestamp(datetime.today())
    # Normalize the timestamp to remove time (set to midnight)
    end_date_normalized = end_date.normalize()

    start_date = end_date-timedelta(days=4)

    collect_auction_data(start_date, end_date)

    collect_intraday_data(start_date, end_date)