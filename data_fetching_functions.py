import requests
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
import random
import time


# 0 ------------------------------------------------------------------------------------------------


functions_list = [
  'fetch_lending_rates_data() -> dict', 'fetch_treasury_data() -> dict',
  'fetch_treasury_data() -> dict',
  'fetch_currency_quotes(currency_pair) -> dict',
  'fetch_company_summary(company_symbol) -> dict',
  'fetch_stock_financials(stock_name, period) -> dict',
  'fetch_stock_quotes(stock_name) -> dict'
]


# non_stock_data_functions -------------------------------------------------------------------------


# 1 ------------------------------------------------------------------------------------------------


def fetch_lending_rates_data() -> dict:

  dictionary = {
    'dates': list(),
    'data': {
      'values': list(),
      'for_grouping': list()
    }
  }

  params = {
    'startDt': '1000-01-01',
    'endDt': '9999-12-31',
    'eventCodes': '500',
    'productCode': '50',
    'sort': 'postDt:-1',
    'eventCode': '1',
    'startPosition': '0'
  }

  response = requests.get('https://markets.newyorkfed.org/read', params=params)

  if response.status_code != 200:

    error_dict = {
      'function': 'fetch_lending_rates_data()',
      'error_code': response.status_code
    }

    raise ValueError(error_dict)

  for i in response.json()['data']:
    val = json.loads(i['data'])
    dictionary['dates'].append(val['refRateDt'])
    dictionary['data']['values'].append(val['dailyRate'])
    dictionary['data']['for_grouping'].append(val['refRateDt'])

  return dictionary

# 2 ------------------------------------------------------------------------------------------------


def fetch_treasury_data() -> dict:

  external_params = {
    'type': 'daily_treasury_yield_curve',
    'field_tdr_date_value': '2023'
  }

  external_headers = {
    'user-agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
  }

  external_response = requests.get(
    'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView',
    params=external_params,
    headers=external_headers)

  if external_response.status_code != 200:

    error_dict = {
      'function': 'fetch_treasury_data()(external_response)',
      'error_code': external_response.status_code
    }

    raise ValueError(error_dict)

  external_soup = BeautifulSoup(external_response.text, 'html.parser')

  dictionary = {
    'dates': list(),
    'data': {
      'values': list(),
      'for_grouping': list()
    }
  }

  years = external_soup.find('select',
                             attrs={'id': 'edit-field-tdr-date-value'})

  for i in years.find_all('option')[1:-1]:

    params = {
      'type': 'daily_treasury_yield_curve',
      'field_tdr_date_value': i.text
    }

    headers = {
      'user-agent':
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    response = requests.get(
      'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView',
      params=params,
      headers=headers)

    if response.status_code != 200:

      error_dict = {
        'function': 'fetch_treasury_data()',
        'error_code': response.status_code
      }

      raise ValueError(error_dict)

    soup = BeautifulSoup(response.text, 'html.parser')

    for a in soup.find_all('tr')[1:]:

      value = '0' if a.find_all('td')[15].string == ' ' else a.find_all(
        'td')[15].string

      dictionary['dates'].append(a.find('time').text)

      dictionary['data']['values'].append(float(value))

      dictionary['data']['for_grouping'].append(a.find('time').text)

  return dictionary


# 3 ------------------------------------------------------------------------------------------------


def fetch_currency_quotes(currency_pair) -> dict:

  headers = {
    'user-agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
  }

  params = {
    'interval': '1d',
    'period1': str(int(datetime(1975, 1, 1).timestamp())),
    'period2': str(int(datetime(2100, 1, 1).timestamp()))
  }

  response = requests.get(
    f'https://query1.finance.yahoo.com/v8/finance/chart/{currency_pair}',
    params=params,
    headers=headers)

  if response.status_code != 200:

    error_dict = {
      'function': 'fetch_currency_quotes()',
      'error_code': response.status_code,
      'error_text_code': response.json()['chart']['error']['code'],
      'error_description': response.json()['chart']['error']['description']
    }

    raise ValueError(error_dict)

  dictionary = {
    'dates': response.json()['chart']['result'][0]['timestamp'],
    'data': response.json()['chart']['result'][0]['indicators']['quote'][0]
  }

  del dictionary['data']['volume']
  dictionary['data']['for_grouping'] = dictionary['dates']

  return dictionary


# end ----------------------------------------------------------------------------------------------


# stock_data_functions -----------------------------------------------------------------------------


# 1 ------------------------------------------------------------------------------------------------


def fetch_company_summary(company_symbol) -> dict:

  response = requests.get('https://fc.yahoo.com/')

  cookie = list(response.cookies)[0]

  headers = {
    'user-agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
  }

  url = 'https://query2.finance.yahoo.com/v1/test/getcrumb'

  crumb = requests.get(url, headers=headers).text

  headers = {
    'user-agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
  }

  crumb = requests.get(
    'https://query2.finance.yahoo.com/v1/test/getcrumb',
    headers=headers,
    cookies={cookie.name: cookie.value}
  ).text
  
  asset_response = requests.get(
    f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{company_symbol}?modules=assetProfile&crumb={crumb}',
    headers=headers,
    cookies={cookie.name: cookie.value})
  
  search_response = requests.get(
    f'https://query2.finance.yahoo.com/v1/finance/search?q={company_symbol}',
    headers=headers,
    cookies={cookie.name: cookie.value})
  
  if asset_response.status_code != 200 or search_response.status_code != 200:

    error_dict = {
      'function':
      'fetch_company_summary()',
      'error_code':
      asset_response.status_code,
      'error_text_code':
      asset_response.json()['quoteSummary']['error']['code'],
      'error_description':
      asset_response.json()['quoteSummary']['error']['description']
    }

    raise ValueError(error_dict)

  company_data = {
    'asset_profile':
    asset_response.json()['quoteSummary']['result'][0]['assetProfile'],
    'asset_search':
    search_response.json()['quotes'][0]
  }

  return {
    'symbol': company_symbol.upper(),
    'name': company_data['asset_search']['longname'],
    'country': company_data['asset_profile']['country'],
    'city': company_data['asset_profile']['city'],
    'address': company_data['asset_profile']['address1'],
    'industry': company_data['asset_profile']['industry'],
    'sector': company_data['asset_profile']['sector'],
    'stoc_exchange': company_data['asset_search']['exchDisp'],
    'employees_number': company_data['asset_profile']['fullTimeEmployees'],
    'audit_risk': company_data['asset_profile']['auditRisk']
                        if 'auditRisk' in company_data['asset_profile'] else 0,
    'board_risk': company_data['asset_profile']['boardRisk']
                        if 'boardRisk' in company_data['asset_profile'] else 0
  }


# 2 ------------------------------------------------------------------------------------------------


def fetch_stock_financials(stock_name, period) -> dict:

  dictionary = {
    'income-statement': list(),
    'balance-sheet': list(),
    'cash-flow-statement': list(),
    'financial-ratios': list()
  }

  freequencies = {'quarterly': 'Q', 'yearly': 'A'}

  if period not in freequencies.keys():

    error_dict = {
      'function':
      'fetch_stock_financials()',
      'error_text':
      f"Wrong entered period: {period} Acceptable periods: 'quarterly', 'annualy'."
    }

    raise ValueError(error_dict)

  headers = {
    'user-agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
  }

  def lowerize(str) -> str:
    act1 = "_".join([x.lower() for x in re.split(r" |-", str)])
    act2 = act1.replace('/', '_dash_')
    act3 = act2.replace(',_', '_')
    act4 = act3.replace('(', '_or_')
    act5 = act4.replace(')', '')
    act6 = re.sub(r"___|__", '_', act5)
    act7 = act6.replace('&', '_and_')
    return act7

  def parse_data(text) -> dict:

    re_find = re.findall(r'var originalData = .*;', text)

    raw_data = json.loads(re_find[0][18:-1])

    data = {'data': dict()}

    for i in raw_data:
      data['data'][lowerize(re.findall(r'>.*<', i['field_name'])[0][1:-1])] = list()
      del i['field_name']
      del i['popup_icon']

    for a in range(len(data['data'].keys())):
      data['data'][list(data['data'].keys())[a]] = list(raw_data[a].values())

    data['dates'] = list(raw_data[0].keys())
    data['data']['for_grouping'] = list(raw_data[0].keys())

    return data
  
  chrome_options = webdriver.ChromeOptions()

  chrome_options.add_argument("--no-sandbox")
  chrome_options.add_argument("--headless")
  from selenium_stealth import stealth
  wd = webdriver.Chrome(options=chrome_options)
  stealth(wd,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
        )

  wd.get(f"https://www.macrotrends.net/stocks/charts/{stock_name.upper()}/")

  company_name = wd.current_url.split('/')[-2]
  time.sleep(random.randint(25, 35))
  wd.quit()

  main_link = 'https://www.macrotrends.net/stocks/charts/AAPL/apple/income-statement?freq=Q'

  main_link = requests.get(
    f'https://www.macrotrends.net/stocks/charts/{stock_name.upper()}/{company_name}/',
    headers=headers)

  for i in dictionary.keys():

    wd = webdriver.Chrome(options=chrome_options)

    stealth(wd,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
        )

    wd.get(main_link.url + i + f'?freq={freequencies[period]}')
    
    dictionary[i] = parse_data(wd.page_source)

    time.sleep(random.randint(25, 35))

    wd.quit()

  dictionary['period'] = period

  return dictionary


# 3 ------------------------------------------------------------------------------------------------


def fetch_stock_quotes(stock_name) -> dict:

  headers = {
    'user-agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
  }

  params = {
    'interval': '1d',
    'period1': str(int(datetime(1975, 1, 1).timestamp())),
    'period2': str(int(datetime(2100, 1, 1).timestamp()))
  }

  response = requests.get(
    f'https://query1.finance.yahoo.com/v8/finance/chart/{stock_name}',
    params=params,
    headers=headers)

  if response.status_code != 200:

    error_dict = {
      'function': 'fetch_stock_quotes()',
      'error_code': response.status_code,
      'error_text_code': response.json()['chart']['error']['code'],
      'error_description': response.json()['chart']['error']['description']
    }

    raise ValueError(error_dict)

  dictionary = {
    'dates': response.json()['chart']['result'][0]['timestamp'],
    'data': response.json()['chart']['result'][0]['indicators']['quote'][0]
  }

  dictionary['data']['for_grouping'] = dictionary['dates']

  return dictionary
