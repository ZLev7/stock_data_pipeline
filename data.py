import data_fetching_functions as dff
import transform_functions as tf

# regular loading ------------------------------------------------------------------------------------

class NonStockData:

  def __init__(self, currency_pair='usdeur=x'):

    self.base_currency = currency_pair

    self.raw_lending_data = list()
    self.raw_treasury_data = list()
    self.raw_currency_data = list()

  def get_monthly(self) -> dict:

    self.raw_lending_data = dff.fetch_lending_rates_data()
    self.raw_treasury_data = dff.fetch_treasury_data()
    self.raw_currency_data = dff.fetch_currency_quotes(self.base_currency)

    result = {
      'base_currency': self.base_currency,
      'period': 'monthly',
      'data': {
        'monthly_lending': tf.transform_non_yahoo_data(
          self.raw_lending_data, 'monthly'
        ),
        'monthly_treasury': tf.transform_non_yahoo_data(
          self.raw_treasury_data, 'monthly'
        ),
        'monthly_currency': tf.transform_currency_data(
          self.raw_currency_data, 'monthly'
        )
      }
    }

    return result

  def get_quarterly(self) -> dict:

    self.raw_lending_data = dff.fetch_lending_rates_data()
    self.raw_treasury_data = dff.fetch_treasury_data()
    self.raw_currency_data = dff.fetch_currency_quotes(self.base_currency)

    result = {
      'base_currency': self.base_currency,
      'period': 'quarterly',
      'data': {
        'quarterly_lending': tf.transform_non_yahoo_data(
          self.raw_lending_data, 'quarterly'
        ),
        'quarterly_treasury': tf.transform_non_yahoo_data(
          self.raw_treasury_data, 'quarterly'
        ),
        'quarterly_currency': tf.transform_currency_data(
          self.raw_currency_data, 'quarterly'
        )
      }
    }

    return result

  def get_yearly(self) -> dict:

    self.raw_lending_data = dff.fetch_lending_rates_data()
    self.raw_treasury_data = dff.fetch_treasury_data()
    self.raw_currency_data = dff.fetch_currency_quotes(self.base_currency)

    result = {
      'base_currency': self.base_currency,
      'period': 'yearly',
      'data': {
        'yearly_lending': tf.transform_non_yahoo_data(
          self.raw_lending_data, 'yearly'
        ),
        'yearly_treasury': tf.transform_non_yahoo_data(
          self.raw_treasury_data, 'yearly'
        ),
        'yearly_currency': tf.transform_currency_data(
          self.raw_currency_data, 'yearly'
        )
      }
    }

    return result



class StockData:

  def __init__(self, symbol):
    
    self.symbol = symbol.upper()
    self.company_summary = list()
    self.stock_financials = dict()
    self.stock_quotes = list()

  def get_company_summary(self) -> dict:

    self.company_summary = dff.fetch_company_summary(self.symbol)

    result = {
      'symbol': self.symbol,
      'period': 'none',
      'data': {
        'company_summary': tf.transform_company_summary(self.company_summary)
      }
    }

    return result

  def get_monthly(self) -> dict:

    self.stock_quotes = dff.fetch_stock_quotes(self.symbol)

    result = {
      'symbol': self.symbol,
      'period': 'monthly',
      'data': {
        'monthly_stock_quotes': tf.transform_stock_data(self.stock_quotes, 'monthly')
      }
    }

    return result

  def get_quarterly(self) -> dict:

    self.stock_financials['quarterly'] = dff.fetch_stock_financials(self.symbol, 'quarterly')
    self.stock_quotes = dff.fetch_stock_quotes(self.symbol)

    result = {
      'symbol': self.symbol,
      'period': 'quarterly',
      'data': {
        'quarterly_balance_sheet': tf.transform_financials_data(
          self.stock_financials['quarterly']['balance-sheet'], 'quarterly'
        ),
        'quarterly_income_statement': tf.transform_financials_data(
          self.stock_financials['quarterly']['income-statement'], 'quarterly'
        ),
        'quarterly_cash_flow_statement': tf.transform_financials_data(
          self.stock_financials['quarterly']['cash-flow-statement'], 'quarterly'
        ),
        'quarterly_financial_ratios': tf.transform_financials_data(
          self.stock_financials['quarterly']['financial-ratios'], 'quarterly'
        ),
        'quarterly_stock_quotes': tf.transform_stock_data(
          self.stock_quotes, 'quarterly'
        )
      }
    }

    return result

  def get_yearly(self) -> dict:

    self.stock_financials['yearly'] = dff.fetch_stock_financials(self.symbol, 'yearly')
    self.stock_quotes = dff.fetch_stock_quotes(self.symbol)

    result = {
      'symbol': self.symbol,
      'period': 'yearly',
      'data': {
        'yearly_balance_sheet': tf.transform_financials_data(
          self.stock_financials['yearly']['balance-sheet'], 'yearly'
        ),
        'yearly_income_statement': tf.transform_financials_data(
          self.stock_financials['yearly']['income-statement'], 'yearly'
        ),
        'yearly_cash_flow_statement': tf.transform_financials_data(
          self.stock_financials['yearly']['cash-flow-statement'], 'yearly'
        ),
        'yearly_financial_ratios': tf.transform_financials_data(
          self.stock_financials['yearly']['financial-ratios'], 'yearly'
        ),
        'yearly_stock_quotes': tf.transform_stock_data(
          self.stock_quotes, 'yearly'
        )
      }
    }

    return result


# sample loading ----------------------------------------------------------------------------------

class SampleNonStockData:

  def __init__(self, raw_data, currency_pair='usdeur=x'):

    self.base_currency = currency_pair

    self.raw_lending_data = raw_data['lending']
    self.raw_treasury_data = raw_data['treasury']
    self.raw_currency_data = raw_data['currency']

  def get_monthly(self) -> dict:

    result = {
      'base_currency': self.base_currency,
      'period': 'monthly',
      'data': {
        'monthly_lending': tf.transform_non_yahoo_data(
          self.raw_lending_data, 'monthly'
        ),
        'monthly_treasury': tf.transform_non_yahoo_data(
          self.raw_treasury_data, 'monthly'
        ),
        'monthly_currency': tf.transform_currency_data(
          self.raw_currency_data, 'monthly'
        )
      }
    }

    return result

  def get_quarterly(self) -> dict:

    result = {
      'base_currency': self.base_currency,
      'period': 'quarterly',
      'data': {
        'quarterly_lending': tf.transform_non_yahoo_data(
          self.raw_lending_data, 'quarterly'
        ),
        'quarterly_treasury': tf.transform_non_yahoo_data(
          self.raw_treasury_data, 'quarterly'
        ),
        'quarterly_currency': tf.transform_currency_data(
          self.raw_currency_data, 'quarterly'
        )
      }
    }

    return result

  def get_yearly(self) -> dict:

    result = {
      'base_currency': self.base_currency,
      'period': 'yearly',
      'data': {
        'yearly_lending': tf.transform_non_yahoo_data(
          self.raw_lending_data, 'yearly'
        ),
        'yearly_treasury': tf.transform_non_yahoo_data(
          self.raw_treasury_data, 'yearly'
        ),
        'yearly_currency': tf.transform_currency_data(
          self.raw_currency_data, 'yearly'
        )
      }
    }

    return result



class SampleStockData:

  def __init__(self, raw_data):
    
    self.symbol = raw_data['symbol']
    self.company_summary = raw_data['company_summary']
    self.stock_financials = {
      'quarterly': raw_data['stock_financials']['quarterly'],
      'yearly': raw_data['stock_financials']['yearly']
    }
    self.stock_quotes = raw_data['stock_quotes']

  def get_company_summary(self) -> dict:

    result = {
      'symbol': self.symbol,
      'period': 'none',
      'data': {
        'company_summary': tf.transform_company_summary(self.company_summary)
      }
    }

    return result

  def get_monthly(self) -> dict:

    result = {
      'symbol': self.symbol,
      'period': 'monthly',
      'data': {
        'monthly_stock_quotes': tf.transform_stock_data(self.stock_quotes, 'monthly')
      }
    }

    return result

  def get_quarterly(self) -> dict:

    result = {
      'symbol': self.symbol,
      'period': 'quarterly',
      'data': {
        'quarterly_balance_sheet': tf.transform_financials_data(
          self.stock_financials['quarterly']['balance-sheet'], 'quarterly'
        ),
        'quarterly_income_statement': tf.transform_financials_data(
          self.stock_financials['quarterly']['income-statement'], 'quarterly'
        ),
        'quarterly_cash_flow_statement': tf.transform_financials_data(
          self.stock_financials['quarterly']['cash-flow-statement'], 'quarterly'
        ),
        'quarterly_financial_ratios': tf.transform_financials_data(
          self.stock_financials['quarterly']['financial-ratios'], 'quarterly'
        ),
        'quarterly_stock_quotes': tf.transform_stock_data(
          self.stock_quotes, 'quarterly'
        )
      }
    }

    return result

  def get_yearly(self) -> dict:

    result = {
      'symbol': self.symbol,
      'period': 'yearly',
      'data': {
        'yearly_balance_sheet': tf.transform_financials_data(
          self.stock_financials['yearly']['balance-sheet'], 'yearly'
        ),
        'yearly_income_statement': tf.transform_financials_data(
          self.stock_financials['yearly']['income-statement'], 'yearly'
        ),
        'yearly_cash_flow_statement': tf.transform_financials_data(
          self.stock_financials['yearly']['cash-flow-statement'], 'yearly'
        ),
        'yearly_financial_ratios': tf.transform_financials_data(
          self.stock_financials['yearly']['financial-ratios'], 'yearly'
        ),
        'yearly_stock_quotes': tf.transform_stock_data(
          self.stock_quotes, 'yearly'
        )
      }
    }

    return result
  