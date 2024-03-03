import pandas as pd


# 0 ------------------------------------------------------------------------------------------------


functions_list = [
  "transform_non_yahoo_data(data, period) -> pd.DataFrame",
  "transform_currency_data(data, period) -> pd.DataFrame",
  "transform_company_summary(data) -> pd.DataFrame",
  "transform_stock_data(data, period) -> pd.DataFrame",
  "transform_financials_data(data, period) -> pd.DataFrame"
]


# non_stock_data_functions -------------------------------------------------------------------------


# 1 ------------------------------------------------------------------------------------------------


def transform_non_yahoo_data(data, period) -> pd.DataFrame:

  periods = {'monthly': 'M', 'quarterly': 'Q', 'yearly': 'Y'}

  if period not in periods.keys():

    error_dict = {
      'function':
      'transform_non_yahoo_data()',
      'error_description':
      f"Wrong entered period: '{period}'. Available periods: 'monthly', 'quarterly', 'yearly'"
    }

    raise ValueError(error_dict)

  df = pd.DataFrame(index=pd.to_datetime(data['dates']),
                    data=data['data']).sort_index()

  grouped_df = df.groupby([
    pd.DatetimeIndex(df.index).to_period(periods[period])
  ]).agg(open_date=('for_grouping', 'min'),
         last_date=('for_grouping', 'max'),
         open=('values', 'first'),
         average=('values', 'mean'),
         close=('values', 'last'),
         low=('values', 'min'),
         low_date=('values', 'idxmin'),
         high=('values', 'max'),
         high_date=('values', 'idxmax'))

  grouped_df['open_date'] = pd.to_datetime(grouped_df['open_date'])

  grouped_df['last_date'] = pd.to_datetime(grouped_df['last_date'])

  execute = f"""grouped_df.insert(
    1, 'close_date',
    pd.to_datetime(grouped_df['open_date']) + business_dates.B{period.capitalize()[:-2]}End())"""

  eval(execute)

  grouped_df.insert(0, 'period', grouped_df.index)
  grouped_df['period'] = grouped_df['period'].map(lambda x: str(x))

  for i in grouped_df.select_dtypes(include=['datetime64[ns]']).columns:
    grouped_df[i] = grouped_df[i].dt.date

  return grouped_df.sort_index(ascending=False)


# 2 ------------------------------------------------------------------------------------------------


def transform_currency_data(data, period) -> pd.DataFrame:

  df = pd.DataFrame(index=pd.to_datetime(data['dates'], unit='s'),
                    data=data['data'])

  periods = {'monthly': 'M', 'quarterly': 'Q', 'yearly': 'Y'}

  if period not in periods.keys():

    error_dict = {
      'function':
      'transform_stock_data()',
      'error_description':
      f"Wrong entered period: '{period}'. Available periods: 'monthly', 'quarterly', 'yearly'"
    }

    raise ValueError(error_dict)

  grouped_df = df.groupby([df.index.to_period(periods[period])
                           ]).agg(open_date=('for_grouping', 'min'),
                                  last_date=('for_grouping', 'max'),
                                  open=('open', 'first'),
                                  avg_open=('open', 'mean'),
                                  close=('close', 'last'),
                                  avg_close=('close', 'mean'),
                                  low=('low', 'min'),
                                  avg_low=('low', 'mean'),
                                  low_date=('low', 'idxmin'),
                                  high=('high', 'max'),
                                  avg_high=('high', 'mean'),
                                  high_date=('high', 'idxmax'))

  grouped_df['open_date'] = pd.to_datetime(grouped_df['open_date'], unit='s')

  execute = f"""grouped_df.insert(
    1, 'close_date',
    pd.to_datetime(grouped_df['open_date']) + business_dates.B{period.capitalize()[:-2]}End())"""

  eval(execute)

  grouped_df['last_date'] = pd.to_datetime(grouped_df['last_date'], unit='s')
  
  grouped_df.insert(0, 'period', grouped_df.index)
  grouped_df['period'] = grouped_df['period'].map(lambda x: str(x))

  for i in grouped_df.select_dtypes(include=['datetime64[ns]']).columns:
    grouped_df[i] = grouped_df[i].dt.date

  return grouped_df.sort_index(ascending=False)


# stock_data_functions -----------------------------------------------------------------------------


# 1 ------------------------------------------------------------------------------------------------


def transform_company_summary(data) -> pd.DataFrame:

  df = pd.DataFrame(index=[1], data=data)

  return df


# 2 ------------------------------------------------------------------------------------------------


def transform_stock_data(data, period) -> pd.DataFrame:

  df = pd.DataFrame(index=pd.to_datetime(data['dates'], unit='s'),
                    data=data['data'])

  periods = {'monthly': 'M', 'quarterly': 'Q', 'yearly': 'Y'}

  if period not in periods.keys():

    error_dict = {
      'function':
      'transform_stock_data()',
      'error_description':
      f"Wrong entered period: '{period}'. Available periods: 'monthly', 'quarterly', 'yearly'"
    }

    raise ValueError(error_dict)

  grouped_df = df.groupby([df.index.to_period(periods[period])
                           ]).agg(open_date=('for_grouping', 'min'),
                                  last_date=('for_grouping', 'max'),
                                  open=('open', 'first'),
                                  avg_open=('open', 'mean'),
                                  close=('close', 'last'),
                                  avg_close=('close', 'mean'),
                                  low=('low', 'min'),
                                  avg_low=('low', 'mean'),
                                  low_date=('low', 'idxmin'),
                                  high=('high', 'max'),
                                  avg_high=('high', 'mean'),
                                  high_date=('high', 'idxmax'),
                                  avg_volume=('volume', 'mean'),
                                  open_volume=('volume', 'first'),
                                  close_volume=('volume', 'last'),
                                  low_volume=('volume', 'min'),
                                  low_volume_date=('volume', 'idxmin'),
                                  high_volume=('volume', 'max'),
                                  high_volume_date=('volume', 'idxmax'))

  grouped_df['open_date'] = pd.to_datetime(grouped_df['open_date'], unit='s')

  execute = f"""grouped_df.insert(
    1, 'close_date',
    pd.to_datetime(grouped_df['open_date']) + business_dates.B{period.capitalize()[:-2]}End())"""

  eval(execute)

  grouped_df['last_date'] = pd.to_datetime(grouped_df['last_date'], unit='s')

  grouped_df.insert(0, 'period', grouped_df.index)
  grouped_df['period'] = grouped_df['period'].map(lambda x: str(x))

  for i in grouped_df.select_dtypes(include=['datetime64[ns]']).columns:
    grouped_df[i] = grouped_df[i].dt.date

  return grouped_df.sort_index(ascending=False)


# 3 ------------------------------------------------------------------------------------------------


def transform_financials_data(data, period) -> pd.DataFrame:

  periods = {'quarterly': 'Q', 'yearly': 'Y'}

  if period not in periods.keys():

    error_dict = {
      'function':
      'transform_financials_data()',
      'error_description':
      f"Wrong entered period: '{period}'. Available periods: 'quarterly', 'yearly'"
    }

    raise ValueError(error_dict)

  df = pd.DataFrame(index=pd.DatetimeIndex(data['dates']).to_period(
    periods[period]),
                    data=data['data'])

  df.insert(0, 'period', df.index)

  df['period'] = df['period'].map(lambda x: str(x))

  for i in df.select_dtypes(include=['datetime64[ns]']).columns:
    [i] = df[i].dt.date

  for a in df.columns[1:-1]:
    df[a] = df[a].replace('', 0)
    df[a] = df[a].apply(float)

  return df
