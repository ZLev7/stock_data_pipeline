import data_fetching_functions as dff
from pathlib import Path
import json


def write_reference_data(default_symbol='aapl', default_currency='usdeur=x'):

    dict_to_write = {

        'symbol': default_symbol,

        'company_summary': dff.fetch_company_summary(default_symbol),

        'non_stock_data': {

            'lending': dff.fetch_lending_rates_data(),
    
            'treasury': dff.fetch_treasury_data(),
            
            'currency': dff.fetch_currency_quotes(default_currency)

        },

        'stock_quotes': dff.fetch_stock_quotes(default_symbol),
        
        'stock_financials': {
            'quarterly': dff.fetch_stock_financials(default_symbol, 'quarterly'),
            'yearly': dff.fetch_stock_financials(default_symbol, 'yearly')
        }

    }

    loc = Path(__file__).with_name('reference_data.txt')

    with open(loc, 'w') as file:

        file.write(json.dumps(dict_to_write))

    return 'Reference data has been written to file "reference_data.txt"'

print(write_reference_data())



