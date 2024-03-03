from data import StockData
from data import NonStockData

stocks = [
    'hubs',
    'msft',
    'orcl',
    'crm',
    'sap'
]

stock_data = list()

for i in stocks:
    stock_data.append(StockData(i))
    print(f"{i} was sucessfuly fetched")


non_stock_data = NonStockData()