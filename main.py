import pandas as pd
from sqlalchemy import create_engine, text
from fastapi import FastAPI
from fastapi.responses import FileResponse
from typing import Union
from pydantic import BaseModel
from fastapi import HTTPException
import pickle
import database_functions as dfs
import data_fetching_functions as dff
import data
from pathlib import Path

engine = dict()

root_folder = Path(__file__).parent.resolve()

with open(f'{root_folder}/reference_data.txt', 'r') as file:

    raw_data = file.read()

app = FastAPI()

db_url = ''

with open(f'{root_folder}/db_url.txt', 'r') as file:
    db_url = file.read()

try:
    engine = create_engine(db_url)
except:
    engine = ''

@app.get('/')
async def read_root():
    return {
        'create-db-connection': 'Establishes connection with database', #
        'init-database': 'Creates tables, views, functions', #
        'add-to-load-queue': 'Adds entered stock symbol to data load queue', #
        'remove-from-load-queue': 'Remove stock symbol from data load queue', #
        'fetch_and-transform-data': 'Downloads, adapts and saves data on storage',
        'insert-monthly-data': 'Loads monthly data to database', #
        'insert-quearterly-data': 'Loads quarterly data to database', #
        'insert-yearly-data': 'Loads yearly data to database', #
        'download-csv': 'Extract necessary data based of stock',
        'kamalov-end': 'Deletes all tables, views and functions related to stock_data_pipeline' #
    }

class dataset(BaseModel):
    stock_symbol: str
    data: str = 'balance_sheet'
    period: Union[int, None] = None

@app.post('/post_data')
async def post_data(dataset: dataset):

    return f"{dataset.stock_symbol} data for {dataset.data} during {dataset.period} returned."

class db_envelope(BaseModel):
    db_url: str

@app.post('/connect-database', responses={
    403: {
        'detail': 'string',
        "description": "Invalid database url"
    }
})
def connect_database(db_envelope: db_envelope):

    """
        Note(!): Tested only with PostgreSQL. Please use this database management system.
        Establishes connection with your database.
    """
    pass

    try:
        with open(f'{root_folder}/db_url.txt', 'w') as file:
            file.write(db_envelope.dict()['db_url'])

        with open(f'{root_folder}/db_url.txt', 'r') as file:
            db_url = file.read()

        engine = create_engine(db_url)

        return 'Database connected.'
    except:
        raise HTTPException(status_code=403, detail='Connection to database failed. Please check if you entered a valid dburl')



@app.post('/init-database', responses={
    401: {
        'detail': 'string',
        "description": "Invalid database url"
    },
    403: {
        'detail': 'string',
        "description": "Database connection error"
    }
})
async def init_database():

    """
        Creates database tables, views, functions.
    """
    pass

    try:
        engine.connect()
    except:
        raise HTTPException(status_code=401, detail='Database initialization failed. Please check your db_url.')
    else:
        try:
            dfs.init_database(engine)
            return 'Database successfully initiated.'
        except:
            raise HTTPException(status_code=403, detail='Database initialization failed. Contact developer of this app. (Oh god, please don`t)')

@app.put('/add-to-load-queue', responses={
    403:{
        'detail': 'string',
        "description": "Database connection error"
    },
    500:{
        'detail': 'string',
        "description": "Addition error"
    }
})
async def add_to_load_queue(stock_symbol: str):

    """
        Adds entered stock symbol to data load queue.
        Please enter a valid stock symbol from stock exchange.
        For example: Microsoft has MSFT as stock symbol.
    """
    pass

    try:
        engine.connect()
    except:
        raise HTTPException(status_code=403, detail='Connection to database failed. Please check if you entered a valid database url.')
    else:
         try:
            return dfs.add_to_load_queue(stock_symbol, engine)
         except:
            raise HTTPException(status_code=500, detail='Addition to load queue failed. Contact developer of this app. (Oh god, please don`t)')



@app.delete('/remove-from-load-queue', responses={
    403:{
        'detail': 'string',
        "description": "Database connection error"
    },
    500:{
        'detail': 'string',
        "description": "Removing error"
    }
})
async def remove_from_load_queue(stock_symbol: str):

    """
        Remove stock symbol from data load queue.
        Please enter a valid stock symbol from stock exchange.
        For example: Microsoft has MSFT as stock symbol.
    """
    pass

    try:
        engine.connect()
    except:
        raise HTTPException(status_code=403, detail='Connection to database failed. Please check if you entered a valid database url.')
    else:
        try:
            return dfs.remove_from_load_queue(stock_symbol, engine)
        except:
            raise HTTPException(status_code=500, detail='Removing from load queue failed. Contact developer of this app. (Oh god, please don`t)')



@app.get('/get-load-queue', responses={
    403:{
        "detail": "string",
        "description": "Database connection error."
    }
})
async def get_load_queue():
    """
        Returns stocks for which data will be downloaded to
        database.
    """
    pass

    try:
        engine.connect()
        load_queue = list()
        with engine.connect() as conn:
            slct = conn.execute(text('select * from load_queue'))
            for row in slct:
                load_queue.append(row[0])
        return load_queue
    except:
        raise HTTPException(status_code=403, detail='Connection to database failed. Please check if you entered a valid database url.')
    


@app.put('/insert-monthly-data', responses={
    400:{
        "detail": "string",
        "description": "Load queue is empty"
    },
    403:{
        "detail": "string",
        "description": "Database connection error"
    },
    500:{
        "detail": "string",
        "description": "Loading data error (database)"
    },
    503:{
        "detail": "string",
        "description": "Loading data error (external api)"
    }
})
async def insert_monthly_data():
    """
        Downloads, adapts, inserts monthly data of stocks
        from load queue and non stock data to database.
        Please note that if the load queue is empty app will
        respond with error and not load anything.
    """
    pass

    load_queue = list()

    data_to_load = dict()

    try:
        engine.connect()
    except:
        raise HTTPException(status_code=403, detail='Connection to database failed. Please check if you entered a valid database url.')
    else:
        with engine.connect() as conn:
            slct = conn.execute(text('select stock_symbol from load_queue;'))
            for row in slct:
                load_queue.append(row[0])
        if len(load_queue) == 0:
            raise HTTPException(status_code=400, detail='Load queue is empty. There is nothing to load to database. Please add stock symbols to load queue.')

        try:

            non_stock_data = data.NonStockData()

            stock_data = list(map(data.StockData, load_queue))
            
            with open(f"{root_folder}/data_for_load.pickle", "wb") as data_file:
                write_dict = {
                    "non_stock_data": non_stock_data,
                    "stock_data": stock_data
                }
                pickle.dump(write_dict, data_file)

            with open(f"{root_folder}/data_for_load.pickle", "rb") as data_file:
                read_dict = pickle.load(data_file)
                data_to_load["non_stock_data"] = read_dict['non_stock_data']
                data_to_load["stock_data"] = read_dict["stock_data"]

        except:
            raise HTTPException(status_code=503, detail='Issue with data source. Contact developer of this app. (Oh god, please don`t)')

        try:
            return dfs.insert_monthly_data(
                data_to_load['non_stock_data'],
                data_to_load['stock_data'],
                engine
            )

        except:
            raise HTTPException(status_code=500, detail='Failed to load data to database. Contact developer of this app. (Oh god, please don`t)')



@app.put('/insert-quarterly-data', responses={
    400:{
        "detail": "string",
        "description": "Load queue is empty"
    },
    403:{
        "detail": "string",
        "description": "Database connection error"
    },
    500:{
        "detail": "string",
        "description": "Loading data error (database)"
    },
    503:{
        "detail": "string",
        "description": "Loading data error (external api)"
    }
})
async def insert_quarterly_data():
    """
        Downloads, adapts, inserts quarterly data of stocks
        from load queue and non stock data to database.
        Please note that if the load queue is empty app will
        respond with error and not load anything.
    """
    pass

    load_queue = list()

    data_to_load = dict()

    try:
        engine.connect()
    except:
        raise HTTPException(status_code=403, detail='Connection to database failed. Please check if you entered a valid database url.')
    else:
        with engine.connect() as conn:
            slct = conn.execute(text('select stock_symbol from load_queue;'))
            for row in slct:
                load_queue.append(row[0])
        if len(load_queue) == 0:
            raise HTTPException(status_code=400, detail='Load queue is empty. There is nothing to load to database. Please add stock symbols to load queue.')

        try:

            non_stock_data = data.NonStockData()

            stock_data = list(map(data.StockData, load_queue))
            
            with open(f"{root_folder}/data_for_load.pickle", "wb") as data_file:
                write_dict = {
                    "non_stock_data": non_stock_data,
                    "stock_data": stock_data
                }
                pickle.dump(write_dict, data_file)

            with open(f"{root_folder}/data_for_load.pickle", "rb") as data_file:
                read_dict = pickle.load(data_file)
                data_to_load["non_stock_data"] = read_dict['non_stock_data']
                data_to_load["stock_data"] = read_dict["stock_data"]

        except:
            raise HTTPException(status_code=503, detail='Issue with data source. Contact developer of this app. (Oh god, please don`t)')

        try:
            return dfs.insert_quarterly_data(
                data_to_load['non_stock_data'],
                data_to_load['stock_data'],
                engine
            )

        except:
            raise HTTPException(status_code=500, detail='Failed to load data to database. Contact developer of this app. (Oh god, please don`t)')



@app.put('/insert-yearly-data', responses={
    400:{
        "detail": "string",
        "description": "Load queue is empty"
    },
    403:{
        "detail": "string",
        "description": "Database connection error"
    },
    500:{
        "detail": "string",
        "description": "Loading data error (database)"
    },
    503:{
        "detail": "string",
        "description": "Loading data error (external api)"
    }
})
async def insert_yearly_data():
    """
        Downloads, adapts, inserts yearly data of stocks
        from load queue and non stock data to database.
        Please note that if the load queue is empty app will
        respond with error and not load anything.
    """
    pass

    load_queue = list()

    data_to_load = dict()

    try:
        engine.connect()
    except:
        raise HTTPException(status_code=403, detail='Connection to database failed. Please check if you entered a valid database url.')
    else:
        with engine.connect() as conn:
            slct = conn.execute(text('select stock_symbol from load_queue;'))
            for row in slct:
                load_queue.append(row[0])
        if len(load_queue) == 0:
            raise HTTPException(status_code=400, detail='Load queue is empty. There is nothing to load to database. Please add stock symbols to load queue.')

        try:

            non_stock_data = data.NonStockData()

            print(non_stock_data)

            stock_data = list(map(data.StockData, load_queue))

            print(stock_data)
            
            with open(f"{root_folder}/data_for_load.pickle", "wb") as data_file:
                write_dict = {
                    "non_stock_data": non_stock_data,
                    "stock_data": stock_data
                }
                pickle.dump(write_dict, data_file)

            with open(f"{root_folder}/data_for_load.pickle", "rb") as data_file:
                read_dict = pickle.load(data_file)
                data_to_load["non_stock_data"] = read_dict['non_stock_data']
                data_to_load["stock_data"] = read_dict["stock_data"]

        except:
            raise HTTPException(status_code=503, detail='Issue with data source. Contact developer of this app. (Oh god, please don`t)')

        try:
            return dfs.insert_yearly_data(
                data_to_load['non_stock_data'],
                data_to_load['stock_data'],
                engine
            )

        except:
            raise HTTPException(status_code=500, detail='Failed to load data to database. Contact developer of this app. (Oh god, please don`t)')



@app.get('/download_data', responses={
    400: {
        'detail': 'string',
        'description': 'Incorrect query'
    },
    403: {
        'detail': 'string',
        'description': 'Database connection error'
    },
    404: {
        'detail': 'string',
        'description': 'Data not found'
    }
})
def download_data(
    stock_symbol: str,
    period: str,
    excel: Union[int, None] = None
):

    """
        Downloads data based on following conditions:
        stock_symbol: Stock name in international stock exchange
        period: Period of data. Available periods: monthly, quarterly, yearly
        excel: enter 1 if you need table in xlsx format
    """

    query = f"""
        select * from {period}_data
        where stock_id = (
            select id from companies where symbol = '{stock_symbol.upper()}'
        ) and
        batch_id = (
            select max(batch_id) from data_load_batches
            where data = '{period}_data'
            and status = 'Load Complete'
        )
    """

    pass

    try:
        
        engine.connect()

    except:

        raise HTTPException(status_code=403, detail='Connection to database failed. Please check if you entered a valid database url.')
    
    else:

        try:

            file_to_download = pd.read_sql(
                query, engine
            )

        except:

            raise HTTPException(status_code=400, detail='Failed to query data. Please recheck entered values.')
        
        else:

            file_to_download.to_excel('file_to_download.xlsx')

            if len(file_to_download.index) == 0:

                raise HTTPException(status_code=404, detail='Data for entered stock symbol was not loaded to database. Please check the entered value.')

            else:
            
                if excel == 1:
                    return FileResponse(
                        'file_to_download.xlsx',
                        filename=f'{stock_symbol}_{period}_data.xlsx'
                    )
                else:
                    return file_to_download.to_dict()



@app.delete('/kamalov-end',responses={
    403:{
        "detail": "string",
        "description": "Database connection error"
    },
    500:{
        "detail": "string",
        "description": "Truncate error"
    }
})
def kamalov_end():

    """
        Please keep in mind that this method deletes ALL the data
        related to stock data pipeline. It also means whole downloaded
        data will be deleted as well. Please use this method as the last resort.
        Deletes all tables, views and functions related to stock_data_pipeline.
    """

    pass

    try:
        engine.connect()
    except:
        raise HTTPException(status_code=403, detail='Connection to database failed. Please check if you entered a valid database url.')
    
    try:
        
        DBCleaner = dfs.ClearDB(engine)
        
        DBCleaner.drop_functions()
        
        DBCleaner.drop_views()

        DBCleaner.drop_monthly_tables()
        
        DBCleaner.drop_quarterly_tables()
        
        DBCleaner.drop_yearly_tables()
        
        DBCleaner.drop_base_tables()
        
        return 'Database successfully truncated!'
    except:
        raise HTTPException(status_code=500, detail='Unfortunately truncate failed. Please call tOfFalcon')
