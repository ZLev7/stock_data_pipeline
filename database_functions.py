from datetime import datetime
from sqlalchemy import text, create_engine
import json
import pandas as pd
import data as d
import requests
from pathlib import Path

root_folder = Path(__file__).parent.resolve()

####################################################################################################

# Data Loading -------------------------------------------------------------------------------------

####################################################################################################

# 1 ------------------------------------------------------------------------------------------------

def insert_monthly_data(non_stock_data, stock_data:list, connection) -> dict:

    stock_status_return = dict()

    with connection.connect() as conn:

        conn.execute(
            text(f"""
                insert into data_load_batches(start_date, data, status)
                            values(timestamp '{str(datetime.today())}', 'monthly_data', 'Load Start');
                commit;
            """)
        )

        batch_result = conn.execute(
            text('select max(batch_id) from data_load_batches')
        )

        batch_id = 0

        for row in batch_result:

            batch_id = row[0]

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Initated');
                commit;
            """)
        )


        
        non_stock_df_data = non_stock_data.get_monthly()['data']

        for l1 in non_stock_df_data.keys():

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Start');
                    commit;
                """)
            )

            processed_data = non_stock_df_data[l1]

            processed_data.insert(
                0, 'batch_id', batch_id
            )

            processed_data.to_sql(
                l1,
                con=connection,
                if_exists='append',
                index=False
            )

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Complete');
                    commit;
                """)
            )



        for l1 in stock_data:

            company_data = l1.get_company_summary()

            stock_id = 0

            stock_result = conn.execute(
                text(f"""
                    select id from companies where symbol = upper('{company_data['symbol']}')
                """)
            )

            for row in stock_result:
                stock_id = row[0]

            if stock_id == 0:

                company_data['data']['company_summary'].to_sql(
                    'companies',
                    con=connection,
                    if_exists='append',
                    index=False
                )

                stock_result = conn.execute(
                    text(f"""
                        select id from companies where symbol = upper('{company_data['symbol']}')
                    """)
                )

                for row in stock_result:
                    stock_id = row[0]

            df_data = l1.get_monthly()['data']

            for l2 in df_data.keys():

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', '{company_data['symbol']}', timestamp '{str(datetime.today())}', 'Load Start');
                        commit;
                    """)
                )

                processed_data = df_data[l2]

                processed_data.insert(
                    0, 'batch_id', batch_id
                )

                processed_data.insert(
                    1, 'stock_id', stock_id
                )

                processed_data.to_sql(
                    l2,
                    con=connection,
                    if_exists='append',
                    index=False
                )

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', '{company_data['symbol']}', timestamp '{str(datetime.today())}', 'Load Complete')
                    """)
                )

                stock_status_return[company_data['symbol']] = list(df_data.keys())

        conn.execute(
            text(f"""
                update data_load_batches
                set end_date = timestamp '{str(datetime.today())}',
                    status = 'Load Complete'
                where batch_id = {batch_id}
            """)
        )

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Finished');
                commit;
            """)
        )

        return {
            'status_code': '200',
            'status_string': 'Success',
            'non_stock_data_loaded': list(non_stock_df_data),
            'stock_data_loaded': stock_status_return
        }

# 2 ------------------------------------------------------------------------------------------------

def insert_quarterly_data(non_stock_data, stock_data:list, connection) -> dict:

    stock_status_return = dict()

    with connection.connect() as conn:

        conn.execute(
            text(f"""
                insert into data_load_batches(start_date, data, status)
                            values(timestamp '{str(datetime.today())}', 'quarterly_data', 'Load Start');
                commit;
            """)
        )

        batch_result = conn.execute(
            text('select max(batch_id) from data_load_batches')
        )

        batch_id = 0

        for row in batch_result:

            batch_id = row[0]

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Initated');
                commit;
            """)
        )


        
        non_stock_df_data = non_stock_data.get_quarterly()['data']

        for l1 in non_stock_df_data.keys():

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Start');
                    commit;
                """)
            )

            processed_data = non_stock_df_data[l1]

            processed_data.insert(
                0, 'batch_id', batch_id
            )

            processed_data.to_sql(
                l1,
                con=connection,
                if_exists='append',
                index=False
            )

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Complete');
                    commit;
                """)
            )



        for l1 in stock_data:

            company_data = l1.get_company_summary()

            stock_id = 0

            stock_result = conn.execute(
                text(f"""
                    select id from companies where symbol = upper('{company_data['symbol']}')
                """)
            )

            for row in stock_result:
                stock_id = row[0]

            if stock_id == 0:

                company_data['data']['company_summary'].to_sql(
                    'companies',
                    con=connection,
                    if_exists='append',
                    index=False
                )

                stock_result = conn.execute(
                    text(f"""
                        select id from companies where symbol = upper('{company_data['symbol']}')
                    """)
                )

                for row in stock_result:
                    stock_id = row[0]

            df_data = l1.get_quarterly()['data']

            for l2 in df_data.keys():

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', '{company_data['symbol']}', timestamp '{str(datetime.today())}', 'Load Start');
                        commit;
                    """)
                )

                processed_data = df_data[l2]

                processed_data.insert(
                    0, 'batch_id', batch_id
                )

                processed_data.insert(
                    1, 'stock_id', stock_id
                )

                processed_data.to_sql(
                    l2,
                    con=connection,
                    if_exists='append',
                    index=False
                )

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', '{company_data['symbol']}', timestamp '{str(datetime.today())}', 'Load Complete')
                    """)
                )

                stock_status_return[company_data['symbol']] = list(df_data.keys())

        conn.execute(
            text(f"""
                update data_load_batches
                set end_date = timestamp '{str(datetime.today())}',
                    status = 'Load Complete'
                where batch_id = {batch_id}
            """)
        )

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Finished');
                commit;
            """)
        )

        return {
            'status_code': '200',
            'status_string': 'Success',
            'non_stock_data_loaded': list(non_stock_df_data),
            'stock_data_loaded': stock_status_return
        }

# 3 ------------------------------------------------------------------------------------------------

def insert_yearly_data(non_stock_data, stock_data:list, connection) -> dict:

    stock_status_return = dict()

    with connection.connect() as conn:

        conn.execute(
            text(f"""
                insert into data_load_batches(start_date, data, status)
                            values(timestamp '{str(datetime.today())}', 'yearly_data', 'Load Start');
                commit;
            """)
        )

        batch_result = conn.execute(
            text('select max(batch_id) from data_load_batches')
        )

        batch_id = 0

        for row in batch_result:

            batch_id = row[0]

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Initated');
                commit;
            """)
        )


        
        non_stock_df_data = non_stock_data.get_yearly()['data']

        for l1 in non_stock_df_data.keys():

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Start');
                    commit;
                """)
            )

            processed_data = non_stock_df_data[l1]

            processed_data.insert(
                0, 'batch_id', batch_id
            )

            processed_data.to_sql(
                l1,
                con=connection,
                if_exists='append',
                index=False
            )

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Complete');
                    commit;
                """)
            )



        for l1 in stock_data:

            company_data = l1.get_company_summary()

            stock_id = 0

            stock_result = conn.execute(
                text(f"""
                    select id from companies where symbol = upper('{company_data['symbol']}')
                """)
            )

            for row in stock_result:
                stock_id = row[0]

            if stock_id == 0:

                company_data['data']['company_summary'].to_sql(
                    'companies',
                    con=connection,
                    if_exists='append',
                    index=False
                )

                stock_result = conn.execute(
                    text(f"""
                        select id from companies where symbol = upper('{company_data['symbol']}')
                    """)
                )

                for row in stock_result:
                    stock_id = row[0]

            df_data = l1.get_yearly()['data']

            for l2 in df_data.keys():

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', '{company_data['symbol']}', timestamp '{str(datetime.today())}', 'Load Start');
                        commit;
                    """)
                )

                processed_data = df_data[l2]

                processed_data.insert(
                    0, 'batch_id', batch_id
                )

                processed_data.insert(
                    1, 'stock_id', stock_id
                )

                processed_data.to_sql(
                    l2,
                    con=connection,
                    if_exists='append',
                    index=False
                )

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', '{company_data['symbol']}', timestamp '{str(datetime.today())}', 'Load Complete')
                    """)
                )

                stock_status_return[company_data['symbol']] = list(df_data.keys())

        conn.execute(
            text(f"""
                update data_load_batches
                set end_date = timestamp '{str(datetime.today())}',
                    status = 'Load Complete'
                where batch_id = {batch_id}
            """)
        )

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Finished');
                commit;
            """)
        )

        return {
            'status_code': '200',
            'status_string': 'Success',
            'non_stock_data_loaded': list(non_stock_df_data),
            'stock_data_loaded': stock_status_return
        }

# 4 ------------------------------------------------------------------------------------------------

def add_to_load_queue(stock_symbol, connection) -> str:

    response = requests.get('https://fc.yahoo.com/')

    cookie = list(response.cookies)[0]

    headers = {
        'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    }

    url = 'https://query2.finance.yahoo.com/v1/test/getcrumb'

    crumb = requests.get(url, headers=headers, cookies={cookie.name: cookie.value}).text

    headers = {
        'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    }

    query = f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{stock_symbol.upper()}?modules=assetProfile&crumb={crumb}'

    response = requests.get(query, headers=headers, cookies={cookie.name: cookie.value})
    # return cookie
    try:

        check = response.json()['quoteSummary']['result'][0]['assetProfile']

        with connection.connect() as conn:

            response = str()

            query_result = conn.execute(text(
                f"select add_to_queue('{stock_symbol.upper()}');"
            ))

            for i in query_result:
                response = i

            conn.execute(text('commit;'))

        return response[0]

    except:

        return 'Such stock symbol does not exist. Please check entered value.'
    
# 5 ------------------------------------------------------------------------------------------------

def remove_from_load_queue(stock_symbol, connection) -> str:

    try:

        with connection.connect() as conn:

            response = str()

            query_result = conn.execute(text(
                f"select remove_from_queue('{stock_symbol.upper()}');"
            ))

            for i in query_result:
                response = i

            conn.execute(text('commit;'))

        return response[0]

    except:

        return 'Such stock symbol does not exist. Please check entered value.'

####################################################################################################

# DataBase Initiation ------------------------------------------------------------------------------

####################################################################################################

# 0 ------------------------------------------------------------------------------------------------

def create_base_tables(connection):

    with connection.connect() as conn:

        conn.execute(
            text(
                """

                    create table companies(
                        id serial PRIMARY KEY,
                        symbol varchar(10),
                        name varchar(35),
                        country varchar(35),
                        city varchar(35),
                        address varchar(35),
                        industry varchar(40),
                        sector varchar(40),
                        stoc_exchange varchar(20),
                        employees_number int,
                        audit_risk int,
                        board_risk int
                    );

                    create table data_load_batches(
                        batch_id serial PRIMARY KEY,
                        data VARCHAR(15),
                        start_date timestamp,
                        end_date timestamp,
                        status VARCHAR(13)
                    );

                    create table load_log(
                        batch_id int references data_load_batches(batch_id),
                        table_name varchar(30),
                        stock_symbol varchar(10),
                        effective_date timestamp,
                        status VARCHAR(13) 
                    );

                    create table load_queue(
                        stock_symbol varchar
                    );

                    commit;

                """
            )
        )

    return 'Base Tables were successfully created'

# 1 ------------------------------------------------------------------------------------------------

def create_monthly_tables(non_stock_data, stock_data:list, connection) -> dict:

    stock_status_return = dict()

    with connection.connect() as conn:

        conn.execute(
            text(f"""
                insert into data_load_batches(start_date, data, status)
                            values(timestamp '{str(datetime.today())}', 'monthly_data', 'Load Start');
                commit;
            """)
        )

        batch_result = conn.execute(
            text('select max(batch_id) from data_load_batches')
        )

        batch_id = 0

        for row in batch_result:

            batch_id = row[0]

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Initated');
                commit;
            """)
        )


        
        non_stock_df_data = non_stock_data.get_monthly()['data']

        for l1 in non_stock_df_data.keys():

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Start');
                    commit;
                """)
            )

            processed_data = non_stock_df_data[l1]

            processed_data.insert(
                0, 'batch_id', batch_id
            )

            processed_data.to_sql(
                l1,
                con=connection,
                if_exists='append',
                index=False
            )

            conn.execute(
                text(
                    f"""
                        alter table {l1}
                        add constraint {l1}_bfk foreign key(batch_id)
                        references data_load_batches(batch_id);
                        commit;
                    """
                )
            )

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Complete');
                    commit;
                """)
            )



        for l1 in stock_data:

            company_data = l1.get_company_summary()

            stock_id = 0

            stock_result = conn.execute(
                text(f"""
                    select id from companies where symbol = upper('{company_data['symbol']}');
                """)
            )
            
            for row in stock_result:
                stock_id = row[0]

            if stock_id == 0:

                company_data['data']['company_summary'].to_sql(
                    'companies',
                    con=connection,
                    if_exists='append',
                    index=False
                )

                stock_result = conn.execute(
                    text(f"""
                        select id from companies where symbol = upper('{company_data['symbol']}')
                    """)
                )
                
                for row in stock_result:
                    stock_id = row[0]
                
            df_data = l1.get_monthly()['data']

            for l2 in df_data.keys():

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', upper('{company_data['symbol']}'), timestamp '{str(datetime.today())}', 'Load Start');
                        commit;
                    """)
                )

                processed_data = df_data[l2]

                processed_data.insert(
                    0, 'batch_id', batch_id
                )

                processed_data.insert(
                    1, 'stock_id', stock_id
                )
                
                processed_data.to_sql(
                    l2,
                    con=connection,
                    if_exists='append',
                    index=False
                )

                conn.execute(
                    text(
                        f"""
                            alter table {l2}
                            add constraint {l2}_bfk foreign key(batch_id)
                            references data_load_batches(batch_id),
                            add constraint {l2}_sfk foreign key(stock_id)
                            references companies(id);
                            commit;
                        """
                    )
                )

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', upper('{company_data['symbol']}'), timestamp '{str(datetime.today())}', 'Load Complete');
                        commit;
                    """)
                )

                stock_status_return[company_data['symbol']] = list(df_data.keys())

        conn.execute(
            text(f"""
                update data_load_batches
                set end_date = timestamp '{str(datetime.today())}',
                    status = 'Load Complete'
                where batch_id = {batch_id};
                commit;
            """)
        )

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Finished');
                commit;
            """)
        )

        conn.execute(text('commit;'))

        return {
            'status_code': '200',
            'status_string': 'Success',
            'non_stock_data_loaded': list(non_stock_df_data),
            'stock_data_loaded': stock_status_return
        }

# 2 ------------------------------------------------------------------------------------------------

def create_quarterly_tables(non_stock_data, stock_data:list, connection) -> dict:

    stock_status_return = dict()

    with connection.connect() as conn:

        conn.execute(
            text(f"""
                insert into data_load_batches(start_date, data, status)
                            values(timestamp '{str(datetime.today())}', 'quarterly_data', 'Load Start');
                commit;
            """)
        )


        batch_result = conn.execute(
            text('select max(batch_id) from data_load_batches')
        )

        batch_id = 0

        for row in batch_result:

            batch_id = row[0]

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Initated');
                commit;
            """)
        )

        

        non_stock_df_data = non_stock_data.get_quarterly()['data']

        for l1 in non_stock_df_data.keys():

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Start');
                    commit;
                """)
            )

            processed_data = non_stock_df_data[l1]

            processed_data.insert(
                0, 'batch_id', batch_id
            )

            processed_data.to_sql(
                l1,
                con=connection,
                if_exists='append',
                index=False
            )

            conn.execute(
                text(
                    f"""
                        alter table {l1}
                        add constraint {l1}_bfk foreign key(batch_id)
                        references data_load_batches(batch_id);
                        commit;
                    """
                )
            )

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Complete');
                    commit;
                """)
            )



        for l1 in stock_data:

            company_data = l1.get_company_summary()

            stock_id = 0

            stock_result = conn.execute(
                text(f"""
                    select id from companies where symbol = upper('{company_data['symbol']}')
                """)
            )

            for row in stock_result:
                stock_id = row[0]

            if stock_id == 0:

                company_data['data']['company_summary'].to_sql(
                    'companies',
                    con=connection,
                    if_exists='append',
                    index=False
                )

                stock_result = conn.execute(
                    text(f"""
                        select id from companies where symbol = upper('{company_data['symbol']}')
                    """)
                )

                for row in stock_result:
                    stock_id = row[0]

            df_data = l1.get_quarterly()['data']

            for l2 in df_data.keys():

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', upper('{company_data['symbol']}'), timestamp '{str(datetime.today())}', 'Load Start');
                        commit;
                    """)
                )

                processed_data = df_data[l2]

                processed_data.insert(
                    0, 'batch_id', batch_id
                )

                processed_data.insert(
                    1, 'stock_id', stock_id
                )

                processed_data.to_sql(
                    l2,
                    con=connection,
                    if_exists='append',
                    index=False
                )

                conn.execute(
                    text(
                        f"""
                            alter table {l2}
                            add constraint {l2}_bfk foreign key(batch_id)
                            references data_load_batches(batch_id),
                            add constraint {l2}_sfk foreign key(stock_id)
                            references companies(id);
                            commit;
                        """
                    )
                )

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', upper('{company_data['symbol']}'), timestamp '{str(datetime.today())}', 'Load Complete');
                        commit;
                    """)
                )

                stock_status_return[company_data['symbol']] = list(df_data.keys())

        conn.execute(
            text(f"""
                update data_load_batches
                set end_date = timestamp '{str(datetime.today())}',
                    status = 'Load Complete'
                where batch_id = {batch_id};
                commit;
            """)
        )

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Finished');
                commit;
            """)
        )

        conn.execute(text('commit;'))

        return {
            'status_code': '200',
            'status_string': 'Success',
            'non_stock_data_loaded': list(non_stock_df_data),
            'stock_data_loaded': stock_status_return
        }

# 3 ------------------------------------------------------------------------------------------------

def create_yearly_tables(non_stock_data, stock_data:list, connection) -> dict:

    stock_status_return = dict()

    with connection.connect() as conn:

        conn.execute(
            text(f"""
                insert into data_load_batches(start_date, data, status)
                            values(timestamp '{str(datetime.today())}', 'yearly_data', 'Load Start');
                commit;
            """)
        )


        batch_result = conn.execute(
            text('select max(batch_id) from data_load_batches')
        )

        batch_id = 0

        for row in batch_result:

            batch_id = row[0]

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Initated');
                commit;
            """)
        )

        

        non_stock_df_data = non_stock_data.get_yearly()['data']

        for l1 in non_stock_df_data.keys():

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Start');
                    commit;
                """)
            )

            processed_data = non_stock_df_data[l1]

            processed_data.insert(
                0, 'batch_id', batch_id
            )

            processed_data.to_sql(
                l1,
                con=connection,
                if_exists='append',
                index=False
            )

            conn.execute(
                text(
                    f"""
                        alter table {l1}
                        add constraint {l1}_bfk foreign key(batch_id)
                        references data_load_batches(batch_id);
                        commit;
                    """
                )
            )

            conn.execute(
                text(f"""
                    insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                    values({batch_id}, '{l1}', 'None', timestamp '{str(datetime.today())}', 'Load Complete');
                    commit;
                """)
            )



        for l1 in stock_data:

            company_data = l1.get_company_summary()

            stock_id = 0

            stock_result = conn.execute(
                text(f"""
                    select id from companies where symbol = upper('{company_data['symbol']}')
                """)
            )

            for row in stock_result:
                stock_id = row[0]

            if stock_id == 0:

                company_data['data']['company_summary'].to_sql(
                    'companies',
                    con=connection,
                    if_exists='append',
                    index=False
                )

                stock_result = conn.execute(
                    text(f"""
                        select id from companies where symbol = upper('{company_data['symbol']}')
                    """)
                )

                for row in stock_result:
                    stock_id = row[0]

            df_data = l1.get_yearly()['data']

            for l2 in df_data.keys():

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', upper('{company_data['symbol']}'), timestamp '{str(datetime.today())}', 'Load Start');
                        commit;
                    """)
                )

                processed_data = df_data[l2]

                processed_data.insert(
                    0, 'batch_id', batch_id
                )

                processed_data.insert(
                    1, 'stock_id', stock_id
                )

                processed_data.to_sql(
                    l2,
                    con=connection,
                    if_exists='append',
                    index=False
                )

                conn.execute(
                    text(
                        f"""
                            alter table {l2}
                            add constraint {l2}_bfk foreign key(batch_id)
                            references data_load_batches(batch_id),
                            add constraint {l2}_sfk foreign key(stock_id)
                            references companies(id);
                            commit;
                        """
                    )
                )

                conn.execute(
                    text(f"""
                        insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                        values({batch_id}, '{l2}', upper('{company_data['symbol']}'), timestamp '{str(datetime.today())}', 'Load Complete');
                        commit;
                    """)
                )

                stock_status_return[company_data['symbol']] = list(df_data.keys())

        conn.execute(
            text(f"""
                update data_load_batches
                set end_date = timestamp '{str(datetime.today())}',
                    status = 'Load Complete'
                where batch_id = {batch_id};
                commit;
            """)
        )

        conn.execute(
            text(f"""
                insert into load_log(batch_id, table_name, stock_symbol, effective_date, status)
                values({batch_id}, 'data_load_batches', 'None', timestamp '{str(datetime.today())}', 'Load Finished');
                commit;
            """)
        )

        conn.execute(text('commit;'))

        return {
            'status_code': '200',
            'status_string': 'Success',
            'non_stock_data_loaded': list(non_stock_df_data),
            'stock_data_loaded': stock_status_return
        }

def create_view(data, connection) -> str:

    columns_list = list()

    tables_list = list()

    filters_list = list()

    with connection.connect() as conn:

        def first_letter(str) -> str:
            return str[0]
        
        res = conn.execute(text('SELECT current_database();'))
        database_name = list(res)[0][0]

        for table_name in data['tables']:

            tables_list.append(table_name + ' ' + ''.join(list(map(first_letter, table_name.split('_')))))

            for row in pd.read_sql(
                f"""
                    select column_name from INFORMATION_SCHEMA.columns tbl
                    where table_catalog = '{database_name}'
                    and table_name = '{table_name}'
                    order by ordinal_position;
                """,
                conn
            ).iterrows():
                
                column_name = row[1]['column_name']

                if column_name == 'id':
                    filters_list.append(
                        ''.join(list(map(first_letter, data['tables'][1].split('_')))) + '.stock_id' + 
                        ' = ' + ''.join(list(map(first_letter, table_name.split('_')))) + '.' + column_name
                    )
                    columns_list.append(''.join(list(map(first_letter, table_name.split('_')))) + '.' + column_name + ' as stock_id')
                elif column_name in ['stock_id', 'period', 'batch_id'] and 'stock' not in table_name:
                    filters_list.append(
                        ''.join(list(map(first_letter, data['tables'][1].split('_')))) + '.' + column_name + ' = ' +
                        ''.join(list(map(first_letter, table_name.split('_')))) + '.' + column_name
                    )
                    pass
                elif column_name == 'stock_id' and 'stock' in table_name or column_name == 'for_grouping':
                    pass
                else:
                    if any(word in table_name for word in ['currency', 'lending', 'treasury']):
                        columns_list.append(
                            ''.join(list(map(first_letter, table_name.split('_')))) + '.' + column_name + ' as ' + 
                            ''.join(list(map(first_letter, table_name.split('_')))) + '_' + column_name
                        )
                    else:
                        columns_list.append(''.join(list(map(first_letter, table_name.split('_')))) + '.' + column_name)

        
        data_name = data['data_name']

        conn.execute(
            text(
                f'create view {data_name} as\n'
                'select' + '\n' +
                ',\n'.join(columns_list) + '\nfrom\n' +
                ',\n'.join(tables_list) + '\nwhere\n' +
                ' and \n'.join(filters_list)
                + ';\n' + 'commit;'
            )
        )

        return f'View "{data_name}" was successfuly created.'

# 4 ------------------------------------------------------------------------------------------------

class ClearDB:

    def __init__(self, connection):

        self.engine = connection

    def drop_base_tables(self):

        with self.engine.connect() as con:
            
            con.execute(text(
                """
                    drop table load_log;
                    drop table companies;
                    drop table data_load_batches;
                    drop table load_queue;
                    commit;
                """
            ))

        return 'Base tables were successfully dropped.'
    
    def drop_monthly_tables(self):

        with self.engine.connect() as con:
            
            con.execute(text(
                """
                    drop table monthly_stock_quotes;
                    drop table monthly_currency;
                    drop table monthly_lending;
                    drop table monthly_treasury;
                    commit;
                """
            ))

        return 'Monthly tables were successfully dropped.'
    
    def drop_quarterly_tables(self):

        with self.engine.connect() as con:
            
            con.execute(text(
                """
                    drop table quarterly_stock_quotes;
                    drop table quarterly_currency;
                    drop table quarterly_lending;
                    drop table quarterly_treasury;
                    drop table quarterly_balance_sheet;
                    drop table quarterly_income_statement;
                    drop table quarterly_cash_flow_statement;
                    drop table quarterly_financial_ratios;
                    commit;
                """
            ))

        return 'Quarterly tables were successfully dropped.'
    
    def drop_yearly_tables(self):

        with self.engine.connect() as con:
            
            con.execute(text(
                """
                    drop table yearly_stock_quotes;
                    drop table yearly_currency;
                    drop table yearly_lending;
                    drop table yearly_treasury;
                    drop table yearly_balance_sheet;
                    drop table yearly_income_statement;
                    drop table yearly_cash_flow_statement;
                    drop table yearly_financial_ratios;
                    commit;
                """
            ))

        return 'Yearly tables were successfully dropped.'
    
    def drop_functions(self):

        with self.engine.connect() as con:
            
            con.execute(
                text(
                    """
                        drop function add_to_queue;
                        drop function remove_from_queue;
                        commit;
                    """
                )
            )

        return 'Functions successfully dropped.'

    def drop_views(self):

        with self.engine.connect() as con:

            con.execute(
                text(
                    """
                        drop view monthly_data;
                        drop view quarterly_data;
                        drop view yearly_data;
                        commit;
                    """
                )
            )

        return 'Views successfully dropped.'
    
    def truncate_base_tables(self):

        with self.engine.connect() as con:

            con.execute(text(
                """
                    TRUNCATE table load_log RESTART IDENTITY CASCADE;
                    TRUNCATE table companies RESTART IDENTITY CASCADE;
                    TRUNCATE table data_load_batches RESTART IDENTITY CASCADE;
                    commit;
                """
            ))

        return 'Base tables were successfully truncated.'
    
    def truncate_monthly_tables(self):

        with self.engine.connect() as con:
            
            con.execute(text(
                """
                    TRUNCATE table monthly_stock_quotes RESTART IDENTITY;
                    TRUNCATE table monthly_currency RESTART IDENTITY;
                    TRUNCATE table monthly_lending RESTART IDENTITY;
                    TRUNCATE table monthly_treasury RESTART IDENTITY;
                    commit;
                """
            ))

        return 'Monthly tables were successfully truncated.'
    
    def truncate_quarterly_tables(self):

        with self.engine.connect() as con:
            
            con.execute(text(
                """
                    TRUNCATE table quarterly_stock_quotes RESTART IDENTITY;
                    TRUNCATE table quarterly_currency RESTART IDENTITY;
                    TRUNCATE table quarterly_lending RESTART IDENTITY;
                    TRUNCATE table quarterly_treasury RESTART IDENTITY;
                    TRUNCATE table quarterly_balance_sheet RESTART IDENTITY;
                    TRUNCATE table quarterly_income_statement RESTART IDENTITY;
                    TRUNCATE table quarterly_cash_flow_statement RESTART IDENTITY;
                    TRUNCATE table quarterly_financial_ratios RESTART IDENTITY;
                    commit;
                """
            ))

        return 'Quarterly tables were successfully truncated.'
    
    def truncate_yearly_tables(self):

        with self.engine.connect() as con:
            
            con.execute(text(
                """
                    TRUNCATE table yearly_stock_quotes RESTART IDENTITY;
                    TRUNCATE table yearly_currency RESTART IDENTITY;
                    TRUNCATE table yearly_lending RESTART IDENTITY;
                    TRUNCATE table yearly_treasury RESTART IDENTITY;
                    TRUNCATE table yearly_balance_sheet RESTART IDENTITY;
                    TRUNCATE table yearly_income_statement RESTART IDENTITY;
                    TRUNCATE table yearly_cash_flow_statement RESTART IDENTITY;
                    TRUNCATE table yearly_financial_ratios RESTART IDENTITY;
                    commit;
                """
            ))

        return 'Yearly tables were successfully truncated.'

# 5 ------------------------------------------------------------------------------------------------

def create_database_functions(connection) -> str:

    with connection.connect() as conn:

        conn.execute(text("""
            create or replace function add_to_queue(inserting_value varchar)
                returns varchar
                language plpgsql
            as
            $$
            begin
                if (
                    select count(1) from load_queue where stock_symbol = upper(inserting_value)
                ) >= 1 then
                    return 'Entered stock symbol is already in load queue.';
                elsif (select count(1) from load_queue) >= 5 then
                    return 'Load queue is full.';
                else
                    insert into load_queue(stock_symbol) values(upper(inserting_value));
                    return 'Stock symbol added to load queue.';
                end if;
            commit;
            end;
            $$;
        """))

        conn.execute(text("""
            create or replace function remove_from_queue(removing_value varchar)
                returns varchar
                language plpgsql
            as
            $$
            declare
                result varchar;
            begin
                if (
                    select count(1) from load_queue where stock_symbol = upper(removing_value)
                ) >= 1 then
                    delete from load_queue where stock_symbol = upper(removing_value);
                    return removing_value || ' was deleted from load queue';
                end if;
                return 'Entered stock symbol does not exist in load queue';
            commit;
            end;
            $$;
        """))

        conn.execute(text('commit;'))

        return 'Database functions created'

# 6 ------------------------------------------------------------------------------------------------

def init_database(connection) -> str:

    try:

        with connection.connect() as conn:

            conn.execute(text(
                'select * from load_log;'
            ))

            return 'Tables are already created'
        
    except:

        with open(f'{root_folder}/reference_data.txt', 'r') as file:

            raw_data = file.read()

            non_stock_data = d.SampleNonStockData(json.loads(raw_data)['non_stock_data'])

            stock_data = d.SampleStockData(json.loads(raw_data))

            data_to_view = [
                {
                    'data_name': 'monthly_data',
                    'tables': [
                        'companies',
                        'monthly_stock_quotes',
                        'monthly_currency',
                        'monthly_lending',
                        'monthly_treasury'
                    ]
                },
                {
                    'data_name': 'quarterly_data',
                    'tables': [
                        'companies',
                        'quarterly_stock_quotes',
                        'quarterly_currency',
                        'quarterly_lending',
                        'quarterly_treasury',
                        'quarterly_financial_ratios',
                        'quarterly_income_statement',
                        'quarterly_cash_flow_statement',
                        'quarterly_balance_sheet'           
                    ]
                },
                {
                    'data_name': 'yearly_data',
                    'tables': [
                        'companies',
                        'yearly_stock_quotes',
                        'yearly_currency',
                        'yearly_lending',
                        'yearly_treasury',
                        'yearly_financial_ratios',
                        'yearly_income_statement',
                        'yearly_cash_flow_statement',
                        'yearly_balance_sheet'
                    ]
                }
            ]

            DB_Cleaner = ClearDB(connection)

            create_base_tables(connection)

            create_monthly_tables(non_stock_data, [stock_data], connection)

            create_quarterly_tables(non_stock_data, [stock_data], connection)

            create_yearly_tables(non_stock_data, [stock_data], connection)

            create_database_functions(connection)

            DB_Cleaner.truncate_monthly_tables()

            DB_Cleaner.truncate_quarterly_tables()

            DB_Cleaner.truncate_yearly_tables()

            DB_Cleaner.truncate_base_tables()

            for data in data_to_view:

                create_view(data, connection)

            with connection.connect() as conn:
                conn.execute(text('commit;'))

        return 'Tables were successfuly created.'



