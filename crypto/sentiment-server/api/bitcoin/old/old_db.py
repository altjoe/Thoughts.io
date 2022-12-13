from sshtunnel import SSHTunnelForwarder
from datetime import datetime
import time
import pandas as pd
import psycopg2
import threading


class database:
    con = None
    cur = None

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __init__(self):
        self.con = psycopg2.connect(host='localhost', database='alterejo_dailycoin_bitcoin', port=5432,
                                    user='alterejo', password='04x.IN)Mp6du1D')
        self.cur = self.con.cursor()

    def close(self):
        self.cur.close()
        self.con.close()

    def clear_conn(self):
        self.run_file('clear_conn.sql')

    def select_df(self, command, index=None):
        return pd.read_sql_query(command, self.con, index_col=index)

    def select_df_file(self, file, index=None):
        return pd.read_sql_query(open(file, 'r').read(), self.con, index_col=index)

    def query(self, command):
        self.cur.execute(command)
        self.con.commit()

    def query_without_commit(self, command):
        self.cur.execute(command)
    
    def commit(self):
        self.con.commit()

    def get_all_table_info(self):
        return self.select_df('select * from information_schema.tables where table_schema = \'public\'')

    def get_all_column_info(self):
        return self.select_df('select * from information_schema.columns')

    def fetch(self):
        return self.cur.fetchall()

    def fetch_one(self):
        return self.cur.fetchone()[0]

    def run_file(self, filename):
        with open(filename, 'r') as file:
            self.cur.execute(file.read())
            self.con.commit()

    def df_to_table(self, df : pd.DataFrame, table_name, primary_key):
        df_postgres = {'object':'text','int64':'bigint','float64':'numeric','bool':'boolean','datetime64':'timestamp','timedelta':'interval'}

        #create table
        if (primary_key not in df.columns): raise Exception(f'Primary key {primary_key} is not in df columns {list(df.columns)}')
        command = f'create table if not exists {table_name} ('
        for col in df.columns:
            if str(df.dtypes[col]) in df_postgres:
                if primary_key is not col: 
                    command += f'{col} {df_postgres[str(df.dtypes[col])]}, '
                else:
                    command += f'{col} {df_postgres[str(df.dtypes[col])]} primary key, '
            else:
                raise Exception(f'Cannot convert type {col}')
        command = command[:-2] + ')'
        self.query(command)

        self.insert_df(df, table_name)

    def insert_df(self, df : pd.DataFrame, table_name):
        #insert data
        command = f'insert into {table_name} ('
        for col in df.columns:
            command += f'{col}, '
        command = command[:-2] + ') values '

        for i, t in enumerate(df.itertuples(), 1):

            tup = t[1:]
            if len(tup) > 1:
                command += f'{tup}, '
            else:
                string = str(tup)[1: -2]
                command += f'({string}), '

        command = command[:-2] + ' ON CONFLICT DO NOTHING'
        self.query(command)

