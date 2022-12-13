import psycopg2
import sqlalchemy
import pandas as pd

class Database:
    def __init__(self):
        connstring = 'postgresql://alterejo:04x.IN)Mp6du1D@localhost:5432/alterejo_dailycoin_bitcoin'
        self.con = psycopg2.connect(connstring)

        self.engine = sqlalchemy.create_engine(connstring)
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def query(self, command):
        self.cur = self.con.cursor()
        self.cur.execute(command)
        self.con.commit()

    def close(self):
        self.con.close()

    def fetch(self):
        fetch = self.cur.fetchall()
        self.cur.close()
        return fetch

    def fetch_one(self):
        fetch = self.cur.fetchone()[0]
        self.cur.close()
        return fetch

    def clear_conn(self):
        self.run_file('clear_conn.sql')

    def run_file(self, filename):
        with open(filename, 'r') as file:
            self.cur = self.con.cursor()
            self.cur.execute(file.read())
            self.con.commit()
        self.cur.close()

    def select_df(self, command, index=None):
        return pd.read_sql_query(command, self.con, index_col=index)

    def create_table_df(self, df : pd.DataFrame, tablename):
        df.to_sql(tablename, self.engine, if_exists='replace', index=False)

    def insert_df(self, df : pd.DataFrame, tablename):
        df.to_sql(tablename, self.engine, if_exists='append', index=False)

    def get_all_table_info(self):
        return self.select_df('select * from information_schema.tables where table_schema = \'public\'')