import pretty_errors
import time
from sqlalchemy import create_engine
import psycopg2
from thoughts.mytools.db.db_tunnel import tunnel


def create_postgres_engine(username, password, hostname, port, dbname):
    engine = create_engine(
        'postgresql://{}:{}@{}:{}/{}'.format(username, password, hostname, port, dbname))


    return engine


# with engine.connect() as connection:
#     print('Connected to database')

def main():
    engine = create_postgres_engine(
        'alterejo_sampleuser', 'oEr6Ein3V2VyvtbPoymfvBde6', 'localhost', '5432', 'alterejo_trading')

    with engine.connect() as connection:
        print('Connected to database')
        time.sleep(5)


if __name__ == '__main__':
    tunnel(main)
