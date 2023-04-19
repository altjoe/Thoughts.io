import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, DeclarativeBase
import psycopg2
from thoughts.mytools.db.db_tunnel import tunnel


def create_postgres_engine(username, password, hostname, port, dbname):
    engine = create_engine(
        'postgresql://{}:{}@{}:{}/{}'.format(username, password, hostname, port, dbname))

    return engine


def session_test():

    cmd = text('select * from xlmusd_trade limit 1000')
    with Session(engine) as session:
        print('Connected: {}'.format(session.bind))
        result = session.execute(cmd)
        keys = result.keys()
        print(keys)
        # for row in result:
        #     print(row)

        pass

class Base(DeclarativeBase):
    pass

def main():
    engine = create_postgres_engine(
        'alterejo_server', r'o7E*d%iE2wwr@rgQkGnasrCza5z#FCTL^KuNz5#&ZnfSW@%dM%', 'localhost', '5432', 'alterejo_trading')

    with Session(engine) as session:
        print('Connected: {}'.format(session.bind))
        pass

    base = Base()
    print(base.metadata.tables)


tunnel(main)
