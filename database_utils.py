from sqlalchemy import create_engine
from sqlalchemy import inspect
import psycopg2
import yaml


class DatabaseConnector():

    def __init__(self):
        pass

    def read_db_creds(self):

        with open('db_creds.yaml') as f:
            database = yaml.safe_load(f)

        return database

    def init_db_engine(self):

        db_creds = self.read_db_creds()
        # with psycopg2.connect(host='RDS_HOST', user='RDS_USER', password='RDS_PASSWORD', dbname='RDS_DATABASE', port='RDS_PORT')
        # DBAPI = 'psycopg2'
        # DATABASE_TYPE = 'postgresql'
        # engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        connector = f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        engine = create_engine(connector)
        return engine
        
    def list_db_tables(self):

        data = self.init_db_engine()
        connection = data.connect()
        inspector = inspect(data)
        table_names = inspector.get_table_names()
        for table_name in table_names:
            print(table_name)
        connection.close()


db_creds = DatabaseConnector()
db_creds.list_db_tables()