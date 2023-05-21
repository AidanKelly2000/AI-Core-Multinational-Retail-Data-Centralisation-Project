from data_cleaning import DataCleaning
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import psycopg2
import requests
import yaml


class DatabaseConnector():

    def __init__(self):

        self.db_creds = self.read_db_creds()
        self.engine = self.init_db_engine()
        
    def read_db_creds(self):

        with open('db_creds.yaml') as f:
            database = yaml.safe_load(f)

        return database

    def init_db_engine(self):

        connector = f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
        engine = create_engine(connector)
        return engine
        
    def list_db_tables(self):

        data = self.init_db_engine()
        data.connect()
        inspector = inspect(data)
        print(inspector.get_table_names())
    
        
    def upload_to_db(self, df, table_name):

        host = "localhost",
        user = "postgres",
        dbname = "Sales_Data",
        password = "",
        port = 5432

        with open('db_local_creds.yaml') as f:
            creds = yaml.safe_load(f)

        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}")
        engine.connect()
        df.to_sql(table_name, engine, if_exists='replace')


# db_creds.init_db_engine()
# db_creds.list_db_tables()


# db_connector = DatabaseConnector()
# db_connector.list_db_tables()


# cleaned_users_df = db_cleaner.clean_the_user_data(table='legacy_users')
# db_connector.upload_to_db(cleaned_users_df, 'dim_users')


# return_the_number_of_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
# db_connector.list_number_of_stores(return_the_number_of_stores, headers)

