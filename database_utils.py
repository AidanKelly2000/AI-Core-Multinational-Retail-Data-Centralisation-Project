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

        # conn = psycopg2.connect(
        host = "localhost",
        user = "postgres",
        dbname = "Sales_Data",
        password = "",
        port = 5432
        # )
        with open('db_local_creds.yaml') as f:
            creds = yaml.safe_load(f)

        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}")
        engine.connect()
        df.to_sql(table_name, engine, if_exists='replace')
        # cursor = conn.cursor()
        # cursor.execute(f"CREATE TABLE {table_name} ();")
        # cleaned_users_df.to_sql(table_name, conn, if_exists='replace', index=False)

        

        # Commit changes and close connection

    @staticmethod
    def list_number_of_stores(endpoint, headers):

        response = requests.get(endpoint, headers=headers)
        return int(response.text[37:40])

    @staticmethod
    def retrieve_stores_data(endpoint, headers):

        # Create an empty list to hold the store data
        store_data = []

        # Loop over all possible store numbers (1-999)
        for store_number in range(0, 451):
            # Construct the store endpoint URL using the current store number
            url = endpoint.format(store_number=store_number)

            # Make a request to the store endpoint
            response = requests.get(url, headers=headers)

            # If the response code is not 200 (OK), skip this store
            # if response.status_code != 200:
            #     continue

            # Extract the store data from the response JSON
            store_json = response.json()

            # Append the store data to the store_data list
            store_data.append(store_json)

        # Convert the store data to a pandas DataFrame
        store_data_df = pd.DataFrame(store_data)

        # Return the DataFrame
        return store_data_df



# db_creds.init_db_engine()
# db_creds.list_db_tables()


# db_connector = DatabaseConnector()
# db_connector.list_db_tables()


# cleaned_users_df = db_cleaner.clean_the_user_data(table='legacy_users')
# db_connector.upload_to_db(cleaned_users_df, 'dim_users')


# return_the_number_of_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
# db_connector.list_number_of_stores(return_the_number_of_stores, headers)

