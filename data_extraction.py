from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import boto3
import pandas as pd
import requests
import tabula

class DataExtractor():

    def __init__(self):
        pass

    def read_rds_table(self, engine, table):

        df = pd.read_sql_table(table, engine)
        return df
    
    @staticmethod
    def retrieve_pdf_data(pdf_path):
        
        df = tabula.read_pdf(pdf_path, multiple_tables=False, pages='all', stream=True)
        users_card_details_df = df[0]
        return users_card_details_df
    
    @staticmethod
    def extract_from_s3(s3_address):
        """
        Extracts the data from the specified S3 address and returns a pandas DataFrame.
        
        Args:
        s3_address (str): The S3 bucket address where the products data is stored.
        
        Returns:
        pandas DataFrame: The extracted products data.
        """
        # Set up the S3 client
        s3 = boto3.client('s3')

        # Retrieve the object
        s3_bucket, s3_key = s3_address.split('/', 3)[-2:]
        obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)

        # Read the object data into a pandas DataFrame

        products_df = pd.read_csv(obj['Body'], index_col=0)

        return products_df
    

    @staticmethod
    def extract_json_from_s3(s3_address):

        response = requests.get(s3_address)
        data = response.json()
        dates_and_times = pd.DataFrame(data)

        return dates_and_times




db_connector = DatabaseConnector()
db_extractor = DataExtractor()


# users_df = db_extractor.read_rds_table(table='legacy_users', engine = db_connector.engine)
# cleaned_users_df = DataCleaning.clean_the_user_data(users_df)
# db_connector.upload_to_db(cleaned_users_df, 'dim_users')


# card_details_df = db_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
# cleaned_users_card_details_df = DataCleaning.clean_card_data(card_details_df)
# db_connector.upload_to_db(cleaned_users_card_details_df, 'dim_card_details')


headers={'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
stores_data = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
store_data = db_connector.retrieve_stores_data(stores_data, headers)
cleaned_store_data = DataCleaning.clean_store_data(store_data)
# db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')


# products_df = db_extractor.extract_from_s3("s3://data-handling-public/products.csv")
# converted_weights = DataCleaning.convert_product_weights(products_df)
# db_connector.upload_to_db(converted_weights, 'dim_products')


# orders_df = db_extractor.read_rds_table(table='orders_table', engine=db_connector.engine)
# clean_orders_table = DataCleaning.clean_orders_data(orders_df)
# db_connector.upload_to_db(clean_orders_table, 'orders_table')

# url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
# datetime_df = db_extractor.extract_json_from_s3(url)
# cleaned_datetime = DataCleaning.clean_datetime_df(datetime_df)
# db_connector.upload_to_db(cleaned_datetime, 'dim_date_times')