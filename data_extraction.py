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
    
    @staticmethod
    def list_number_of_stores(endpoint, headers):

        response = requests.get(endpoint, headers=headers)
        return int(response.text[37:40])

    @staticmethod
    def retrieve_stores_data(endpoint, headers):

        # Create an empty list to hold the store data
        store_data = []

        # Loop over all possible store numbers
        for store_number in range(0, 451):
            
            # Construct the store endpoint URL using the current store number
            url = endpoint.format(store_number=store_number)

            # Make a request to the store endpoint
            response = requests.get(url, headers=headers)

            # Extract the store data from the response JSON
            store_json = response.json()

            # Append the store data to the store_data list
            store_data.append(store_json)

        # Convert the store data to a pandas DataFrame
        store_data_df = pd.DataFrame(store_data)

        # Return the DataFrame
        return store_data_df




db_connector = DatabaseConnector()
db_extractor = DataExtractor()


# users_df = db_extractor.read_rds_table(table='legacy_users', engine = db_connector.engine)
# cleaned_users_df = DataCleaning.clean_the_user_data(users_df)
# db_connector.upload_to_db(cleaned_users_df, 'dim_users')


# card_details_df = db_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
# cleaned_users_card_details_df = DataCleaning.clean_card_data(card_details_df)
# db_connector.upload_to_db(cleaned_users_card_details_df, 'dim_card_details')


# headers={'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
# stores_data = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
# store_data = retrieve_stores_data(stores_data, headers)
# cleaned_store_data = DataCleaning.clean_store_data(store_data)
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