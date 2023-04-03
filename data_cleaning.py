import pandas as pd

class DataCleaning():

    def __init__(self):
        pass
      
        
    @staticmethod
    def clean_the_user_data(table):
        
        cleaned_users_df = table
        # perform cleaning operations on the dataframe here
        # cleaned_users_df.info()
        cleaned_users_df['country_code'] = cleaned_users_df['country_code'].astype('category')
        # cleaned_users_df.info()
        
        cleaned_users_df['date_of_birth'] = pd.to_datetime(cleaned_users_df['date_of_birth'], infer_datetime_format=True, errors='coerce')
        cleaned_users_df['join_date'] = pd.to_datetime(cleaned_users_df['join_date'], infer_datetime_format=True, errors='coerce')
        cleaned_users_df.dropna(how='all', inplace=True)

        # create a regex pattern to match rows with random letters and numbers
        pattern = r'^[a-zA-Z0-9]*$'

        # create a boolean mask for missing or random values
        mask = (cleaned_users_df['date_of_birth'].isna()) | (cleaned_users_df['date_of_birth'].astype(str).str.contains(pattern))

        # drop the rows with missing or random values
        cleaned_users_df = cleaned_users_df[~mask]

        pd.set_option('display.max_columns', None)
        
        return cleaned_users_df
    
    @staticmethod
    def clean_card_data(table):

        cleaned_users_card_details_df = table

        cleaned_users_card_details_df.dropna(how='all', inplace=True)
        cleaned_users_card_details_df['date_payment_confirmed'] = pd.to_datetime(cleaned_users_card_details_df['date_payment_confirmed'], infer_datetime_format=True, errors='coerce')

        # create a regex pattern to match rows with random letters and numbers
        pattern = r'^[a-zA-Z0-9]*$'

        # create a boolean mask for missing or random values
        mask = (cleaned_users_card_details_df['date_payment_confirmed'].isna()) | (cleaned_users_card_details_df['date_payment_confirmed'].astype(str).str.contains(pattern))

        # drop the rows with missing or random values
        cleaned_users_card_details_df = cleaned_users_card_details_df[~mask]

        # Define a regular expression that matches all non-numeric characters
        pattern = r'[^0-9]'

        # Use the replace() method to remove all non-numeric characters from the column
        cleaned_users_card_details_df['card_number'] = cleaned_users_card_details_df['card_number'].replace(pattern, '', regex=True)

        print("Longest length is:", cleaned_users_card_details_df['card_number'].astype(str).str.len().max())
        # print("Longest length is:\n", cleaned_users_card_details_df.expiry_date.str.len().max())
        return cleaned_users_card_details_df
    
    @staticmethod
    def clean_store_data(table):

        cleaned_store_data = table
        
        cleaned_store_data.dropna(how='all', inplace=True)
        cleaned_store_data['opening_date'] = pd.to_datetime(cleaned_store_data['opening_date'], infer_datetime_format=True, errors='coerce')

        # create a regex pattern to match rows with random letters and numbers
        pattern = r'^[a-zA-Z0-9]*$'

        # create a boolean mask for missing or random values
        mask = (cleaned_store_data['opening_date'].isna()) | (cleaned_store_data['opening_date'].astype(str).str.contains(pattern))

        # drop the rows with missing or random values
        cleaned_store_data = cleaned_store_data[~mask]
        
        return cleaned_store_data
    
    @staticmethod
    def convert_product_weights(table):

        converted_product_weights = table

        converted_product_weights.dropna(how='all', inplace=True)
        converted_product_weights['date_added'] = pd.to_datetime(converted_product_weights['date_added'], infer_datetime_format=True, errors='coerce')

        # create a regex pattern to match rows with random letters and numbers
        pattern = r'^[a-zA-Z0-9]*$'

        # create a boolean mask for missing or random values
        mask = (converted_product_weights['date_added'].isna()) | (converted_product_weights['date_added'].astype(str).str.contains(pattern))

        # drop the rows with missing or random values
        converted_product_weights = converted_product_weights[~mask]

        new_units = []
        for w in converted_product_weights['weight']:
            w = str(w).strip('.')
            w = str(w).strip('l')
            w = str(w).strip()  # remove any leading/trailing spaces
            if w.endswith('kg'):
                new_units.append(float(w[:-2]))
            elif 'x' in w:
                num, unit = w.split('x')
                new_units.append(float(num)*float(unit.rstrip('g'))/1000)
            elif w.endswith('g'):
                new_units.append(float(w[:-1])/1000)
            elif w.endswith('oz'):
                new_units.append(float(w[:-2])*0.0283)
            elif w.endswith('m'):
                new_units.append(float(w[:-1])/1000)
            else:
                new_units.append(float(w))
        converted_product_weights['weight_class'] = [ 'Light' if x <= 2
                                else 'Mid_Sized' if x >2 and x<= 40 
                                else 'Heavy' if x>40 and x<=140
                                else 'Truck_Required'
                        for x in new_units]

        converted_product_weights.loc[:, 'weight'] = new_units
        return converted_product_weights
    
    @staticmethod
    def clean_orders_data(table):

        cleaned_orders_df = table
        cleaned_orders_df.drop(['level_0', 'first_name', 'last_name', '1'], axis=1, inplace=True)
        # Returning the longest length
        print(cleaned_orders_df.loc[[144]])
        return cleaned_orders_df
    
    @staticmethod
    def clean_datetime_df(table):

        cleaned_datetime_df = table
        cleaned_datetime_df.dropna(how='all', inplace=True)

        pattern = r'^[a-zA-Z0-9]*$'

        # create a boolean mask for missing or random values
        mask = (cleaned_datetime_df['timestamp'].isna()) | (cleaned_datetime_df['timestamp'].astype(str).str.contains(pattern))
        

        # drop the rows with missing or random values
        cleaned_datetime_df = cleaned_datetime_df[~mask]
        return cleaned_datetime_df
    



# print("Longest length is:", cleaned_orders_df['product_quantity'].astype(str).str.len().max())
# print("Longest length is:\n", cleaned_users_df.country_code.str.len().max())
# print(cleaned_users_df.country_code.value_counts())