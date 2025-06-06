from pymongo import MongoClient

client = MongoClient("mongodb://Compilerx_test_user:9Xlnz8a6jG6bMt85aC9ml_NhAdv@46.137.68.213:27017/compilerx_test?authSource=admin")
mydb = client['compilerx_test']

collection = mydb.company

cursor = collection.find({})
data = list(cursor)
cursor.close()

import pandas as pd
import numpy as np
pd.set_option("display.max_columns", None)

df = pd.DataFrame(data)

add_info = df[['uuid', 'company', 'contact_email', 'phone_number', 'company_website', 'description', 'additional_information']]

def extract_additional_info(df):
    extracted_info = []
    
    for _, row in df.iterrows():
        company_info = {}
        company_info['uuid'] = row['uuid']
        company_info['company'] = row['company']
        company_info['contact_email'] = row['contact_email']
        company_info['phone_number'] = row['phone_number']
        company_info['company_website'] = row['company_website']
        company_info['description'] = row['description']
        
        additional_info = row['additional_information']
        if isinstance(additional_info, dict):
            for key, value in additional_info.items():
                company_info[key] = value
        
        extracted_info.append(company_info)
    
    return pd.DataFrame(extracted_info)

extracted_df_company = extract_additional_info(add_info)



def replace_country(value):
    if pd.isnull(value) or value == '' or isinstance(value, float):
        return 'No Data Found'
    elif 'US' in value or 'America' in value or 'Coast' in value or 'Lakes' in value or 'Scandinavia' in value or 'Las Vegas' in value:
        return 'US'
    elif 'APAC' in value:
        return 'APAC'
    elif 'EU' in value:
        return 'EU'
    elif 'EMEA' in value:
        return 'EMEA'
    elif 'Gulf Cooperation Council (GCC)' in value or 'India' in value:
        return 'Asia'
    else:
        return 'No Data Found'

def replace_cat_values(value):
    if pd.isnull(value) or value == '' or isinstance(value, float):
        return 'No Data Found'
    else:
        return value

def replace_cat_type(value):
    if pd.isnull(value) or value == '' or isinstance(value, float):
        return 'No Data Found'
    elif value in ('Privately Held', 'Private'):
        return 'Private'
    elif value in ('Non-profit', 'Nonprofit'):
        return 'Non-profit'
    elif value in ('Public Company', 'Public'):
        return 'Public'
    else:
        return value

def replace_no_of_emp(value): 
    if value == '' or pd.isnull(value) or isinstance(value, float): 
        return 'No Data Found'
    elif value in ('1-10', '11-50', '501-1000', '1001-5000', '1001-5000', '5001-10000'): 
        return value
    elif value in ('51-100', '101-250', '251-500'): 
        return '51-500'
    else:
        return '10001+'

cat_columns_to_process = [
    'description',
    'operating_status', 
    'hub_tags', 
    'investment_stage',  
    'funding_status', 
    'top_5_investors', 
    'ipo_status', 
    'stock_exchange',
    'contact_email',
    'phone_number',
    'company_website'
    
]
for column in cat_columns_to_process:
    extracted_df_company[column] = extracted_df_company[column].apply(replace_cat_values)

extracted_df_company['headquarters_regions'] = extracted_df_company['headquarters_regions'].apply(replace_country)
extracted_df_company['type'] = extracted_df_company['type'].apply(replace_cat_type)
extracted_df_company['number_of_employees'] = extracted_df_company['number_of_employees'].apply(replace_no_of_emp)



def classify_contacts(feature):
    if feature == 'No Data Found':
        return 'No Data Found'
    else:
        return 'Data Found'

contact_cols_to_classify = [
    'contact_email',
    'phone_number',
    'company_website'
]
for column in contact_cols_to_classify:
    extracted_df_company[f'{column}_nan'] = extracted_df_company[column].apply(classify_contacts)



extracted_df_company = extracted_df_company[['uuid', 'company', 'contact_email', 'phone_number', 'company_website', 'headquarters_location', 
                                             'headquarters_regions', 'description', 'operating_status', 'type', 'hub_tags', 'investment_stage', 
                                             'number_of_employees','funding_status', 'top_5_investors', 'number_of_lead_investors', 
                                             'number_of_investors', 'ipo_status', 'stock_exchange', 'contact_email_nan', 'phone_number_nan', 
                                             'company_website_nan']]



def consolidate_company_data(df):
    # Replace NaN values with 'No Data Found'
    df = df.replace(np.nan, 'No Data Found')

    # Group by company name
    grouped = df.groupby('company')

    # Function to consolidate rows within each group
    def consolidate(group):
        consolidated = {}

        # For uuid, take any one value (first in this case)
        consolidated['uuid'] = group['uuid'].iloc[0]
        
        # Include the company name
        consolidated['company'] = group['company'].iloc[0]

        # For each column, replace 'No Data Found' with other values if present
        for col in group.columns:
            if col not in ['uuid', 'company']:
                values = group[col].unique()
                if len(values) == 1:
                    consolidated[col] = values[0]
                else:
                    values_without_no_data = [val for val in values if val != 'No Data Found']
                    if values_without_no_data:
                        consolidated[col] = values_without_no_data[0]
                    else:
                        consolidated[col] = 'No Data Found'

        # Convert the dictionary to a Series
        return pd.Series(consolidated)

    # Apply the consolidate function to each group
    consolidated_df = grouped.apply(consolidate).reset_index(drop=True)

    return consolidated_df

extracted_df_company = consolidate_company_data(extracted_df_company)
extracted_df_company