from pymongo import MongoClient

client = MongoClient("mongodb://Compilerx_test_user:9Xlnz8a6jG6bMt85aC9ml_NhAdv@46.137.68.213:27017/compilerx_test?authSource=admin")
mydb = client['compilerx_test']

collection = mydb.contacts_details

cursor = collection.find({})
data = list(cursor)

cursor.close()

import pandas as pd
import numpy as np
pd.set_option("display.max_columns", None)


df = pd.DataFrame(data)

contact_details_df = df[['_id', 'First_Name', 'Last_Name', 'Experience', 'Email']]

def extract_experience(df):
    extracted_exp = []
    
    for _, row in df.iterrows():
        contact_details_info = {}
        contact_details_info['_id'] = row['_id']
        contact_details_info['First_Name'] = row['First_Name']
        contact_details_info['Last_Name'] = row['Last_Name']
        contact_details_info['Email'] = row['Email']
        
        experience_info = row['Experience']
        if isinstance(experience_info, dict):
            for key, value in experience_info.items():
                contact_details_info[key] = value
        
        extracted_exp.append(contact_details_info)
    
    return pd.DataFrame(extracted_exp)


result_df = extract_experience(contact_details_df)

contact_details_df = result_df[['_id', 'First_Name', 'Last_Name', 'Position', 'Company', 'Email']]

contact_details_df['Full_Name'] = contact_details_df['First_Name'].str.cat(contact_details_df['Last_Name'], sep=' ', na_rep='')
contact_details_df.drop(['First_Name', 'Last_Name'], axis = 1, inplace = True)

def replace_cat_values(value):
    if pd.isnull(value) or value == '' or isinstance(value, float):
        return 'No Data Found'
    else:
        return value

cat_columns_to_process = [
    'Position',
    'Company',  
    'Email', 
    'Full_Name'
]
for column in cat_columns_to_process:
    contact_details_df[column] = contact_details_df[column].apply(replace_cat_values)

contact_details_df