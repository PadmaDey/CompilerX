from pymongo import MongoClient

# created MongoClient
client = MongoClient("mongodb://Compilerx_test_user:9Xlnz8a6jG6bMt85aC9ml_NhAdv@46.137.68.213:27017/compilerx_test?authSource=admin")
mydb = client['compilerx_test']

collection = mydb.contacts

# created a cursor to read the collection
cursor = collection.find({})
data = list(cursor)
cursor.close()

# imported the required libraries and read the data into pandas dataframe
import pandas as pd
import numpy as np
pd.set_option("display.max_columns", None)

df = pd.DataFrame(data)

contact_df = df[['_id', 'contacts', 'uuid', 'company', 'industry', 'domain', 'website', 'facebook']]



def extract_contacts(df):
    extracted_cont = []
    
    for _, row in df.iterrows():
        contact_info = {}
        contact_info['_id'] = row['_id']
        contact_info['uuid'] = row['uuid']
        contact_info['company'] = row['company']
        contact_info['industry'] = row['industry']
        contact_info['domain'] = row['domain']
        contact_info['website'] = row['website']
        contact_info['facebook'] = row['facebook']
        
        contacts = row['contacts']
        if isinstance(contacts, dict):
            for key, value in contacts.items():
                contact_info[key] = value
        
        extracted_cont.append(contact_info)
    
    return pd.DataFrame(extracted_cont)

result_df = extract_contacts(contact_df)



extracted_df_contacts = result_df[['_id', 'uuid', 'company', 'industry', 'domain', 'website', 'facebook','value', 'first_name', 'last_name', 
                          'linkedin', 'twitter', 'phone_number']]


extracted_df_contacts['industry'] = [', '.join(lst) if isinstance(lst, list) else np.nan for lst in extracted_df_contacts['industry']]

extracted_df_contacts['full_name'] = extracted_df_contacts['first_name'].str.cat(extracted_df_contacts['last_name'], sep=' ', na_rep='')
extracted_df_contacts.drop(['first_name', 'last_name'], axis = 1, inplace = True)

extracted_df_contacts = extracted_df_contacts.rename(columns = {'value' : 'Email'})



def replace_cat_values(value):
    if pd.isnull(value) or value == '' or isinstance(value, float):
        return 'No Data Found'
    else:
        return value

cat_columns_to_process = [
    'company',
    'domain',  
    'website', 
    'facebook',
    'linkedin',  
    'twitter',
    'phone_number'
]
for column in cat_columns_to_process:
    extracted_df_contacts[column] = extracted_df_contacts[column].apply(replace_cat_values)



def classify_contacts(feature):
    if feature == 'No Data Found':
        return 'No Data Found'
    else:
        return 'Data Found'

contact_cols_to_classify = [
    'website', 
    'facebook',
    'linkedin',  
    'twitter',
    'phone_number'
]
for column in contact_cols_to_classify:
    extracted_df_contacts[f'{column}_nan'] = extracted_df_contacts[column].apply(classify_contacts)

extracted_df_contacts