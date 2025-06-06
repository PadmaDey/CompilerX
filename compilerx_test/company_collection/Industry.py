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

industry_df = df[['uuid','company', 'industry']]

def extract_industries(df):
    # Create an empty list to store extracted data
    extracted_data = []
    
    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        uuid = row['uuid']
        company = row['company']
        industries = row['industry']
        
        # If industries is a list, extract each industry
        if isinstance(industries, list):
            for industry in industries:
                extracted_data.append({'uuid': uuid,'company': company, 'industry': industry})
    
    # Convert the list of dictionaries into a DataFrame
    extracted_df_industry = pd.DataFrame(extracted_data)
    
    return extracted_df_industry

extracted_df_industry = extract_industries(industry_df)

company_count = extracted_df_industry.groupby('industry')['company'].count().sort_values(ascending = False)
extracted_df_industry['count_of_company'] = extracted_df_industry['industry'].map(company_count)

extracted_df_industry.sort_values(by = 'count_of_company', ascending = False, inplace = True)
extracted_df_industry