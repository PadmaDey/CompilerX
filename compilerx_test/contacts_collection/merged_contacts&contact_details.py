merged_df = pd.merge(extracted_df_contacts, contact_details_df[['Email', 'Position']], how = 'left', on='Email')
merged_df