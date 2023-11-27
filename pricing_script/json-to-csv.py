#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os 
import json
import pandas as pd
from datetime import datetime


# In[2]:


# Function to flatten the nested dictionary
def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f'{parent_key}{sep}{k}' if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


# In[3]:


def find_customer_keys(json_data):
    customer_keys = []
    for key, value in json_data.items():  # this iterates through top level keys, but it is sufficient enough 
        if isinstance(value, dict) and any(sub_key.startswith("gcp-") for sub_key in value):
            customer_keys.append(key)
    return customer_keys


# In[4]:


def json_to_csv(json_data):
    # Initialize an empty DataFrame
    df = pd.DataFrame()

    # Dynamically find customer-related keys
    customer_keys = find_customer_keys(json_data)

    for key in customer_keys:
        if isinstance(json_data[key], dict):
            for sub_key, value in json_data[key].items():
                if isinstance(value, dict):
                    # Flatten nested dictionaries without including the customer key in column names
                    flattened_data = flatten_dict(value, parent_key=key)
                    for flat_key, flat_value in flattened_data.items():
                        df.at[sub_key, flat_key] = flat_value
                else:
                    df.at[sub_key, key] = value

    df.reset_index(inplace=True)
    
    df.rename(columns={'index': 'customer'}, inplace=True)

    # Fill NaN values with 0, except for the 'Total' row
    df.loc[df['customer'] != 'Total'] = df.loc[df['customer'] != 'Total'].fillna(0)

    # Remove the existing 'Total' row if it exists
    if 'Total' in df['customer'].values:
        df = df[df['customer'] != 'Total']
    
    # Calculate the sum for each column to create a new 'Total' row
    total_row = df.sum(numeric_only=True)
    total_row['customer'] = 'Total'
    df = df.append(total_row, ignore_index=True)

    # Combine Davion 
    davion_total = df.loc[df['customer'].isin(['gcp-davion-prod_davion-production-usdt', 
                                               'gcp-davion-prod_davion-production']), 
                          df.columns != 'customer'].sum()
    davion_total['customer'] = 'davion-total'
    df = df.append(davion_total, ignore_index=True)
    
    return df


# In[5]:


# Get the path of the current working directory
current_folder = os.getcwd()

# List all JSON files in the current folder
json_files = [os.path.join(current_folder, file) for file in os.listdir(current_folder) if file.endswith('.json')]

# Initialize an empty list to store DataFrames
dfs = []

# Process each JSON file
for file_path in json_files:
    with open(file_path, 'r') as file:
        json_data = json.load(file)
        df = json_to_csv(json_data)
        dfs.append(df)


# In[6]:


# Save DataFrame to CSV

# Get the current date
current_date = datetime.now().strftime("%Y-%m-%d")

# Save the DataFrame to a new CSV, including the date in the filename
csv_filename = f"output_{current_date}.csv"
df.to_csv(csv_filename)
print(f"Data saved to {csv_filename}")


# In[8]:


# # if Avishag wants me to aggregate 

# combined_df = pd.concat([df, df_2], ignore_index=True)
# aggregate_df = combined_df.groupby('customer').sum().reset_index()


# In[ ]:





# In[ ]:




