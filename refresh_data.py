#pip install requests google-cloud-bigquery pandas pandas-gbq

#import modules
import requests
import pandas as pd
import json
from google.cloud import bigquery


# This block of code is to get the access_token which used by POWER BI API for authentication
#Tenant_id, client_id and client_secret are all gotten after creating the Power BI API App
tenant_id = '96526e04-0b80-45a5-9107-8aa8b4a307a5'
client_id = '69141b39-875b-4949-85c3-9913372e10ea'
client_secret = 'XmN8Q~SjglwBkayj3Oluc6K_c1es7sAQ50OCqcTW'

# function to retrieve the access_token
def get_access_token(tenant_id, client_id, client_secret):
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    body = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://analysis.windows.net/powerbi/api/.default'
    }
    
    response = requests.post(url, headers=headers, data=body)
    response.raise_for_status()
    return response.json().get('access_token')

#variable to store the access_token
access_token = get_access_token(tenant_id, client_id, client_secret)



#This block of code is to get all the datasets in a workspace
# Group ID is the ID of the workspace of Power BI
group_id = 'cf645e14-8a93-4241-8384-decd8ff8b723'

#Function to get all the datasets in a workspace
def get_all_datasets(group_id, access_token):
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets'
    headers = {'Authorization': f'Bearer {access_token}'}
    datasets_response = requests.get(url, headers=headers)

    # Check if the request was successful
    if datasets_response.status_code == 200:
        datasets = datasets_response.json()["value"]
        return datasets
    else:
        print(f"Failed to get datasets: {datasets_response.status_code}")
        datasets = []
        
#Variable to store the workspace dataset list
workspace_datasets = get_all_datasets(group_id, access_token)




#This block of code is to create a dataset that stores the refresh history of the datasets
columns = ['dataset_id', 'dataset_name','refresh_type', 'start_time', 'end_time', 'status', 'error_message']
refresh_history_df = pd.DataFrame(columns=columns)

#function to get the refresh
def get_dataset_refresh_history(access_token, group_id, dataset_id):
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes'
    headers = {'Authorization': f'Bearer {access_token}'}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get('value', [])


for dataset in workspace_datasets:
    dataset_id = dataset["id"]
    dataset_name = dataset["name"]
    
    refresh_history = get_dataset_refresh_history(access_token, group_id, dataset_id)
    
    # Loop through each refresh and append it to the DataFrame
    for refresh in refresh_history:

        start_time_str = refresh.get('startTime', '')
        end_time_str = refresh.get('endTime', '')

        start_time = pd.to_datetime(start_time_str).strftime('%Y-%m-%d %H:%M:%S') if start_time_str else None
        end_time = pd.to_datetime(end_time_str).strftime('%Y-%m-%d %H:%M:%S') if end_time_str else None

        refresh_data = {
            'dataset_id': dataset_id,
            'dataset_name': dataset_name,
            'refresh_type': refresh.get('refreshType', ''),
            'start_time': start_time,
            'end_time': end_time,
            'status': refresh.get('status', ''),
            'error_message': refresh.get('serviceExceptionJson', '')}
            
        refresh_history_df = refresh_history_df._append(refresh_data, ignore_index=True)



#This block of code loads the refresh history dataset into BigQuery
client = bigquery.Client()
project_id = 'babbangona-dev'
bq_dataset_id = 'power_bi_refreshes'
bq_table_id = 'refresh_history'

table_id = f"{project_id}.{bq_dataset_id}.{bq_table_id}"

# Define job configuration for table append
job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

# Push the entire DataFrame into BigQuery
job = client.load_table_from_dataframe(refresh_history_df, table_id, job_config=job_config)

job.result()  # Wait for the job to complete

print(f"Loaded {refresh_history_df.shape[0]} rows into {table_id}")

