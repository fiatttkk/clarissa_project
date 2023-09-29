from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from google.cloud import storage
import pandas as pd
import json
import gspread
import glob
import io
import requests
import os


# Postgres Config
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('POSTGRES_HOST')
db_port = int(os.getenv('POSTGRES_PORT'))
db_name = os.getenv('POSTGRES_DB_NAME')

# API path
owner_credentials_path = '/home/airflow/credentials/crs_credentials.json'
ggsheet_credentials_path = '/home/airflow/credentials/crs_ggsheet_credential.json'
province_api_url = 'https://raw.githubusercontent.com/kongvut/thai-province-data/master/api_province.json'
district_api_url = 'https://raw.githubusercontent.com/kongvut/thai-province-data/master/api_amphure.json'

# Json path
sku_file_path = '/opt/airflow/config/sku.json'
schema_fields = '/opt/airflow/config/schema_fields.json'

# Local path where csv will be placed when export from sale platform
local_path_folder = '/home/airflow/import_data'

# Google cloud platform path
ggsheet_revenue_URL = os.getenv('GGSHEET_REVENUE_URL')
ggsheet_inbound_URL = os.getenv('GGSHEET_INBOUND_URL')
gcs_bucket_name = 'clarissa-bucket'
gcs_raw_data_folder = 'clarissa_raw_data'
gcs_transformed_data_folder = 'clarissa_transformed_data'

# Table name
sale_table_name = 'clarissa_tiktok_sale_data'
revenue_table_name = 'tiktok_revenue_data'
inbound_table_name = 'inbound'

def import_csv_to_postgres(local_path):
    
    # CSV file paths using glob will return path in list.
    csv_file_paths = glob.glob(local_path + '/*.csv')
    
    # Creat engine from SQLAlchemy, I choose SQLALchemy cuz it look cleaner to make connection.
    db_connection = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(db_connection)
    
    for i in range(len(csv_file_paths)):
        
        # To protect of unsorted file I'll use loop in csv_file_paths to get the file name and use as a table name.
        csv_file_path = csv_file_paths[i]
        
        # Split csv_file_path, get last object in list and split '.' and get first object.
        table_name = csv_file_path.split("/")[-1].split(".")[0] 

        # Read data from CSV to PostgreSQL using pandas
        df = pd.read_csv(csv_file_path)
        
        # Creat engine from SQLAlchemy, I choose SQLALchemy cuz it look cleaner to make connection 
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"Table: {table_name} created on PostgreSQL table Successfully!")
        
    # Dispose of the engine to release resources    
    engine.dispose()
    
def fetch_from_postgres_to_gcs(blob_raw_folder, pg_table_name, bucket_name, gcs_credentials):
    
    # Make connection to postgresql with sqlalchemy
    db_connection = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(db_connection)
    
    # Query and export to csv with pandas
    sql = f"SELECT * FROM {pg_table_name}"
    sale_data_df = pd.read_sql(sql, engine)
    
    # Output csv name as table name
    file_name = f"{pg_table_name}.csv"
    sale_data_df.to_csv(file_name, index=False)
    
    # Upload the CSV data to GCS bucket
    storage_client = storage.Client.from_service_account_json(gcs_credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{blob_raw_folder}/{file_name}")
    blob.upload_from_filename(file_name, content_type='text/csv')
    
    print(f"{file_name} from PostgreSQL uploaded to {bucket_name} Successfully!")
    
    engine.dispose()
    
def import_sheet_to_gcs(
    sheetreader_credentials,
    revenue_URL,
    inbound_URL,
    revenue_sheet_name,
    inbound_sheet_name,
    blob_raw_folder,
    gcs_credentials,
    bucket_name
    ):
    
    # Connect to Google Sheets using the service account credentials
    gc = gspread.service_account(sheetreader_credentials)
    
    # Set up Google Cloud Storage client
    storage_client = storage.Client.from_service_account_json(gcs_credentials)
    
    # Create dict in list to specify 'url' and 'name'
    sheets = [
        {'url':revenue_URL, 'name':revenue_sheet_name},
        {'url':inbound_URL, 'name':inbound_sheet_name}
    ]
    
    # Loop to collect values from list of dict above
    for sheet in sheets:
        
        # Open by URL, collect first and second 'url' in an order from list
        sheet_obj = gc.open_by_key(sheet['url'])
        
        # Select the worksheet from list
        worksheet = sheet_obj.worksheet(sheet['name'])
        
        # Get all values from selected worksheet return in list
        values = worksheet.get_all_values()
        
        # Convert list to Dataframe using pandas
        df = pd.DataFrame(values)
        
        # Define the CSV filename as sheet name and output to csv
        file_name = f"{sheet['name']}.csv"
        df.to_csv(file_name, index=False, header=False)
        
        # Upload to GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{blob_raw_folder}/{file_name}")
        blob.upload_from_filename(file_name)
        
        print(f"Successfully imported sheet '{sheet['name']}' to {bucket_name}/{blob_raw_folder}")
        
    print("All sheets imported to Google Cloud Storage Successfully")
    
def get_th_address_api(province_api, district_api, ti):
    
    # Get Thai province API
    url_1 = province_api
    r_1 = requests.get(url_1)
    result_th_province = r_1.json()
    
    # Get Thai District API
    url_2 = district_api
    r_2 = requests.get(url_2)
    result_th_district = r_2.json()
    
    # Convert to pandas dataframe
    th_province = pd.DataFrame(result_th_province)
    th_district = pd.DataFrame(result_th_district)
    th_district = th_district[['name_th', 'name_en', 'province_id']]

    # Merge 2 dataframes to work easier
    th_address = th_district.merge(th_province, how='left', left_on='province_id', right_on='id')

    # Drop unused columns and rename for easier usage
    columns_to_drop = ['province_id', 'id', 'geography_id', 'created_at', 'updated_at', 'deleted_at']
    rename_dict = {'name_th_x': 'district_th',
                'name_en_x': 'district_en',
                'name_th_y': 'province_th',
                'name_en_y': 'province_en'}

    th_address = th_address.drop(columns_to_drop, axis=1).rename(columns=rename_dict)
    th_address['district_th'] = th_address['district_th'].str.replace('^เขต','', regex=True)
    
    # Dict zip from pandas dataframe into dictionary
    district_dict = dict(zip(th_address['district_th'], th_address['district_en']))
    province_dict = dict(zip(th_address['district_en'], th_address['province_en']))
    
    # Push to xcom
    ti.xcom_push(key='district_dict', value=district_dict)
    ti.xcom_push(key='province_dict', value=province_dict)
    
def sale_data_transform(
    gcs_credentials,
    bucket_name,
    sale_table_name,
    blob_raw_folder,
    blob_transformed_folder,
    sku_path,
    ti
    ):
    
    storage_client = storage.Client.from_service_account_json(gcs_credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{blob_raw_folder}/{sale_table_name}.csv")
    content = blob.download_as_text()
    sale_data_df = pd.read_csv(io.StringIO(content))
    
    # Drop unused columns
    sale_data_df = sale_data_df.drop(columns=[
        "Normal or Pre-order",
        "SKU ID",
        "Paid Time",
        "RTS Time",
        "Shipped Time",
        "Delivered Time",
        "Cancel Reason",
        "Fulfillment Type",
        "Warehouse Name",
        "Delivery Option",
        "Shipping Provider Name",
        "Buyer Message",
        "Recipient",
        "Phone #",
        "Zipcode",
        "Detail Address",
        "Additional address information",
        "Weight(kg)",
        "Package ID",
        "Seller Note",
        "Checked Status",
        "Checked Marked by"
    ])
    
    # Remove 'THB' from list 'cols_mod'
    cols_mod = [
        'SKU Unit Original Price',
        'SKU Subtotal Before Discount',
        'SKU Platform Discount',
        'SKU Seller Discount',
        'SKU Subtotal After Discount',
        'Shipping Fee After Discount',
        'Original Shipping Fee',
        'Shipping Fee Seller Discount',
        'Shipping Fee Platform Discount',
        'Taxes',
        'Small Order Fee',
        'Order Amount',
        'Order Refund Amount',
    ]
    sale_data_df[cols_mod] = sale_data_df[cols_mod].apply(lambda x: x.str.replace('THB',''))
    
    # Drop NaN values in 'Seller SKU'
    sale_data_df = sale_data_df.dropna(subset=['Seller SKU'])
    
    # Replace -.* with '' using .replace
    sale_data_df['Seller SKU'] = sale_data_df['Seller SKU'].str.replace('-.*', '', regex=True)
    
    # Replace thai letter with ''
    sale_data_df['Seller SKU'] = sale_data_df['Seller SKU'].str.replace(r'[\u0E00-\u0E7F]+', '', regex=True)
    
    # Read json from path
    with open(sku_path, 'r') as json_file:
        sku_mapping = json.load(json_file)
    
    # Replace values in the 'Seller SKU' column using the sku_mapping dictionary
    sale_data_df['Seller SKU'] = sale_data_df['Seller SKU'].replace(sku_mapping)
    
    # Replace \t with ''
    sale_data_df[['Created Time', 'Cancelled Time']] = sale_data_df[['Created Time', 'Cancelled Time']].replace('\t','', regex=True)
    
    # Change 'Created Time' and 'Cancelled Time' to datetime format
    sale_data_df[['Created Time', 'Cancelled Time']] = sale_data_df[['Created Time', 'Cancelled Time']].apply(pd.to_datetime, format='%d/%m/%Y %H:%M:%S', errors='coerce')
    
    # Change data type of Order ID and Tracking ID to string
    dtype_map = {'Order ID': str, 'Tracking ID': str}
    sale_data_df = sale_data_df.astype(dtype_map)
    
    # After changed to string, remove all .*
    sale_data_df['Tracking ID'] = sale_data_df['Tracking ID'].replace(r'\..*', '', regex=True)
    
    # Change data type of cols_numeric below to float (Detect automatically to float with pd.numeric) 
    cols_to_numeric = [
        'SKU Unit Original Price',
        'SKU Subtotal Before Discount',
        'SKU Platform Discount',
        'SKU Seller Discount',
        'SKU Subtotal After Discount',
        'Shipping Fee After Discount',
        'Original Shipping Fee',
        'Shipping Fee Seller Discount',
        'Shipping Fee Platform Discount',
        'Taxes',
        'Small Order Fee',
        'Order Amount',
        'Order Refund Amount'
    ]
    
    for cols in cols_to_numeric:
        sale_data_df[cols] = pd.to_numeric(sale_data_df[cols], errors='coerce')
    
    # Change all value in colomn 'Country' to 'Thailand'
    sale_data_df['Country'] = 'Thailand'
    
    # Remove unused 'th' from column 'District'
    unused_th = {'^เขต':'' , 'อำเภอ':''}
    sale_data_df['District'] = sale_data_df['District'].replace(unused_th, regex=True)
    
    # Pull dstrict and province dict
    district_dict = ti.xcom_pull(task_ids='get_address_api', key='district_dict')
    province_dict = ti.xcom_pull(task_ids='get_address_api', key='province_dict')
    
    # Use district_dict and province_dict to replace all thai name to eng name
    sale_data_df['District'] = sale_data_df['District'].apply(lambda x: district_dict.get(x, x))
    sale_data_df['Province'] = sale_data_df['District'].apply(lambda x: province_dict.get(x, x))
    
    # Output csv
    file_name = f"{sale_table_name}_transformed.csv"
    sale_data_df.to_csv(file_name, index=False)
    
    # Upload to GCS
    blob = bucket.blob(f"{blob_transformed_folder}/{file_name}")
    blob.upload_from_filename(file_name, content_type='text/csv')
    
    # Push to xcom
    ti.xcom_push(key='sale_data_transformed_name', value=file_name)
    
    print(f"{file_name} uploaded to {bucket_name}/{blob_transformed_folder} Successfully.")
    
def inbound_data_transform(
    gcs_credentials,
    bucket_name,
    blob_raw_folder,
    blob_transformed_folder,
    inbound_table_name,
    ti
):
    
    storage_client = storage.Client.from_service_account_json(gcs_credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{blob_raw_folder}/{inbound_table_name}.csv")
    content = blob.download_as_text()
    inbound_data_df = pd.read_csv(io.StringIO(content), skiprows=1)
    
    # Drop unsused columns (In this case I have to find NaN column with float('nan') to delete 'nan' column)
    # Drop NaN rows subset = SKU
    columns_to_drop = [
        'Order',
        'Unnamed: 2',
        'Color + Size',
        'Location',
        'SKU + Location',
        'Product - Color - Size',
        'Batch+SKU'
    ]
    inbound_data_df = inbound_data_df.drop(columns=columns_to_drop).dropna(subset=['SKU'])
    
    # Change type of column 'Date' to date
    inbound_data_df['Date'] = pd.to_datetime(inbound_data_df['Date'], format='%d/%m/%Y').dt.date
    
    # Change type of column 'QTY' to int
    inbound_data_df['QTY'] = inbound_data_df['QTY'].astype(int)
    
    # Change type of column 'Cost' and 'Total' to float
    cols_float = ['Cost', 'Total']
    for col in cols_float:
        inbound_data_df[col] = inbound_data_df[col].astype(float)
        
    # Output csv
    file_name = f"{inbound_table_name}_transformed.csv"
    inbound_data_df.to_csv(file_name, index=False)
    
    # Upload to GCS
    blob = bucket.blob(f"{blob_transformed_folder}/{file_name}")
    blob.upload_from_filename(file_name, content_type='text/csv')
    
    # Push to xcom
    ti.xcom_push(key='inbound_data_transformed_name', value=file_name)
    
    print(f"{file_name} uploaded to {bucket_name}/{blob_transformed_folder} Successfully.")
    
def revenue_data_transformed(
    gcs_credentials,
    bucket_name,
    blob_raw_folder,
    blob_transformed_folder,
    revenue_table_name,
    ti
):
    
    storage_client = storage.Client.from_service_account_json(gcs_credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{blob_raw_folder}/{revenue_table_name}.csv")
    content = blob.download_as_text()
    revenue_data_df = pd.read_csv(io.StringIO(content))
    
    # Drop unused columns and dropna subset = 'Order/adjustment ID'
    revenue_data_df = revenue_data_df.drop(columns=[
    'Formula',
    'Type',
    'Time(Timezone=UTC)',
    'Currency',
    'Subtotal after seller discounts',
    'Subtotal before discounts',
    'Seller discounts',
    'Refund subtotal after seller discounts',
    'Refund subtotal before seller discounts',
    'Refund of seller discounts',
    'Settlement amount',
    'Related order ID'
    ]).dropna(subset=['Order/adjustment ID'])
    
    # Change all type to float and convert to positive except Order ID and use .abs() to get all positive values
    for column in revenue_data_df.columns:
        if column != 'Order/adjustment ID':
            revenue_data_df[column] = revenue_data_df[column].astype(float).abs()
            
    # Change data type of column 'Order/adjustment ID' to string and drop duplicates
    revenue_data_df['Order/adjustment ID'] = revenue_data_df['Order/adjustment ID'].astype(str).drop_duplicates()
    
    # Output to csv
    file_name = f"{revenue_table_name}_transformed.csv"
    revenue_data_df.to_csv(file_name, index=False)
    
    # Upload to GCS
    blob = bucket.blob(f"{blob_transformed_folder}/{file_name}")
    blob.upload_from_filename(file_name, content_type='text/csv')
    
    # Push to xcom
    ti.xcom_push(key='revenue_data_transformed_name', value=file_name)
    
    print(f"{file_name} uploaded to {bucket_name}/{blob_transformed_folder} Successfully.")
    
def merge_all_data(
    gcs_credentials,
    bucket_name,
    blob_transformed_folder,
    ti
):
    
    storage_client = storage.Client.from_service_account_json(gcs_credentials)
    bucket = storage_client.bucket(bucket_name)
    sale_file_name = ti.xcom_pull(task_ids='sale_data_transform', key='sale_data_transformed_name')
    inbound_file_name = ti.xcom_pull(task_ids='inbound_data_transform', key='inbound_data_transformed_name')
    revenue_file_name = ti.xcom_pull(task_ids='revenue_data_transform', key='revenue_data_transformed_name')
    
    # Define blob path
    sale_blob = bucket.blob(f"{blob_transformed_folder}/{sale_file_name}")
    inbound_blob = bucket.blob(f"{blob_transformed_folder}/{inbound_file_name}")
    revenue_blob = bucket.blob(f"{blob_transformed_folder}/{revenue_file_name}")
    
    # Define blob content
    sale_content = sale_blob.download_as_text()
    inbound_content = inbound_blob.download_as_text()
    revenue_content = revenue_blob.download_as_text()
    
    # Read with pandas and keep the value in different variables
    sale_data_df = pd.read_csv(io.StringIO(sale_content))
    inbound_data_df = pd.read_csv(io.StringIO(inbound_content))
    revenue_data_df = pd.read_csv(io.StringIO(revenue_content))
    
    # Use sale_data_df as a main table, first drop duplicate on inbound_data_df (I will drop duplicate just for merge section because I still need to use whole dataframe from inbound data)
    inbound_data_df = inbound_data_df.drop_duplicates(subset='SKU')
    
    # Merge sale_data_df with inbound_data_df first and drop column 'SKU' (I choose only 3 columns from inbound data because I use only those 3 columns for merged table)
    sale_inbound_merged = sale_data_df.merge(inbound_data_df[['SKU', 'Cost', 'Shop name']], how='left', left_on='Seller SKU', right_on='SKU').drop(columns=['SKU'])
    
    # Merge sale_inbound_merged with revenue_data_df and drop column 'Order/adjustment ID'
    all_merged_df = sale_inbound_merged.merge(revenue_data_df, how='left', left_on='Order ID', right_on='Order/adjustment ID').drop(columns=['Order/adjustment ID'])
    
    # After all merged we need to replace duplicate values from revenue_data_df.columns with np.nan (Because in each unique order it must contain only one revenue transacttion)
    # Define all revenue_data_df column into variable except for 'Order/adjustment ID' because it dropped
    revenue_columns = revenue_data_df.columns
    
    # From revenue_data.columns, loop to get all column name except Order ID then use .mask to make condition which 'Order ID' is duplicated, and replace np.nan in True value
    for cols in revenue_columns:
        if cols != 'Order/adjustment ID':
            all_merged_df[cols] = all_merged_df[cols].mask(all_merged_df['Order ID'].duplicated(), 0)
            all_merged_df[cols].fillna(0, inplace=True)
    
    # Output to csv
    file_name = 'all_data_merged.csv'
    all_merged_df.to_csv(file_name, index=False)
    
    # Upload to GCS
    blob = bucket.blob(f"{blob_transformed_folder}/{file_name}")
    blob.upload_from_filename(file_name, content_type='text/csv')
    
default_args = {
    'owner': 'Fiat',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Load json file, schema_fields
with open(schema_fields, 'r') as json_file:
    schema_fields = json.load(json_file)
    
# Create a DAG
with DAG(
    'clarissa_gcp_pipeline',
    default_args=default_args,
    schedule_interval='@weekly'
    )as dag:

    t1 = PythonOperator(
        task_id="csv_to_postgres",
        python_callable=import_csv_to_postgres,
        op_kwargs={"local_path":local_path_folder},
    )
    
    t2 = PythonOperator(
        task_id="postgres_to_gcs",
        python_callable=fetch_from_postgres_to_gcs,
        op_kwargs={
            "blob_raw_folder":gcs_raw_data_folder,
            "bucket_name":gcs_bucket_name,
            "gcs_credentials":owner_credentials_path,
            "pg_table_name":sale_table_name
            },
    )
    
    t3 = PythonOperator(
        task_id="google_sheets_to_gcs",
        python_callable=import_sheet_to_gcs,
        op_kwargs={
            "sheetreader_credentials":ggsheet_credentials_path,
            "gcs_credentials":owner_credentials_path,
            "revenue_URL":ggsheet_revenue_URL,
            "inbound_URL":ggsheet_inbound_URL,
            "revenue_sheet_name":revenue_table_name,
            "inbound_sheet_name":inbound_table_name,
            "blob_raw_folder":gcs_raw_data_folder,
            "bucket_name":gcs_bucket_name
            },
    )
    
    t4 = PythonOperator(
        task_id="get_address_api",
        python_callable=get_th_address_api,
        op_kwargs={
            "province_api":province_api_url,
            "district_api":district_api_url
        },
    )
    
    t5 = PythonOperator(
        task_id="sale_data_transform",
        python_callable=sale_data_transform,
        op_kwargs={
            "gcs_credentials":owner_credentials_path,
            "bucket_name":gcs_bucket_name,
            "sale_table_name":sale_table_name,
            "inbound_table_name":inbound_table_name,
            "revenue_table_name":revenue_table_name,
            "blob_raw_folder":gcs_raw_data_folder,
            "blob_transformed_folder":gcs_transformed_data_folder,
            "sku_path":sku_file_path
            },
    )
    
    t6 = PythonOperator(
        task_id="inbound_data_transform",
        python_callable=inbound_data_transform,
        op_kwargs={
            "gcs_credentials":owner_credentials_path,
            "bucket_name":gcs_bucket_name,
            "blob_raw_folder":gcs_raw_data_folder,
            "blob_transformed_folder":gcs_transformed_data_folder,
            "inbound_table_name":inbound_table_name
        }
    )
    
    t7 = PythonOperator(
        task_id="revenue_data_transform",
        python_callable=revenue_data_transformed,
        op_kwargs={
            "gcs_credentials":owner_credentials_path,
            "bucket_name":gcs_bucket_name,
            "blob_raw_folder":gcs_raw_data_folder,
            "blob_transformed_folder":gcs_transformed_data_folder,
            "revenue_table_name":revenue_table_name
        }
    )
    
    t8 = PythonOperator(
        task_id="merge_all_data",
        python_callable=merge_all_data,
        op_kwargs={
            "gcs_credentials":owner_credentials_path,
            "bucket_name":gcs_bucket_name,
            "blob_transformed_folder":gcs_transformed_data_folder,
            "sale_table_name":sale_table_name,
            "inbound_table_name":inbound_table_name,
            "revenue_table_name":revenue_table_name
        }
    )
    
    t9 = GCSToBigQueryOperator(
        task_id='all_merged_gcs_to_bq',
        bucket=gcs_bucket_name,
        gcp_conn_id='gcp_default',
        source_objects=[f"{gcs_transformed_data_folder}/all_data_merged.csv"],
        destination_project_dataset_table='norse-figure-391710.clarissa_datasets.all_data_merged',
        schema_fields=schema_fields,
        write_disposition='WRITE_TRUNCATE',
    )
    
    t10 = GCSToBigQueryOperator(
        task_id='inbound_gcs_to_bq',
        bucket=gcs_bucket_name,
        gcp_conn_id='gcp_default',
        source_objects=[f"{gcs_transformed_data_folder}/inbound_transformed.csv"],
        destination_project_dataset_table='norse-figure-391710.clarissa_datasets.inbound_data',
        write_disposition='WRITE_TRUNCATE',
    )

    t1 >> t2 >> t5 >> t8 >> [t9, t10]
    t3 >> [t6, t7] >> t8
    t4 >> t5