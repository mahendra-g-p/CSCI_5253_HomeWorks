import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

mountain_time_zone = pytz.timezone('US/Mountain')

def get_gcs_credentials():
    bucket_name = "bucket_lab_03"
    credentials_info = {
                          "type": "service_account",
                          "project_id": "brilliant-balm-405705",
                          "private_key_id": "766d5d2259d9789ee52d68835bf6bf77882fe035",
                          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCl8/ZSy3ISmUuA\ngq8fKMp0QOkHr3ogCntt5hENWyF9FYm/66dSWkFPbBMHUEWshr/j5f2GJ5yYwyxa\nZsg/SYvosQNiCfrtonQQ5sHe0+F1fxOvj+j1ZM6kvHZqwzJ/OfOSTmlXRHl5ZoNO\nn8gC+2R7RTEswb0tyA7sWR6wk6CtKWQJprYZ2qBpX0wEyx655DoXFHzpfKUkI6m/\nSYIMjIZmuQ0duOoMcW2TfXjrEF3wUtLOm3xAhAtgRB/wAsLw0SKpg7h3Gpb7uFL6\n0jY+NXgZQvF76UUOCqVO7NRPMh8l1cXeXhCB+gzG/VTuu/ug23ZmLbx3A2SvZYQ5\nkgpLvB33AgMBAAECggEABI0cREkFZ90sZOnbQ6TVaJAfKUbMDUo8TaycQHGEtEQo\nSEso25P+FoowO2hy1g3GqslempSbhtT+HRHp4VJzCSlRnm6yTbIW3Mp3UVUePyAC\nZN4y1V1eYsYzDoUr7Vv5ghmvFJBG2egl0bzaug+K7JaJMrZdfylCv4NHhDAy8oy/\nYOopWB6weCde7qHiLh6c0UqJ+ZcwccJ7e0HT+kiSHKLLsz0Vws0ylUh7FkqR1Tce\nE/1F7JHKY6PbbSUFPyjC6+a4n8/xUuiCxx+69j3y842sPV9hgJ/c0wNbD8oQhbDy\nAsCi1ZihkqK6hgSd6ud7tM1MI0TAj2SKO2hpWdvWAQKBgQDd0O/gBDMlfIaddGSG\nlMpFGG45lchUPTJd/143RTkCkeIlBtezVC6n4OoqYlklP3pcClqvJatrHzEjKao5\n94mxhq9ROhUtnsgKawm5/QBTLSJFa2lzJr0Q2tG7UeTH0S4CmYKXQVtJc/rQYanp\nhvyaROFyKlF9l+J82QDscRgxJwKBgQC/hx40txLiyAbFQgqPydTKd+eBygsBYlkA\nG3WoAk94dGzjboG8buni+Gf0T7JEHbgW/SEA4MYpvGLiC2joF/LTOcPOYQYCZ7Eu\nySipvgoN60dxhJbE/SNnOZZvJBLN3rOUZ2yHjr/tBOnaqg87MKZ23e0zbf4U1vmu\ngZ1aTV4OsQKBgD8einPLniZHpiK+ETD+vAQnbI40Yhoeg+udlFjT+OITUy/IH68n\nAFPDO78t10IzKwVv6Ng4Nuw3pwrje04dc3Ax7EeH6KjfqvrT9KOZK/N/P2ZTp2Ee\nH+Tg75eHOTvm+VnCBxg1f92KBFAxymDqiYz4ltKe2iuMAeYW9h1SHVk3AoGAS5MB\ndEOZDCtpoISCrmIxuQK/MxOKbC1meQhc0MK4oQsvvD5qqvQJDip+uoSIDyC69zdC\nwpnvF6DiU9e0uYBZrMdqYBEg0eognBl8Fh6K0Rs5wa1T4L8SLTUoCwrs8JcjvTdi\nN8s4KEp8DHB2OiDkTpsbcePBEnARba5vbKaCqsECgYBaMJULZ6X5ZIQuysPHyK02\nb8Cz7lZRRAZziEydsJKh1cO/BOzLtoC6E/WKS3idGycjS71Z362WViNug6y2GUhe\nYMDYCzf3kZoeF467YVQhVKqWmbsTi7YnIICUahZduWUnaSrLLRrKgyi37cBlhS/U\n1L9w4C1bNBWEnZwiUAgpdQ==\n-----END PRIVATE KEY-----\n",
                          "client_email": "mydemolab03acc@brilliant-balm-405705.iam.gserviceaccount.com",
                          "client_id": "112756558214475169610",
                          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                          "token_uri": "https://oauth2.googleapis.com/token",
                          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/mydemolab03acc%40brilliant-balm-405705.iam.gserviceaccount.com",
                          "universe_domain": "googleapis.com"
                        }
    return credentials_info, bucket_name


def load_data_from_gcs(credentials_info, gcs_bucket_name):
    gcs_file_path = 'raw_data/{}/outcomes_{}.csv'

    client = storage.Client.from_service_account_info(credentials_info)
    
    bucket = client.get_bucket(gcs_bucket_name)
    
    # Get the current date in the format YYYY-MM-DD
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    
    # Format the file path with the current date
    formatted_file_path = gcs_file_path.format(current_date, current_date)
    
    # Read the CSV file from GCS into a DataFrame
    blob = bucket.blob(formatted_file_path)
    csv_data = blob.download_as_text()
    df = pd.read_csv(StringIO(csv_data))

    return df


def write_data_to_gcs(dataframe, credentials_info, bucket_name, file_path):
    
    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS.")


def prep_outcomes_fact_data(data, animal_dim_data, date_dim_data, outcome_type_dim_data):
    # Create or append data to the Outcomes_Fact table, linking to dimension tables
    df_fact = data.merge(date_dim_data, how='inner', left_on='date_recorded', right_on='date_recorded')
    df_fact = df_fact.merge(animal_dim_data, how='inner', left_on='animal_id', right_on='animal_id')
    df_fact = df_fact.merge(outcome_type_dim_data, how='inner', left_on='outcome_type', right_on='outcome_type')

    # Map the merged DataFrame columns to the table columns
    df_fact.rename(columns={
        'date_key': 'date_key',
        'animal_key': 'animal_key',
        'outcome_type_key': 'outcome_type_key'
    }, inplace=True)

    df_fact = df_fact[['date_key', 'animal_key', 'outcome_type_key']]
    return df_fact



# A function to convert age to years
def age_to_years(age):
    age_in_years = 0
    if isinstance(age, str):
        if 'year' in age:
            age_in_years = int(age.split()[0])
        elif 'month' in age:
            age_in_months = int(age.split()[0])
            age_in_years = age_in_months/12
        elif 'day' in age:
            age_in_days = int(age.split()[0])
            age_in_years = age_in_days/365
        

    if age_in_years < 1:
      age_in_years = 0

    return str(age_in_years)

def map_month_to_quarter(month):
    if 1 <= month <= 3:
        return 'Q1'
    elif 4 <= month <= 6:
        return 'Q2'
    elif 7 <= month <= 9:
        return 'Q3'
    else:
        return 'Q4'

def transform_raw_data(data):
    transformed_data = data.copy()

    transformed_data['monthyear'] = pd.to_datetime(transformed_data['monthyear'])
    transformed_data['month_recorded'] = transformed_data['monthyear'].dt.month
    transformed_data['year_recorded'] = transformed_data['monthyear'].dt.year
    transformed_data[['animal_name']] = transformed_data[['name']].fillna('Name_less')
    transformed_data['sex_upon_outcome'] = transformed_data['sex_upon_outcome'].fillna('Unknown Unknown')
    transformed_data[['sterilization_status', 'gender']] = transformed_data['sex_upon_outcome'].str.split(' ', expand=True)
    transformed_data['age_upon_outcome'] = transformed_data['age_upon_outcome'].astype(str)
    transformed_data['age_years'] = transformed_data['age_upon_outcome'].apply(age_to_years)
    transformed_data.drop(columns = ['monthyear', 'name', 'age_upon_outcome', 'age_upon_outcome'], axis=1, inplace=True)
    transformed_data['datetime'] = pd.to_datetime(transformed_data['datetime'])
    transformed_data['day_of_week'] = transformed_data['datetime'].dt.day_name()
    transformed_data['quarter_recorded'] = transformed_data['datetime'].dt.month.apply(map_month_to_quarter)
    transformed_data['datetime'] = pd.to_datetime(transformed_data['datetime'])
    transformed_data['datetime'] = transformed_data['datetime'].dt.date
    transformed_data['outcome_type'] = transformed_data['outcome_type'].fillna('Not_Available')
    cols_mapping = {
    'animal_id': 'animal_id',
    'datetime': 'date_recorded',
    'date_of_birth': 'dob',
    'outcome_type': 'outcome_type',
    'animal_type': 'animal_type',
    'breed': 'breed',
    'color': 'color'
    }
    transformed_data.rename(columns=cols_mapping, inplace=True)

    # Remove special characters from the 'animal_name' column
    transformed_data['animal_name'] = transformed_data['animal_name'].str.replace('\W', '', regex=True)

    return transformed_data

def prepare_outcome_type_dim_data(new_data):
    outcome_type_dim_data = new_data[['outcome_type']].drop_duplicates()
    outcome_type_dim_data['outcome_type_key'] = range(1, len(outcome_type_dim_data) + 1)
    return outcome_type_dim_data

def prepare_animal_dim_data(new_data):
    animal_dim_data = new_data[['animal_id', 'animal_name', 'dob', 'animal_type', 'sterilization_status', 'gender', 'breed', 'color']].drop_duplicates()
    animal_dim_data['animal_key'] = range(1, len(animal_dim_data) + 1)
    return animal_dim_data

def prepare_date_dim_data(new_data):
    date_dim_data = new_data[['date_recorded','day_of_week', 'month_recorded', 'quarter_recorded', 'year_recorded']].drop_duplicates()
    date_dim_data['date_key'] = range(1, len(date_dim_data) + 1)
    return date_dim_data

def transform_data():
    credentials_info, bucket_name = get_gcs_credentials()

    new_data = load_data_from_gcs(credentials_info, bucket_name)
    
    new_data = transform_raw_data(new_data)
    
    animal_dim_data = prepare_animal_dim_data(new_data)
    date_dim_data = prepare_date_dim_data(new_data)
    outcome_type_dim_data = prepare_outcome_type_dim_data(new_data)

    fact_outcomes = prep_outcomes_fact_data(new_data, animal_dim_data, date_dim_data, outcome_type_dim_data)

    animal_dimension_csv_path = "transformed_data/animal_dim_data.csv"
    dates_dimension_csv_path = "transformed_data/date_dim_data.csv"
    outcome_types_dimesion_csv_path = "transformed_data/outcome_type_dim_data.csv"
    fact_outcomes_path = "transformed_data/fact_outcomes.csv"

    write_data_to_gcs(animal_dim_data, credentials_info, bucket_name, animal_dimension_csv_path)
    write_data_to_gcs(date_dim_data, credentials_info, bucket_name, dates_dimension_csv_path)
    write_data_to_gcs(outcome_type_dim_data, credentials_info, bucket_name, outcome_types_dimesion_csv_path)
    write_data_to_gcs(fact_outcomes, credentials_info, bucket_name, fact_outcomes_path)