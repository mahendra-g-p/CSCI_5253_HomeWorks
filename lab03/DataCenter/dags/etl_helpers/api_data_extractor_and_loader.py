import pytz
import requests
import pandas as pd
import json
from datetime import datetime
from google.cloud import storage


# Set the time zone to Mountain Time
mountain_time_zone = pytz.timezone('US/Mountain')


def extract_data_from_api(limit=50000, order='animal_id'):
    
    base_url = 'https://data.austintexas.gov/resource/9t4d-g238.json'
    
    api_key = 'ahvmqjn68rkjh52soek2l7s7t'
    
    headers = { 
        'accept': "application/json", 
        'apikey': api_key,
    }
    
    offset = 0
    all_data = []

    while offset < 157000:  # Assuming there are 157k records
        params = {
            '$limit': str(limit),
            '$offset': str(offset),
            '$order': order,
        }

        response = requests.get(base_url, headers=headers, params=params)
        print("response : ", response)
        current_data = response.json()
        
        # Break the loop if no more data is returned
        if not current_data:
            break

        all_data.extend(current_data)
        offset += limit

    return all_data


def create_data_frame(data):
    columns = [
        'animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
        'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
        'breed', 'color'
    ]

    data_list = []
    for entry in data:
        row_data = [entry.get(column, None) for column in columns]
        data_list.append(row_data)

    df = pd.DataFrame(data_list, columns=columns)
    return df

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

def upload_to_gcs(dataframe, bucket_name, file_path):
    

    credentials_info, bucket_name = get_gcs_credentials()
    client = storage.Client.from_service_account_info(credentials_info)
    
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    formatted_file_path = file_path.format(current_date, current_date)
    
    blob = bucket.blob(formatted_file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS with date: {current_date}.")


def data_extraction():
    extracted_data = extract_data_from_api(limit=50000, order='animal_id')
    shelter_data = create_data_frame(extracted_data)

    gcs_bucket_name = 'bucket_lab_03'
    gcs_file_path = 'raw_data/{}/outcomes_{}.csv'

    upload_to_gcs(shelter_data, gcs_bucket_name, gcs_file_path)