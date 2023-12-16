import psycopg2
#import pandas as pd
from io import StringIO
from google.cloud import storage
#from sqlalchemy import create_engine
import csv
from datetime import datetime

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

def read_data_from_gcs(bucket_name, file_path):
    credentials_info, bucket_name = get_gcs_credentials()
    client = storage.Client.from_service_account_info(credentials_info)
    bucket = client.bucket(bucket_name)

    #blobs = bucket.list_blobs()
    blob = bucket.blob(file_path)
    data = blob.download_as_text()
    return data



def create_table(connection, table_name):

    create_tables_query = " "
    drop_tables_query = " "
    if table_name =="animal_dimension":
        create_tables_query = """
        
        CREATE TABLE animal_dimension (
            animal_key INT PRIMARY KEY,
            animal_id VARCHAR,
            animal_name VARCHAR,
            dob DATE,
            animal_type VARCHAR,
            sterilization_status VARCHAR,
            gender VARCHAR,
            breed VARCHAR,
            color VARCHAR
        );
            """
        
        drop_tables_query = drop_tables_query = """
        -- Drop tables if they exist
        
        DROP TABLE IF EXISTS animal_dimension CASCADE;
        
        """
    elif table_name =="outcome_type_dimension":
        create_tables_query = """
        
        CREATE TABLE outcome_type_dimension (
            outcome_type_key INT PRIMARY KEY,
            outcome_type VARCHAR
        );
            """
        
        drop_tables_query = """
        -- Drop tables if they exist
        
        DROP TABLE IF EXISTS outcome_type_dimension CASCADE;

        """
    elif table_name=="date_dimension":
        create_tables_query = """
        
        CREATE TABLE date_dimension (
            date_key INT PRIMARY KEY,
            date_recorded DATE,
            day_of_week VARCHAR,
            month_recorded VARCHAR,
            quarter_recorded VARCHAR,
            year_recorded VARCHAR
        );
            """
        drop_tables_query = """
        -- Drop tables if they exist
        
        DROP TABLE IF EXISTS date_dimension CASCADE;
        
        """  
    else:
        create_tables_query = """
        
        CREATE TABLE outcomes_fact (
            outcome_key SERIAL PRIMARY KEY,
            date_key INT REFERENCES date_dimension(date_key),
            animal_key INT REFERENCES animal_dimension(animal_key),
            outcome_type_key INT REFERENCES outcome_type_dimension(outcome_type_key)
        );
            """
        drop_tables_query = """
        -- Drop tables if they exist

        DROP TABLE IF EXISTS outcomes_fact CASCADE;
        
        """   


    
    with connection.cursor() as cursor:
        cursor.execute(drop_tables_query)
        cursor.execute(create_tables_query)
    connection.commit()    


def get_sql_connection_details():
    connection = psycopg2.connect(
        host="34.31.21.213",
        user="postgres",
        password="admin123$",
        database="postgres"
    )
    return connection


def load_data_into_table(connection, table_name, data):
    
    queries_string = " "
    if table_name =="animal_dimension":
        animal_dim_data_csv = csv.reader(StringIO(data))
        next(animal_dim_data_csv)
        for animal_row in animal_dim_data_csv:
            animal_id, animal_name, dob, animal_type, sterilization_status, gender, breed, color, animal_key = animal_row
            animal_dimension_insert_query = f"""INSERT INTO animal_dimension (animal_id, animal_name, dob, animal_type, sterilization_status, gender, breed, color, animal_key) VALUES ('{animal_id}', '{animal_name}', '{dob}', '{animal_type}', '{sterilization_status}', '{gender}', '{breed}', '{color}', '{animal_key}');"""       
            queries_string += animal_dimension_insert_query + ' '

    elif table_name =="date_dimension":
        date_dim_data_csv = csv.reader(StringIO(data))
        next(date_dim_data_csv)
        for date_row in date_dim_data_csv:
            date_recorded, day_of_week, month_recorded, quarter_recorded, year_recorded, date_key = date_row
            date_dimension_insert_query = f"""INSERT INTO date_dimension (date_key, date_recorded, day_of_week, month_recorded, quarter_recorded, year_recorded) VALUES ('{date_key}', '{date_recorded}', '{day_of_week}', '{month_recorded}', '{quarter_recorded}', '{year_recorded}');"""
            queries_string += date_dimension_insert_query + ' '


    elif table_name=="outcome_type_dimension":
        outcome_type_dim_data_csv = csv.reader(StringIO(data))
        next(outcome_type_dim_data_csv)
        for outcome_type_row in outcome_type_dim_data_csv:
            outcome_type, outcome_type_key = outcome_type_row
            outcome_dimension_insert_query = f"""INSERT INTO outcome_type_dimension (outcome_type_key, outcome_type) VALUES ('{outcome_type_key}', '{outcome_type}');"""
            queries_string += outcome_dimension_insert_query + ' '

    else:
        fact_outcomes_data_csv = csv.reader(StringIO(data))
        next(fact_outcomes_data_csv)
        for fact_outcome_row in fact_outcomes_data_csv:
            date_key, animal_key, outcome_type_key = fact_outcome_row
            fact_outcome_insert_query = f"""INSERT INTO outcomes_fact (date_key, animal_key, outcome_type_key) VALUES ('{date_key}', '{animal_key}', '{outcome_type_key}');"""
            queries_string += fact_outcome_insert_query + ' '

    with connection.cursor() as cursor:
        cursor.execute(queries_string)
    connection.commit()


def drop_existing_tables():
    connection = get_sql_connection_details()
    drop_tables_query = """
        -- Drop tables if they exist
        DROP TABLE IF EXISTS outcomes_fact CASCADE;
        DROP TABLE IF EXISTS animal_dimension CASCADE;
        DROP TABLE IF EXISTS date_dimension CASCADE;
        DROP TABLE IF EXISTS outcome_type_dimension CASCADE;
        """
    with connection.cursor() as cursor:
            cursor.execute(drop_tables_query)
    connection.commit()

def load_animal_dimension_data():
    connection = get_sql_connection_details()
    bucket_name = 'bucket_lab_03'
    animal_dimension_csv_path = "transformed_data/animal_dim_data.csv"

    # Read data from GCS
    animal_dim_data = read_data_from_gcs(bucket_name, animal_dimension_csv_path)

    create_table(connection,"animal_dimension")

    load_data_into_table(connection, "animal_dimension", animal_dim_data)

    connection.close()

def load_date_dimension_data():
    connection = get_sql_connection_details()
    bucket_name = 'bucket_lab_03'
    dates_dimension_csv_path = "transformed_data/date_dim_data.csv"

    # Read data from GCS
    date_dim_data = read_data_from_gcs(bucket_name, dates_dimension_csv_path)

    create_table(connection,"date_dimension")

    load_data_into_table(connection, "date_dimension", date_dim_data)

    connection.close()

def load_outcome_type_dimesion_data():
    connection = get_sql_connection_details()
    bucket_name = 'bucket_lab_03'
    outcome_types_dimesion_csv_path = "transformed_data/outcome_type_dim_data.csv"

    # Read data from GCS
    outcome_type_dim_data = read_data_from_gcs(bucket_name, outcome_types_dimesion_csv_path)

    create_table(connection,"outcome_type_dimension")

    load_data_into_table(connection, "outcome_type_dimension", outcome_type_dim_data)

    connection.close()

def load_fact_outcomes_data():
    connection = get_sql_connection_details()
    bucket_name = 'bucket_lab_03'
    fact_outcomes_path = "transformed_data/fact_outcomes.csv"

    # Read data from GCS
    fact_outcomes_data = read_data_from_gcs(bucket_name, fact_outcomes_path)

    create_table(connection,"outcomes_fact")

    load_data_into_table(connection, "outcomes_fact", fact_outcomes_data)

    connection.close()