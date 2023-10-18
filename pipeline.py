
# import libraries
import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine



def read_data(source):
    df = pd.read_csv(source)
    return df



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

def transform_data(data):
    transformed_data = data.copy()
    transformed_data[['month_recorded', 'year_recorded']] = transformed_data['MonthYear'].str.split(' ', expand=True)
    transformed_data[['animal_name']] = transformed_data[['Name']].fillna('Name_less')
    transformed_data['Sex upon Outcome'] = transformed_data['Sex upon Outcome'].fillna('Unknown')
    transformed_data['Sex upon Outcome'] = transformed_data['Sex upon Outcome'].replace('Unknown', 'Unknown Unknown')
    transformed_data[['sterilization_status', 'gender']] = transformed_data['Sex upon Outcome'].str.split(' ', expand=True)
    transformed_data['Age upon Outcome'] = transformed_data['Age upon Outcome'].astype(str)
    transformed_data['age_years'] = transformed_data['Age upon Outcome'].apply(age_to_years)
    transformed_data.drop(columns = ['MonthYear', 'Name', 'Sex upon Outcome', 'Age upon Outcome', 'Outcome Subtype'], axis=1, inplace=True)
    transformed_data['DateTime'] = pd.to_datetime(transformed_data['DateTime'])
    transformed_data['day_of_week'] = transformed_data['DateTime'].dt.day_name()
    transformed_data['quarter_recorded'] = transformed_data['DateTime'].dt.month.apply(map_month_to_quarter)
    transformed_data['DateTime'] = pd.to_datetime(transformed_data['DateTime'])
    transformed_data['DateTime'] = transformed_data['DateTime'].dt.date
    transformed_data['Outcome Type'] = transformed_data['Outcome Type'].fillna('Not_Available')
    cols_mapping = {
    'Animal ID': 'animal_id',
    'DateTime': 'date_recorded',
    'Date of Birth': 'dob',
    'Outcome Type': 'outcome_type',
    'Animal Type': 'animal_type',
    'Breed': 'breed',
    'Color': 'color'
    }
    transformed_data.rename(columns=cols_mapping, inplace=True)


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


def export_data(new_data):

    animal_dim_data = prepare_animal_dim_data(new_data)
    date_dim_data = prepare_date_dim_data(new_data)
    outcome_type_dim_data = prepare_outcome_type_dim_data(new_data)

    db_url = "postgresql+psycopg2://mahi:admin123$@db:5432/shelter"
    connection = create_engine(db_url)
    animal_dim_data.to_sql("animal_dimension", connection, if_exists="append", index=False)
    date_dim_data.to_sql("date_dimension", connection, if_exists="append", index=False)
    outcome_type_dim_data.to_sql("outcome_type_dimension", connection, if_exists="append", index=False)

    # Create or append data to the Outcomes_Fact table, linking to dimension tables
    df_fact = new_data.merge(date_dim_data, how='inner', left_on='date_recorded', right_on='date_recorded')
    df_fact = df_fact.merge(animal_dim_data, how='inner', left_on='animal_id', right_on='animal_id')
    df_fact = df_fact.merge(outcome_type_dim_data, how='inner', left_on='outcome_type', right_on='outcome_type')

    # Map the merged DataFrame columns to the table columns
    df_fact.rename(columns={
        'date_key': 'date_key',
        'animal_key': 'animal_key',
        'outcome_type_key': 'outcome_type_key'
    }, inplace=True)


    # Create or append data to the Outcomes_Fact table
    df_fact[['date_key', 'animal_key', 'outcome_type_key']].to_sql('outcomes_fact', connection, if_exists='append', index=False)



if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('source', help='source csv')
#    args_parser.add_argument('target', help=('target file'))
    args = args_parser.parse_args()

    print("Start processing....")
    data = read_data(args.source)
    new_data = transform_data(data)
    export_data(new_data)
    print("Finished processing.....")


