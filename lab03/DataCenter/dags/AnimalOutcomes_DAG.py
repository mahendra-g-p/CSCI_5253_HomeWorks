import os
import sys
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


# code_path = "/root/demo/lab03/etl_scripts"
# sys.path.insert(0, code_path)

from etl_helpers.gcs_data_processing import transform_data
from etl_helpers.api_data_extractor_and_loader import data_extraction
from etl_helpers.gcs_to_postgresql import drop_existing_tables, load_animal_dimension_data, load_date_dimension_data, load_outcome_type_dimesion_data, load_fact_outcomes_data

# AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
# CREDS_TARGET_DIR = AIRFLOW_HOME + '/warm-physics-405522-a07e9b7bfc0d.json'

# with open(CREDS_TARGET_DIR, 'r') as f:
#     credentials_content = f.read()


default_args = {
    "owner": "mahendra.gajulapothamsetty",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}


with DAG(
    dag_id="outcomes_dag",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
        start = BashOperator(task_id = "START",
                             bash_command = "echo start")

        # copy_creds = BashOperator(task_id = "COPY_CREDS", bash_command = "echo start")

        extract_api_data_to_gcs =  PythonOperator(task_id = "EXTRACT_API_DATA_TO_GCS",
                                                  python_callable = data_extraction,)

        transform_data_step = PythonOperator(task_id="TRANSFORM_DATA",
                                             python_callable=transform_data,)
        
        #load_data_to_postgres = PythonOperator(task_id="LOAD_DATA",
        #                                     python_callable=load_data_into_postgres,)

        drop_existing_tables_data = PythonOperator(task_id="DROP_EXISTING_TABLES_DATA",
                                             python_callable=drop_existing_tables,)

        load_animal_dimension_data = PythonOperator(task_id="LOAD_ANIMALS_DIMENSION",
                                             python_callable=load_animal_dimension_data,)

        load_dates_dimension_data = PythonOperator(task_id="LOAD_DATES_DIMENSION",
                                             python_callable=load_date_dimension_data,)

        load_outcome_types_dimension_data = PythonOperator(task_id="LOAD_OUTCOME_TYPES_DIMENSION",
                                             python_callable=load_outcome_type_dimesion_data,)
        
        load_fact_outcomes_data = PythonOperator(task_id="LOAD_FACT_OUTCOMES",
                                             python_callable=load_fact_outcomes_data,)
        
        end = BashOperator(task_id = "END", bash_command = "echo end")

        #start >> extract_api_data_to_gcs >> transform_data_step >> [load_dim_animals, load_dim_outcome_types, load_dim_dates, load_fct_outcomes] >> end
        start >> extract_api_data_to_gcs >> transform_data_step >> drop_existing_tables_data >> load_animal_dimension_data >> load_dates_dimension_data >> load_outcome_types_dimension_data >> load_fact_outcomes_data >> end
        