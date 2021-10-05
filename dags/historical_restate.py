from datetime import timedelta
from pprint import pprint
import enum
from math import ceil
import os
import logging
import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

PROJECT_ID = 'mark-sample-327500'
CLUSTER_NAME = 'cluster-e134'
REGION = 'australia-southeast1'
ZONE = 'australia-southeast1-c'
BUCKET = 'dataproc-staging-au-southeast1-488016609135-qh7iyzfd'
BUCKET_NAME = 'mark-bucket-2021-2'
GCP_CONN_NAME = 'MarkGCP'
DATASET_NAME = 'transactions_data'
TABLE_NAME = 'transactions'


def branch(**kwargs):
    """
    This Python function will decide whether to proceed with uploading the data
    into the cloud or not, based on the validation result.

    """
    allow = Variable.get('monthly_dag_validation_result')
    allow = bool(allow)

    if allow:
        return 'activate_gcp_cluster'
    else:
        return 'notify_data_integrity_issue'

def spun_group(**kwargs):

    """
    
    This function will upload the partitions into Google Bigquery Table.

    This will spun individual tasks to upload the file into Bigquery, depending on the number of partitions.

    """

    file_names = Variable.get('monthly_dag_gcs_filenames', deserialize_json=True)


    task_list = []
    index = 0
    for p in file_names:

        # Do other validation here
        if 'transactions' in p:
            task_1 = GCSToBigQueryOperator(
                task_id = "upload_to_bigquery_" + str(index),
                bucket = BUCKET_NAME,
                source_objects = [p],
                google_cloud_storage_conn_id = GCP_CONN_NAME,
                bigquery_conn_id = GCP_CONN_NAME,
                max_bad_records=1000,
                destination_project_dataset_table = f"{DATASET_NAME}.{TABLE_NAME}",
                schema_fields=[
                    {'name': 'Date', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'Description', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'Deposits', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'Withdrawls', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'Balance', 'type': 'STRING', 'mode': 'NULLABLE'}
                ],
                write_disposition = 'WRITE_APPEND',
            )

            task_list.append(task_1)
            index = index + 1

        # Ensure we don't spun too much tasks
        if index > 12:
            break
    
    return task_list

def print_gcs_files(ti):
    file_names = ti.xcom_pull(task_ids='check_gcs_files', dag_id='monthly_upload_datawarehouse_transactions', key='return_value')
    file_names = [x for x in file_names if 'transaction' in x]
    logging.info('GCS Files: \n' + '\n'.join(file_names))
    val = Variable.set('monthly_dag_gcs_filenames', file_names, serialize_json=True)

args = {
    'owner': 'airflow',
    'email': ['mrk.jse@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}



with DAG(
    dag_id='monthly_upload_datawarehouse_transactions',
    default_args=args,
    schedule_interval='30 1 L * *', # Every 1:30 AM on the last day of the month
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=5),
    tags=['ANZ', 'Mark'],
    params={'owner': 'Mark Jose'}
) as dag:


    check_gcs_files = GCSListObjectsOperator(
        task_id='check_gcs_files',
        bucket=BUCKET_NAME,
        prefix='staging/',
        google_cloud_storage_conn_id=GCP_CONN_NAME,
    )

    confirm_gcs_files = PythonOperator(
        task_id='confirm_gcs_files',
        provide_context=True,
        python_callable=print_gcs_files,
    )

    start = DummyOperator(
        task_id='start',
    )

    end = DummyOperator(
        task_id ='end',
    )

    start >> check_gcs_files 
    check_gcs_files  >>  confirm_gcs_files >> spun_group() >> end  


if __name__ == "__main__":
    dag.cli()

