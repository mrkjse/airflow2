
"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta, datetime
from pprint import pprint
import enum
from math import ceil
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.edgemodifier import Label

# Enum for size units
class SIZE_UNIT(enum.Enum):
   BYTES = 1
   KB = 2
   MB = 3
   GB = 4


PROJECT_ID = 'mark-sample-327500'
CLUSTER_NAME = 'cluster-e134'
REGION = 'australia-southeast1'
ZONE = 'australia-southeast1-c'
BUCKET = 'dataproc-staging-au-southeast1-488016609135-qh7iyzfd'
BUCKET_NAME = 'mark-bucket-2021-2'
GCP_CONN_NAME = 'MarkGCP'
DATASET_NAME = 'transactions_data'
TABLE_NAME = 'transactions'


def convert_unit(size_in_bytes, unit):
   """ Convert the size from bytes to other units like KB, MB or GB """
   if unit == SIZE_UNIT.KB:
       return size_in_bytes/1024
   elif unit == SIZE_UNIT.MB:
       return size_in_bytes/(1024*1024)
   elif unit == SIZE_UNIT.GB:
       return size_in_bytes/(1024*1024*1024)
   else:
       return size_in_bytes

def get_file_size(file_name, size_type = SIZE_UNIT.BYTES ):
   """ Get file in size in given unit like KB, MB or GB"""
   size = os.path.getsize(file_name)
   return convert_unit(size, size_type)


def check_data_integrity(ti, **kwargs):
    """
    This Python function will perform some integrity checks in the flat file.

    Examples: Check shape, check columns, check duplicate transactions etc.

    ---
    Parameters
    ---
    input_location - str
                   - the location of the transactions flat file into 30 GB chunks.
    
    input_file - str
               - the filename of the transactions flat file.

    ---
    Returns
    ---
    validation_result - list
                      - a list of dictionaries that shows any issues/warnings in the data

    """

    # Validate data here...

    validation_result = True

    ti.xcom_push(key='monthly_dag_validation_result', value=validation_result)

    return validation_result


def branch(**kwargs):
    """
    This Python function will decide whether to proceed with uploading the data
    into the cloud or not, based on the validation result.

    """
    allow = Variable.get('monthly_dag_validation_result')
    allow = bool(allow)

    if allow:
        return 'upload_to_gcs'
    else:
        return 'notify_data_integrity_issue'

args = {
    'owner': 'airflow',
    'email': ['mrk.jse@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}



with DAG(
    dag_id='daily_upload_datawarehouse_transactions',
    default_args=args,
    schedule_interval='30 0 1 * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=5),
    tags=['ANZ', 'Mark'],
    params={'owner': 'Mark Jose'}
) as dag:

    notify_data_integrity_issue = DummyOperator(task_id="notify_data_integrity_issue")

    check_transaction_data_integrity = PythonOperator(
        task_id = 'check_transaction_data_integrity',
        provide_context=True,
        python_callable=check_data_integrity,
        op_kwargs={'input_location': '/opt/airflow/dags/data/',
            'input_file': 'bank_transactions.csv'},
    )

    # determine_partition = PythonOperator(
    #     task_id = 'determine_partition',
    #     provide_context=True,
    #     python_callable=partition_this_csv,
    #     op_kwargs={'input_location': '/opt/airflow/dags/data/',
    #         'input_file': 'bank_transactions.csv'},
    # )

    # partition_files = BashOperator(
    #      task_id="partition_files",
    #      bash_command= 'cd /opt/airflow/dags/data/ && split -b 9000000 bank_transactions_daily.csv partitions_daily/transactions_daily_',
    #      params={'input_location': 'plugins',
    #      'input_file': 'bank_transactions.csv'},
    #  )

    upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id = 'upload_to_gcs',
            src = '/opt/airflow/dags/data/bank_transactions_daily.csv',
            dst = 'staging_daily/transactions_daily_' + datetime.now().strftime('%Y%m%d'), 
            bucket = BUCKET_NAME,
            gcp_conn_id = GCP_CONN_NAME,
            mime_type = 'text/plain'
        )
    
    upload_to_bigquery = GCSToBigQueryOperator(
            task_id = "upload_to_bigquery",
            bucket = BUCKET_NAME,
            source_objects = ['staging_daily/transactions_daily_' + datetime.now().strftime('%Y%m%d')],
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


    determine_integrity = BranchPythonOperator(
        task_id = 'determine_integrity',
        python_callable = branch,
        provide_context=True,
    )



    start = DummyOperator(
        task_id='start',
    )

    end = DummyOperator(
        task_id ='end',
    )

    check_hdfs_directory = DummyOperator(
        task_id='check_hdfs_directory'
    )
    start >> check_transaction_data_integrity >> determine_integrity
    determine_integrity  >> upload_to_gcs >> upload_to_bigquery >> end  
    determine_integrity >> notify_data_integrity_issue >> end





if __name__ == "__main__":
    dag.cli()

