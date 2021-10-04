
"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta
from datetime import datetime
from pprint import pprint
import enum
from math import ceil
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
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


def convert_unit(size_in_bytes, unit):
   """ Convert the size from bytes to other units like KB, MB or GB"""
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

    validation_result = True

    ti.xcom_push(key='monthly_dag_validation_result', value=validation_result)

    return validation_result


def partition_this_csv(ti, **kwargs):
    """
    This Python function will partition the transactions flat file into 30 GB chunks.

    ---
    Parameters
    ---
    input_location - str
                   - the location of the transactions flat file into 30 GB chunks.
    
    input_file - str
               - the filename of the transactions flat file.

    """

    filename = kwargs['input_location'] + kwargs['input_file']
    return_val = ''
    partition_size = 0

    if os.path.exists(filename):

        partition_size = get_file_size(filename, SIZE_UNIT.MB)
        partition_size = ceil(partition_size/4) # assuming this is 30 Gb

        print('This function will partition {} into {} files.'.format(filename, str(partition_size)))

    else:
        raise ValueError('Invalid filename. Please check input_location and input_file.')
    
    ti.xcom_push(key='return_val', value=return_val)
    ti.xcom_push(key='partition_size', value=partition_size)
    partition_size = Variable.set("monthly_dag_partition_size", partition_size)

def branch(**kwargs):
    partition_size = Variable.get('monthly_dag_partition_size')
    print('Partition size: {}'.format(partition_size))

    partition_size = int(partition_size)
    print(partition_size)

    if partition_size > 5:
        return 'partition_files'
    else:
        return 'notify_data_integrity_issue'

def spun_group():
    """
    
    This function will upload the partitions into Google Cloud Storage.

    """
    partition_size = Variable.get('monthly_dag_partition_size')
    print('Partition size: {}'.format(partition_size))

    partition_size = int(partition_size)
    
    # Due to Airflow constraints, we can only spun a maximum of 10 parallel tasks at a time
    # But with an upgraded configuration, we can definitely spun more tasks to upload all 600 GB
    # Otherwise, we can further subdivide the DAG to run 10 tasks at a time to overcome this limitation
    if partition_size > 10:
        partition_size = 10

    task_list = []

    files_to_upload = os.listdir('/opt/airflow/dags/data/partitions')

    for p in files_to_upload[:partition_size]:

        task_1 = LocalFilesystemToGCSOperator(
            task_id = 'upload_to_gcs_' + p,
            src = '/opt/airflow/dags/data/partitions/' + p,
            dst = 'staging/' + p + '_' + datetime.now().strftime('%Y%m%d'), 
            bucket = BUCKET_NAME,
            gcp_conn_id = GCP_CONN_NAME,
            mime_type = 'text/plain'
        )

        task_list.append(task_1)
    
    return task_list

args = {
    'owner': 'airflow',
    'email': ['mrk.jse@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}



with DAG(
    dag_id='monthly_upload_transactions_for_enrichment',
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

    determine_partition = PythonOperator(
        task_id = 'determine_partition',
        provide_context=True,
        python_callable=partition_this_csv,
        op_kwargs={'input_location': '/opt/airflow/dags/data/',
            'input_file': 'bank_transactions.csv'},
    )

    partition_files = BashOperator(
         task_id="partition_files",
         bash_command= 'cd /opt/airflow/dags/data/ && split -b 4000000 bank_transactions.csv partitions/transactions_',
         params={'input_location': 'plugins',
         'input_file': 'bank_transactions.csv'},
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
    start >> check_transaction_data_integrity >> determine_partition >> determine_integrity
    determine_integrity  >> partition_files >> spun_group() >> end  
    determine_integrity >> notify_data_integrity_issue >> end



if __name__ == "__main__":
    dag.cli()

