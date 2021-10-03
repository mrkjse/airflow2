
"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta
from pprint import pprint
import enum
from math import ceil
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.edgemodifier import Label

# Enum for size units
class SIZE_UNIT(enum.Enum):
   BYTES = 1
   KB = 2
   MB = 3
   GB = 4

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

def spun_group():
    partition_size = Variable.get('monthly_dag_partition_size')
    print('Partition size: {}'.format(partition_size))

    partition_size = int(partition_size)

    if partition_size > 10:
        partition_size = 10

    task_list = []

    for p in range(0, partition_size):
        filename = 'transactions_{}.csv'.format(str(p))

        """
        
        This is just a dummy operator.
        To actually send the data to cloud (ie to GCP Dataproc Cluster HDFS), we could use airflow.providers.google.cloud.operators.dataproc
        More information provided here: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/example_dags/example_dataproc.html

        # [START how_to_cloud_dataproc_hadoop_config]
        HADOOP_JOB = {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "hadoop_job": {
                "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
                "args": ["wordcount", "gs://pub/shakespeare/rose.txt", OUTPUT_PATH],
            },
        }
        # [END how_to_cloud_dataproc_hadoop_config]

        # [START how_to_cloud_dataproc_trigger_workflow_template]
        trigger_workflow = DataprocInstantiateWorkflowTemplateOperator(
            task_id="trigger_workflow", region=REGION, project_id=PROJECT_ID, template_id=WORKFLOW_NAME
        )
        # [END how_to_cloud_dataproc_trigger_workflow_template]

        hadoop_task = DataprocSubmitJobOperator(
        task_id="hadoop_task", job=HADOOP_JOB, region=REGION, project_id=PROJECT_ID
        )


        """
        task_1 = BashOperator(
            task_id = "update_hive_tables_with_partition_" + str(p),
            params = {"filename":filename},
            bash_command='echo "Uploading {{ params["filename"] }}..."',
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
    dag_id='monthly_historical_restate',
    default_args=args,
    schedule_interval='30 1 30 * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=5),
    tags=['ANZ', 'Mark'],
    params={'owner': 'Mark Jose'}
) as dag:

    notify_data_integrity_issue = DummyOperator(task_id="notify_data_integrity_issue")

    check_staging_data_integrity = PythonOperator(
        task_id = 'check_staging_data_integrity',
        provide_context=True,
        python_callable=check_data_integrity,
        op_kwargs={'staging_hdfs_location': '/opt/airflow/plugins/'},
    )


    determine_hdfs_partitions = BranchPythonOperator(
        task_id = 'determine_hdfs_partitions',
        python_callable = branch,
        provide_context=True,
    )

    activate_gcp_cluster = DummyOperator(
        task_id = 'activate_gcp_cluster',
    )

    check_hive_tables = DummyOperator(
        task_id = 'check_hive_tables',
    )

    start = DummyOperator(
        task_id='start',
    )

    end = DummyOperator(
        task_id ='end',
    )

    start >> check_staging_data_integrity >> determine_hdfs_partitions 
    determine_hdfs_partitions  >> activate_gcp_cluster >> check_hive_tables >> spun_group() >> end  
    determine_hdfs_partitions >> notify_data_integrity_issue >> end



if __name__ == "__main__":
    dag.cli()

