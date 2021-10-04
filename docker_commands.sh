# Initial run
# Retrieve Airflow Docker YAML file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.1.4/docker-compose.yaml'
# Initialise Airflow directory
pushd ~
mkdir airflow
cd airflow
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
AIRFLOW_UID=50000
AIRFLOW_GID=0


# BAU runs can start from here
# This is from https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
docker-compose up airflow-init
docker-compose up
# Login uid: airflow pwd: airflow

# Resetting the Docker environment
pushd ~/airflow
docker-compose down --volumes --remove-orphans
rm -rf ~/airflow
# Start from line 1