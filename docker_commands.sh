# This is from https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
# The link above contains info on how to install Docker and Apache Airflow

# Initial run
# Retrieve Airflow Docker YAML file
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.1.4/docker-compose.yaml'
# Initialise Airflow directory
pushd ~
mkdir airflow2
cd airflow2
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
AIRFLOW_UID=50000
AIRFLOW_GID=0


# BAU runs can start from here
pushd ~/airflow2
docker-compose up airflow-init
docker-compose up
# Go to localhost:8080
# Login uid: airflow pwd: airflow

# Resetting the Docker environment
pushd ~/airflow2
docker-compose down --volumes --remove-orphans
rm -rf ~/airflow2
# Start from line 6