export AIRFLOW_HOME=$PWD/airflow
echo $AIRFLOW_HOME

airflow users delete -u admin
airflow users create \
    --username admin \
    --password admin \
    --firstname Olenya \
    --lastname Zapumchakovna \
    --role Admin \
    --email spiderman@superhero.org

airflow scheduler &
airflow webserver -p 8080 
