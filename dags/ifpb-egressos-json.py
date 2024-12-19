import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.ifpb_egressos import ingest_ifpb_egressos_json


with DAG(
    dag_id="ifpb-egressos-json",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["suap", "linkedin"],
) as dag:
    
    ############ SUAP ############
    
    task_ingest_ifpb_egressos_json = PythonOperator(
        task_id="task-ingest-ifpb-egressos-json", 
        python_callable=ingest_ifpb_egressos_json.main
    )

    task_ingest_ifpb_egressos_json