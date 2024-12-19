import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.ifpb_egressos import (
    ingest_egressos_data_from_suap,
    landing_to_raw_egressos_suap_data,
    raw_to_trusted_egressos_suap_data,
    ingest_egressos_data_from_linkedin,
    landing_to_raw_egressos_linkedin_data,
    raw_to_trusted_egressos_linkedin_data,
)


with DAG(
    dag_id="ifpb-egressos-elt",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["suap", "linkedin"],
) as dag:
    
    ############ SUAP ############
    
    task_ingest_egressos_data_from_suap = PythonOperator(
        task_id="task-ingest-egressos-data-from-suap", 
        python_callable=ingest_egressos_data_from_suap.main
    )

    task_landing_to_raw_egressos_suap_data = PythonOperator(
        task_id="task-landing-to-raw-egressos-suap-data",
        python_callable=landing_to_raw_egressos_suap_data.main
    )

    task_to_trusted_egressos_suap_data = PythonOperator(
        task_id="task-raw-to-trusted-egressos-suap-data",
        python_callable=raw_to_trusted_egressos_suap_data.main
    )

    ############ LINKEDIN ############

    task_ingest_egressos_data_from_linkedin = PythonOperator(
        task_id="task-ingest-egressos-data-from-linkedin",
        python_callable=ingest_egressos_data_from_linkedin.main
    )

    task_landing_to_raw_egressos_linkedin_data = PythonOperator(
        task_id="task-landing-to-raw-egressos-linkedin-data",
        python_callable=landing_to_raw_egressos_linkedin_data.main
    )

    task_raw_to_trusted_egressos_linkedin_data = PythonOperator(
        task_id="task-raw-to-trusted-egressos-linkedin-data",
        python_callable=raw_to_trusted_egressos_linkedin_data.main
    )


(
    task_ingest_egressos_data_from_suap 
    >> task_landing_to_raw_egressos_suap_data 
    >> task_to_trusted_egressos_suap_data
    >> task_ingest_egressos_data_from_linkedin 
    >> task_landing_to_raw_egressos_linkedin_data 
    >> task_raw_to_trusted_egressos_linkedin_data
)