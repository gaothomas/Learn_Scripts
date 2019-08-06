from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta



args = {
    'owner': 'gaoqiang',
    'start_date': datetime(2019, 7, 22),
    'email': 'gaoqiang@lenztechretail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='pg_1907',
    default_args=args,
    schedule_interval='00 02 * * *',
    dagrun_timeout=timedelta(minutes=100))

pgdata=BashOperator(task_id="t1",bash_command='cd /usr/local/airflow/dags/bi_etl/baojie/;nohup python pg_hsm.py &',dag=dag)

sumdata=BashOperator(task_id="t2",bash_command='cd /usr/local/airflow/dags/bi_etl/baojie/;nohup python pg_hsm_07_visualization.py &',dag=dag)

pgdata.set_downstream(sumdata)
