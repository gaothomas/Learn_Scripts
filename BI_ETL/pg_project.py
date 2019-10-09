from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta



args = {
    'owner': 'gaoqiang',
    'start_date': datetime(2019, 9, 19),
    'email': 'gaoqiang@lenztechretail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='pg_hsm',
    default_args=args,
    schedule_interval='00 02 * * *',
    dagrun_timeout=timedelta(minutes=100))

t1=BashOperator(task_id="pg_w",bash_command='cd /usr/local/airflow/dags/bi_etl/baojie/;nohup python pg_hsm_w.py &',dag=dag)

t2=BashOperator(task_id="pg_w_m",bash_command='cd /usr/local/airflow/dags/bi_etl/baojie/;nohup python pg_hsm_w_mail.py &',dag=dag)

t3=BashOperator(task_id="pg_p",bash_command='cd /usr/local/airflow/dags/bi_etl/baojie/;nohup python pg_hsm_p.py &',dag=dag)

t4=BashOperator(task_id="pg_p_m",bash_command='cd /usr/local/airflow/dags/bi_etl/baojie/;nohup python pg_hsm_p_mail.py &',dag=dag)

t5=BashOperator(task_id="pg_cp",bash_command='cd /usr/local/airflow/dags/bi_etl/baojie/;nohup python pg_hsm_cp.py &',dag=dag)

t6=BashOperator(task_id="pg_cp_m",bash_command='cd /usr/local/airflow/dags/bi_etl/baojie/;nohup python pg_hsm_cp_mail.py &',dag=dag)

t1.set_downstream(t2)
t3.set_downstream(t4)
t5.set_upstream([t2, t4])
t5.set_downstream(t6)
