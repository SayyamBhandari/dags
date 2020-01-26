from datetime import datetime,timedelta
import airflow
from airflow import DAG
import snowflake.connector
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from SD1 import sub_dag1
from SD2 import sub_dag2
from SD3 import sub_dag3
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance
from airflow.executors import GetDefaultExecutor

def sub_dag_operator_with_default_executor(subdag, *args, **kwargs):
    return SubDagOperator(subdag=subdag, executor=GetDefaultExecutor(), *args, **kwargs)

default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2019, 10, 3),
    'email':['sayyam72@gmail.com'],
    'retries':0,
    'retries_delay':timedelta(minutes=1),
}

main_dag = DAG(
    'Concurrency_Test',
    default_args=default_args,
    schedule_interval='@once',
    )

def copy_command():
    dwh_hook = SnowflakeHook(snowflake_conn_id=args['snowflake_conn_id'])
    result = dwh_hook.get_first("select count(*) from FINANCE_DEV.INGEST.ALL_FILES")
    logging.info("Number of rows in `LOAD.ALL_FILES`  - %s", result[0])
    # a=a/0
def copy_command2():
    dwh_hook = SnowflakeHook(snowflake_conn_id=args['snowflake_conn_id'])
    result = dwh_hook.get_first("select count(*) from FINANCE_DEV.INGEST.ALL_FTG")
    logging.info("LOAD.ALL_FTG`  - %s", result[0])

def taskFailure(context):
    instance = context['task_instance']
    error=context['exception']
    date=str(instance.execution_date)
    cs = con.cursor()
    sql="INSERT INTO JOB_STATUS VALUES('"+instance.dag_id+"','"+instance.task_id+"','"+date+"','"+instance.state+"','"+str(error)+"')"
    print(sql)
    cs.execute(sql)
    cs.close()

def taskSuccess(context):
    instance = context['task_instance']
    date=str(instance.execution_date)
    cs = con.cursor()
    sql="INSERT INTO JOB_STATUS VALUES('%s','%s','%s','%s','NULL')"%(instance.dag_id,instance.task_id,date,instance.state)
    cs.execute(sql)
    cs.close()

sub_dag1 = sub_dag_operator_with_default_executor(
    task_id='SUBDAG1',
    subdag=sub_dag1('Concurrency_Test', 'SUBDAG1', default_args),
    dag=main_dag,
)
t5 = PythonOperator(task_id = "copy_command4",
    python_callable = copy_command,
    dag=main_dag
    )
sub_dag2 = sub_dag_operator_with_default_executor(
    task_id='SUBDAG2',
    subdag=sub_dag2('Concurrency_Test', 'SUBDAG2', default_args),
    dag=main_dag,
)
t11 = PythonOperator(task_id = "copy_command10",
    python_callable = copy_command,
    dag=main_dag
    )
sub_dag3 = sub_dag_operator_with_default_executor(
    task_id='SUBDAG3',
    subdag=sub_dag3('Concurrency_Test', 'SUBDAG3', default_args),
    dag=main_dag,
)

sub_dag1>>t5>>sub_dag2>>t11>>sub_dag3
