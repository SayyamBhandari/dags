from datetime import datetime,timedelta
import airflow
from airflow import DAG
import snowflake.connector
from airflow.operators.python_operator import PythonOperator
from airflow.models import TaskInstance

# Dag is returned by a factory method
def sub_dag2(parent_dag_name, child_dag_name, args):
  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    default_args=args,
    schedule_interval='@once',
  )

  def copy_command():
    for i in range(10):
        a=i

  def taskFailure(context):
    con = snowflake.connector.connect(
    user='sbhandari',
    password='Abz91tweak10!',
    account='lg65488',
    warehouse='AIRBNB_WH',
    database='AIRBNB',
    schema='LOAD'
    )
    instance = context['task_instance']
    error=context['exception']
    date=str(instance.execution_date)
    cs = con.cursor()
    sql="INSERT INTO JOB_STATUS VALUES('"+instance.dag_id+"','"+instance.task_id+"','"+date+"','"+instance.state+"','"+str(error)+"')"
    print(sql)
    cs.execute(sql)
    cs.close()

  def taskSuccess(context):
    con = snowflake.connector.connect(
    user='sbhandari',
    password='Abz91tweak10!',
    account='lg65488',
    warehouse='AIRBNB_WH',
    database='AIRBNB',
    schema='LOAD'
    )
    instance = context['task_instance']
    date=str(instance.execution_date)
    cs = con.cursor()
    sql="INSERT INTO JOB_STATUS VALUES('%s','%s','%s','%s','NULL')"%(instance.dag_id,instance.task_id,date,instance.state)
    cs.execute(sql)
    cs.close()

  t6 = PythonOperator(task_id = "copy_command0",
    python_callable = copy_command,
    on_failure_callback = taskFailure,
    on_success_callback = taskSuccess,
    dag=dag
    )
  t7 = PythonOperator(task_id = "copy_command1",
    python_callable = copy_command,
    on_failure_callback = taskFailure,
    on_success_callback = taskSuccess,
    dag=dag
    )
  t8 = PythonOperator(task_id = "copy_command2",
    python_callable = copy_command,
    on_failure_callback = taskFailure,
    on_success_callback = taskSuccess,
    dag=dag
    )
  t9 = PythonOperator(task_id = "copy_command3",
    python_callable = copy_command,
    on_failure_callback = taskFailure,
    on_success_callback = taskSuccess,
    dag=dag
    )
  [t6,t7,t8,t9]

  return dag