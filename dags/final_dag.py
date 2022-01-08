from airflow import DAG
from datetime import datetime, timedelta
from airflow.operator.python_operator import PythonOperator
from airflow.operator.mysql_operator import MySqlOperator
from airflow.operator.bash_operator import BashOperator

from pull_


yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
  'owner':'Spark_Airflow',
  'start_date':datetime(2022, 1, 7),
  'retries':1,
  'retry_delay': timedelta(seconds=5)
}

with DAG('spark_dag', default_args=default_args, schedule_interval='@daily', template_searchpath=['<local_path>/sql_files'], catchup=True) as dag:
  
  t1=PythonOperator(task_id='users_subscription_data_pull', python_callable=users_subscription_data_pull)
  t2=PythonOperator(task_id='messages_data_pull', python_callable=messages_data_pull)
  
  t3=MySqlOperator(task_id='create_mysql_users_sql', mysql_conn_id="mysql_conn", sql="create_mysql_users_sql.sql")
  t4=MySqlOperator(task_id='create_mysql_subscriptions_sql', mysql_conn_id="mysql_conn", sql="create_mysql_subscriptions_sql.sql")
  t5=MySqlOperator(task_id='create_mysql_messages_sql', mysql_conn_id="mysql_conn", sql="create_mysql_messages_sql.sql")
  
  t6=MySqlOperator(task_id='merge_load_mysql_users_sql', mysql_conn_id="mysql_conn", sql="merge_load_mysql_users_sql.sql")
  t7=MySqlOperator(task_id='merge_load_mysql_subscriptions_sql', mysql_conn_id="mysql_conn", sql="merge_load_mysql_subscriptions_sql.sql")
  t8=MySqlOperator(task_id='merge_load_mysql_messages_sql', mysql_conn_id="mysql_conn", sql="merge_load_mysql_messagess_sql.sql")
  
  t9=BashOperator(task_id='rename_users_file', bash_command='mv <location_path_>/users.csv <location_path_>/users_%s.csv'%yesterday_date)
  t10=BashOperator(task_id='rename_subscriptions_file', bash_command='mv <location_path_>/subscriptions.csv <location_path_>/subscriptions_%s.csv'%yesterday_date)
  t11=BashOperator(task_id='rename_messages_file', bash_command='mv <location_path_>/messages.csv <location_path_>/messages_%s.csv'%yesterday_date)
  
  [t1, t2] >> [t3, t4, t5] >> [t6, t7, t8] >> [t9, t10, t11]
  
  
