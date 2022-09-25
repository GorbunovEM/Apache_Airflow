import sqlite3
from sqlalchemy import create_engine
import os

def sql_query(sql, conn):
  if 'select' in sql[0:6]:
    return conn.execute(sql).fetchall()
  else:
    conn.execute(sql)

CONN = create_engine('sqlite:///exchange.db', echo=False)
sql_currency = 'create table currency (date VARCHAR(12), code VARCHAR(5), rate FLOAT, base VARCHAR(5), start_date VARCHAR(12), end_date VARCHAR(12))'
sql_data = 'create table data (currency VARCHAR(5), value INT, date VARCHAR(12))' 
sql_join = 'create table join_data (currency VARCHAR(5), value INT, date VARCHAR(12), rate FLOAT)'

sql_query(sql_currency, CONN)
sql_query(sql_data, CONN)
sql_query(sql_join, CONN)

os.makedirs('/content/root/airflow/dags/csv', exist_ok=True)


from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import requests
import re
import datetime
from pathlib import Path
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.email_operator import EmailOperator
from airflow import AirflowException
from airflow.providers.telegram.operators.telegram import TelegramOperator

dict_ = {'currency' : '/content/root/airflow/dags/csv/out_cur.csv'}

def extract_currency(sdate, edate):  
  start_date = sdate
  end_date = edate
  base='EUR'
  symbols='USD'
  format='csv'
  url = f'https://api.exchangerate.host/' + \
        f'timeseries?start_date={start_date}&end_date={end_date}&base={base}' + \
        f'&symbols={symbols}&format={format}'
  
  response = requests.get(url)
  
  data = pd.read_csv(url)  
#  Path('/content/root/airflow/dags/csv/out_cur.csv').touch()
#  data.to_csv('/content/root/airflow/dags/csv/out_cur.csv', index=False)
  return list(data['rate'])[0]

def extract_data(sdate):
  date = sdate
  url = f'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data_new/{date}.csv'
  response = requests.get(url)
  data = pd.read_csv(url)  
  Path('/content/root/airflow/dags/csv/out_dat_{0}.csv'.format(date)).touch()  
  data.to_csv('/content/root/airflow/dags/csv/out_dat_{0}.csv'.format(date), index=False) 


def insert_to_db(**kwargs):
  data = kwargs['data']
#  currency = kwargs['currency']
  df_data = pd.read_csv(data + 'out_dat_{0}.csv'.format(kwargs['sdate']))
#  df_currency = pd.read_csv(currency)
  conn = create_engine('sqlite:////content/exchange.db', echo=False)
  df_data.to_sql('data', con=conn, if_exists='append', index=False)
#  df_currency.to_sql('currency', con=conn, if_exists='append', index=False)


def join_table(**kwargs):
  #rate = float(kwargs['n'].split("'")[1].replace(',','.'))
  conn = create_engine('sqlite:////content/exchange.db', echo=False)
  dat = f"'{kwargs['sdate']}'"
  ds = pd.read_sql(sql = "SELECT * FROM data WHERE date ="+dat, con = conn)
  #ds['rate'] = rate
  ds.to_sql('join_data', con=conn, if_exists='append', index=False)

def exception():
  raise AirflowException

def on_failure_callback(context):
  send_message = TelegramOperator(
    task_id = 'send_message',
    telegram_conn_id = 'telegram_default',
    chat_id = '-1001726782936',
    token = 'xxxxxx',
    text = 'Hello from Airflow!',
    dag=dag)
  return send_message.execute(context=context)

dag = DAG('dag',
          default_args={'owner': 'airflow'},
          schedule_interval='@daily',
          start_date=datetime.datetime(2021, 1, 1),
          end_date=datetime.datetime(2021, 1, 4),
          on_failure_callback=on_failure_callback)

t0 = PythonOperator(
  task_id='t0',
  provide_context=True,
  python_callable=extract_currency,
  op_kwargs = {'sdate': '{{ ds }}',
  'edate': '{{ ds }}'},
  dag=dag
)

t1 = PythonOperator(
  task_id='t1',
  provide_context=True,
  python_callable=extract_data,
  op_kwargs = {'sdate': '{{ ds }}'},
  dag=dag
)

t2 = PythonOperator(
  task_id='t2',
  provide_context=True,
  python_callable=insert_to_db,
  op_kwargs = {'data' : '/content/root/airflow/dags/csv/',
  'sdate': '{{ ds }}'},
  dag=dag
)

t3 = PythonOperator(
  task_id='t3',
  provide_context=True,
  python_callable=join_table,
  op_kwargs = {'n' : """ {{ti.xcom_pull(key='return_value')}} """,
  'sdate': '{{ ds }}'},
  dag=dag
)

t4 = PythonOperator(
  task_id='t4',
  provide_context=True,
  python_callable=exception,
  dag=dag
)

#send_message = TelegramOperator(
#    task_id = 'send_message',
#    chat_id = '-xxxxxxx',
#    text = 'Hello from Airflow!',
#    token = 'xxxxxxx',
#    trigger_rule='one_failed',
#    dag=dag)

join_data = SqliteOperator(
    task_id='join_data',
    sql="""UPDATE join_data SET rate = {{ti.xcom_pull(key='return_value').replace(',','.')}} WHERE date = '{{ ds }}'""",
    dag=dag,
)

#email_op = EmailOperator(
#        task_id='send_email',
#        to="air_flow@ro.ru",
#        subject="Test Email Please Ignore",
#        html_content=None,
#        files=['/content/exchange.db'],
#        dag=dag
#    )

t0 >> t1 >> t2 >> t3 >> t4 >> join_data
