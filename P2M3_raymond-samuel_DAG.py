'''
=================================================
Milestone 3

Nama  : Raymond Samuel
Batch : FTDS-030-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset HR. Analitik HR membantu kita dalam menginterpretasikan data organisasi. Analitik ini menemukan tren yang terkait dengan orang-orang dalam data dan memungkinkan Departemen HR untuk mengambil langkah-langkah yang tepat untuk menjaga organisasi berjalan dengan lancar dan menguntungkan. Attrisi dalam setup korporat adalah salah satu tantangan kompleks yang harus dihadapi oleh manajer orang dan personel HR.
=================================
'''

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.task_group import TaskGroup
import datetime as dt
from datetime import timedelta
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd
import re

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def load_csv_to_postgres():

    '''
    Fungsi ini ditujukan untuk mengambil import data dari csv ke PostgreSQL.
    '''

    database = "project_m3"
    username = "airflow"
    password = "airflow"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/P2M3_raymond-samuel_data_raw.csv')
    #df.to_sql(nama_table, conn, index=False, if_exists='replace')
    df.to_sql('table_m3', conn, index=False, if_exists='replace')  # M
    
def get_data():
    '''
    Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.
    '''
    # fetch data
    database = "project_m3"
    username = "airflow"
    password = "airflow"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query("select * from table_m3", conn) #nama table sesuaikan sama nama table di postgres
    df.to_csv('/opt/airflow/dags/P2M3_raymond-samuel_data_raw_postgres.csv', sep=',', index=False)
    
def preprocessing(): 
    ''' 
    Fungsi untuk membersihkan data / data cleaning
    '''
    # pembisihan data
    data = pd.read_csv("/opt/airflow/dags/P2M3_raymond-samuel_data_raw_postgres.csv")

    # bersihkan data 
    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)
    data.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', x).lower() for x in data.columns] 
    data.to_csv('/opt/airflow/dags/P2M3_raymond-samuel_data_clean.csv', index=False)
    
def upload_to_elasticsearch():
    ''' 
    Fungsi untuk upload data ke elasticsearch
    '''

    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_raymond-samuel_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        
        
default_args = {
    'owner': 'raymond', 
    'start_date': dt.datetime(2024, 5, 24) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=15),
}

with DAG(
    "P2M3_raymond-samuel_DAG", #atur sesuai nama project kalian
    description='Milestone_3_DAG',
    schedule_interval='30 6 * * *', #atur schedule untuk menjalankan airflow pada 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    # Task : 1
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) #sesuaikan dengan nama fungsi yang kalian buat
    
    #task: 2
    data_get = PythonOperator(
        task_id='get_data',
        python_callable=get_data) #
    
    # Task: 3
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    data_clean = PythonOperator(
        task_id='data_cleaning',
        python_callable=preprocessing)

    # Task: 4
    data_upload = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    # proses untuk menjalankan di airflow
    load_csv_task >> data_get >> data_clean >> data_upload



