from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.operators.http import SimpleHttpOperator
from airflow.operatros.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from pandas import json_normalize
import json

default_args = {
    'start_date' : datetime(2021,4,10),
}

def _processsing_user(ti):

    users = ti.xcom_pull(task_id=['extracting_user'])
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    user = users[0]['results'][0]
    processed_user =json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']

    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, Header=Fales)



with DAG('user_processing', schedule_interval='@daily',
        start_date=datetime(2021,4,10), 
        catchup=False) as dag:
    

    creating_table =SqliteOperator(
        task_id = 'creating_user_table',
        sqlite_conn_id='db_sqlite',
        sql = '''
            CREATE TABLE users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL PRIMARY KEY
            );
        '''
   
   
    is_api_available = HTTPSensor(
       task_id='is_api_available',
       http_conn_id ='user_api',
       endpoint = 'api/'

   )

   extracting_user = SimplerHttpOperator(
       task_id = 'extracting_user',
       http_conn_id ='user_api',
       endpoint='api/'
       method='GET',
       response_filter=lambda response: json.loads(response.text),
       log_response=True
   
    processing_user = PythonOperator(
     task_id = 'processing_user'

    )
    storing_user =BashOperator(
        task_id = 'Storing_user'
        bash_command= 'echo -e ".sparator ","\n import /tmp/processed_user.csv users" | sqllite /home/airflow/airflow/airflow.db'
    )
