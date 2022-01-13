# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from custom_operators.tworp_spark_submit_operator import TwoRPSparkSubmitOperator
from datetime import datetime, timedelta

user = 'hive'
database = 'work_dataeng'

kinit_cmd = "kinit -kt /home/{0}/{0}.keytab {0}".format(user)

default_args = {
    'owner': user,
    'start_date': datetime(2022,1,1),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 0,
    'email': [
        #'blockbusters'@2rpnet.com,
    ],
    'email_on_retry': False,
    'email_on_failure': True,
}

with DAG(dag_id='de_konkatenator_2.0', 
    schedule_interval=None, 
    default_args=default_args, 
    catchup=False) as dag:

    def bash_command(cmd: str, **context):
        
        t_kinit = BashOperator(
            task_id='t_kinit',
            bash_command = kinit_cmd
        )

        t_kinit.execute(context)

        t_bash = BashOperator(
            task_id = 't_bash',
            bash_command = cmd
        )

        t_bash.execute(context)

    t_01_konkatenator = TwoRPSparkSubmitOperator(
        task_id="t_01_konkatenator_{}".format(user),
        name="konkatenator2.0-t_01_konkatenator",
        conn_id="spark_conn",
        application=f"/home/{user}/konkatenator_2.0.py",
        proxy_user=None,
        run_as_user=user,
        keytab="/home/{0}/{0}.keytab".format(user),
        principal=user,
        verbose=True,
        application_args=[user, database, "False"]

    )
    
    t_02_cleanerCheckpoint = PythonOperator(
        task_id='t_02_cleanerCheckpoint_{}'.format(user),
        python_callable = bash_command,
        provide_context = True,
        op_kwargs={'cmd' : 'hdfs dfs -rm -r -skipTrash /user/{0}/sparkCheckpoint'.format(user)}
    )
    
    t_01_konkatenator >> t_02_cleanerCheckpoint