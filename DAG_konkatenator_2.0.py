# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from custom_operators.tworp_spark_submit_operator import TwoRPSparkSubmitOperator
from datetime import datetime, timedelta

user = 'hive'
userR = '2rp-leonardos'
database = 'work_dataeng'

kinit_cmd = "kinit -kt /home/{0}/{0}.keytab {0}".format(userR)

default_args = {
    'owner': userR,
    'start_date': datetime(2022,1,1),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 2,
    'run_as_user':userR,
    'proxy_user': userR,
}

with DAG(dag_id='de_konkatenator_2.0.work_dataeng', 
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
        name="konkatenatorParquet-t_01_konkatenator",
        conn_id="spark_conn",
        application=f"/home/{userR}/konkatenator_2.py",
        proxy_user=None,
        run_as_user=userR,
        keytab="/home/{0}/{0}.keytab".format(userR),
        principal=userR,
        verbose=True,
        application_args=[user, userR, database, "False"]

    )
    '''
    t_02_cleanerCheckpoint = PythonOperator(
        task_id='t_02_cleanerCheckpoint_{}'.format(user),
        python_callable = bash_command,
        provide_context = True,
        op_kwargs={'cmd' : 'hdfs dfs -rm -r -skipTrash /user/{0}/sparkCheckpoint'.format(userR)}
    )
    '''
    t_01_konkatenator