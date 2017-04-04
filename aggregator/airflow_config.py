"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 4, 3),
    'email': ['rubbal@chymeravr.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('Analytics_Aggregator', default_args=default_args, schedule_interval= timedelta(minutes=30))

t1 = BashOperator(
    task_id='ad_event_aggregator',
    bash_command="""/home/hdpuser/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class com.chymeravr.pipeline.aggregator.AdEventAggregator --master spark://10.0.0.6:7077 /home/hdpuser/aggregator-assembly-1.0.jar hdfs://13.88.183.3:9000 /topics/attributed_events/ 8""",
    dag=dag)

t2 = BashOperator(
    task_id='placement_event_aggregator',
    bash_command="""/home/hdpuser/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class com.chymeravr.pipeline.aggregator.PlacementEventAggregator --master spark://10.0.0.6:7077 /home/hdpuser/aggregator-assembly-1.0.jar hdfs://13.88.183.3:9000 /topics/attributed_events/ 8""",
    dag=dag)