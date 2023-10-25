#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# Define the base path
base_path = '\home\projet'

# Define default_args
default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 25),
    'retries': 1,
}

# Create a DAG
dag = DAG(
    'mastodon_data_processing',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,
)

# Task to run the data collection script
collect_data_task = BashOperator(
    task_id='collect_data',
    bash_command=f'python3 {base_path}/Collecting.py',
    dag=dag,
)

# Task to run the Hadoop MapReduce job
mapreduce_task = BashOperator(
    task_id='run_mapreduce',
    bash_command=f'hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar -files {base_path}/mapper.py,{base_path}/reducer.py -mapper "python3 {base_path}/mapper.py" -reducer "python3 {base_path}/reducer.py" -input /Mostodon/Raw/mastodon_dataaa_2023-10-24.json -output /output/ReducerResulttt',
    dag=dag,
)

# Task to start the HBase Thrift server
start_thrift_task = BashOperator(
    task_id='start_thrift',
    bash_command='/usr/local/Hbase/bin/hbase-daemon.sh start thrift',
    dag=dag,
)

# Task to create HBase tables
create_hbase_tables_task = BashOperator(
    task_id='create_hbase_tables',
    bash_command=f'/usr/local/Hbase/bin/hbase shell -n {base_path}/create_tables.hbase',
    dag=dag,
)

# Task to run the HBase script
hbase_script_task = BashOperator(
    task_id='run_hbase_script',
    bash_command=f'python3 {base_path}/hbase_script.py',
    dag=dag,
)

# Set task dependencies
collect_data_task >> mapreduce_task >> start_thrift_task >> create_hbase_tables_task >> hbase_script_task

if __name__ == "__main__":
    dag.cli()

