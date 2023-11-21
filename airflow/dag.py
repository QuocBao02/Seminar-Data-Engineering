# from datetime import timedelta
# from time import sleep
from datetime import timedelta 
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# def my_python_function():
#     # Your Python code goes here
#     sleep(30)  # Sleep for 30 seconds

target_time = 18
hour=target_time - 7 # 7 is the time zone of VietNam 

# define DAG arguments 
default_args={
    'owner': 'Quoc Bao',
    'start_date': days_ago(1),
    'email': ['baonguyen022002499@gmail.com'], 
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# define the DAG 
dag=DAG(
    dag_id="Ingest_Extract_Transform_Load_Binance_Market_Data",
    default_args=default_args,
    description=" Auto Ingest Data from Binance into DataLake, ETL into Datawarehouse",
    schedule_interval=f"06 {hour} * * *",
)

# # Define the Python operator with a sleep
# delay_task = PythonOperator(
#     task_id='delay_task',
#     python_callable=my_python_function,
#     dag=dag,
# )


# # define the start_hdfs
# start_hdfs= BashOperator(
#     task_id='start_hdfs', 
#     bash_command=["sudo", "-u", "hadoop", "/usr/local/hadoop/sbin/start-all.sh"],
#     dag=dag,
# )

# # define the start_hive_metastore
# start_hive_metastore= BashOperator(
#     task_id='start_hive_metastore', 
#         bash_command=["sudo", "-u", "hadoop", "/usr/local/hive/bin/hive --service metastore"],
#     dag=dag,
# )

# # define the start_hive_hiveserver2
# start_hive_hiveserver2= BashOperator(
#     task_id='start_hive_hiveserver2', 
#     bash_command=["sudo", "-u", "hadoop", "/usr/local/hive/bin/hive --service hiveserver2"],
#     dag=dag,
# )

# define the ingestion data from Binance task 
ingestion= BashOperator(
    task_id='ingestion', 
    bash_command="python3 /home/huannguyen/Data/source_code/Seminar-Data-Engineering/Data\ Lake/getdata_ver2_write_parquet.py", 
    dag=dag,
)

# define the etl task 
etl= BashOperator(
    task_id='etl',
    bash_command="python3 /home/huannguyen/Data/source_code/Seminar-Data-Engineering/Data\ Modeling/ETL.py",
    dag=dag,
)


# define the start_hive_hiveserver2
build_report= BashOperator(
    task_id='build_report', 
    bash_command="python3 /home/huannguyen/Data/source_code/Seminar-Data-Engineering/Data\ Modeling/build_report.py",
    dag=dag,
)

# task pipeline 
# start_hdfs>> delay_task >>start_hive_metastore>>start_hive_hiveserver2>>ingestion >> etl
ingestion >> etl >> build_report