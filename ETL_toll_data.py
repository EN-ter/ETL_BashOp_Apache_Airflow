from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

file_path = "/home/project/airflow/dags/finalassignment/"

#DAG arguments
default_args = {
'owner': 'owner1',
'start_date': days_ago(0),
'email': ['nussb003@csusm.edu'],
'retries': 1,
'retry_delay': timedelta(minutes=5),
}

#define the DAG(Directed Acyclic Graph)
dag = DAG('ETL_toll_data',
   default_args=default_args,
   description='Apache Airflow Final Assignment',
   schedule_interval=timedelta(days=1),
)

# define tasks
# task1-unzip data
unzip_data = BashOperator(
   task_id='unzip_data',
   bash_command=f"tar -xzf {file_path}tolldata.tgz -C  {file_path}staging",
   dag=dag,
)

# task2-extract toll data (Rowid, Timestamp, vehicle number, vehicle type) from csv
extract_data_from_csv = BashOperator(
   task_id='extract_data_from_csv',
   bash_command=(
      f"""echo "rowid, timestamp, vehicle_number, vehicle_type" > {file_path}staging/csv_data.csv """
      f"cut -d',' -f1,2,3,4 {file_path}staging/vehicle-data.csv >> "
      f"{file_path}staging/csv_data.csv"),
   dag=dag,
)

# task3-extract (number of axles, tollplaza id, tollplaza code) from tsv
extract_data_from_tsv = BashOperator(
   task_id='extract_data_from_tsv',
   bash_command=(
       f"""echo "number_of_axles, tollplaza_id, tolplaza_code" > {file_path}staging/tsv_data.csv """
       f"cut -d$'\t' -f5,6,7 {file_path}staging/tollplaza-data.tsv | tr $'\t' ',' >> "
       f"{file_path}staging/tsv_data.csv"),
   dag=dag,
)

# task4-extract (payment code, vehicle code) using fixed width from csv
extract_data_from_fixed_width = BashOperator(
   task_id='extract_data_from_fixed_width',
   bash_command=(
      f"""echo "payment_code, vehicle_code" > {file_path}staging/fixed_width_data.csv """
      f"cut -c 59-67 {file_path}staging/payment-data.txt | tr ' ' ',' > "
      f"{file_path}staging/fixed_width_data.csv"),
   dag=dag,
)

# task5-consolidate data into one csv
consolidate_data = BashOperator(
   task_id='consolidate_data',
   bash_command=(f"paste -d',' {file_path}staging/csv_data.csv "
   f"{file_path}staging/fixed_width_data.csv {file_path}staging/tsv_data.csv > "
   f"{file_path}staging/extracted_data.csv"),
   dag=dag,
)

# task6-transform (uppercase)
transform_data = BashOperator(
   task_id='transform_data',
   bash_command=(f"tr 'a-z' 'A-Z' < {file_path}staging/extracted_data.csv > """
   f"{file_path}staging/transformed_data.csv"),
   dag=dag,
)
# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
