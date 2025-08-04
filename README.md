# ETL_BashOp_Apache_Airflow
Toll booth data ETL Data Pipleline with Apache Airflow and BashOperator.
The goal is to prepare traffic data from different toll plazas for future anaylsis of the road traffic to reduce congestion on the national highways. Each highway is operated by a different toll operator with a different IT setup that uses different file formats. The job is to collect data available in different formats and consolidate it into a single file.

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

#Shell commands prior to exporting to Airflow
#make directory, change permissions, get tolldata

sudo mkdir -p /home/project/airflow/dags/finalassignment/staging

sudo chmod -R 777 /home/project/airflow/dags/finalassignment

sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o /home/project/airflow/dags/finalassignment/tolldata.tgz

#export DAG to Airflow from terminal

export AIRFLOW_HOME=/home/project/airflow
 cp ETL_toll_data.py $AIRFLOW_HOME/dags




