import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# os.getenv(variable, default)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'rankings_ext')

def format_to_parquet(src_file,year):
	df = pd.read_csv(src_file)
	if int(year) > 2015:
		df['stats_number_students'] = df['stats_number_students'].str.replace(',','').astype(int)
		df['stats_pc_intl_students'] = df['stats_pc_intl_students'].str.strip('%').replace('',0).astype(float)
	else:
		df['stats_number_students'] = 0
	df["year"] = int(year)
	df.to_parquet(src_file.replace('.csv', '.parquet'))
	
def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
	
local_workflow = DAG(
	"IngestionDag",
	schedule_interval = "0 0 1 1 *",
	start_date= datetime(2011,1,1),
	end_date= datetime(2024,1,1),
	is_paused_upon_creation = False
)

#url = https://raw.githubusercontent.com/EngMohamedKemany/DE-ZoomCAMP-Project_Universities_Rank/master/Data/2011_rankings.csv


with local_workflow:
	URL_PREFIX = 'https://raw.githubusercontent.com/EngMohamedKemany/DE-ZoomCAMP-Project_Universities_Rank/master/Data/'
	URL_TEMPLATE = URL_PREFIX + '{{ execution_date.strftime(\'%Y\') }}_rankings.csv'
	OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y\') }}_rankings.csv'
	parquet_file = 'output_{{ execution_date.strftime(\'%Y\') }}_rankings.parquet'
	
	wget_task = BashOperator(
		task_id='wget',
		bash_command=f'wget {URL_TEMPLATE} -O {OUTPUT_FILE_TEMPLATE}'
	)
	
	format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": OUTPUT_FILE_TEMPLATE,
			"year": '{{ execution_date.strftime(\'%Y\') }}'
        },
    )
	
	local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"rankings/{parquet_file}",
            "local_file": f"{AIRFLOW_HOME}/{parquet_file}",
        },
    )
	
	bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table"
			
            },
			"schema": {
				"fields": [
					{"name": "rank_order", "type": "INTEGER", "mode": "NULLABLE"},
					{"name": "name", "type": "STRING", "mode": "NULLABLE"},
					{"name": "scores_overall_rank", "type": "INTEGER", "mode": "NULLABLE"},
					{"name": "scores_teaching_rank", "type": "INTEGER", "mode": "NULLABLE"},
					{"name": "scores_international_outlook_rank", "type": "INTEGER", "mode": "NULLABLE"},
					{"name": "scores_industry_income_rank", "type": "INTEGER", "mode": "NULLABLE"},
					{"name": "scores_research_rank", "type": "INTEGER", "mode": "NULLABLE"},
					{"name": "scores_citations_rank", "type": "INTEGER", "mode": "NULLABLE"},
					{"name": "location", "type": "STRING", "mode": "NULLABLE"},
					{"name": "stats_number_students", "type": "INTEGER", "mode": "NULLABLE"},
					{"name": "stats_student_staff_ratio", "type": "FLOAT", "mode": "NULLABLE"},
					{"name": "stats_pc_intl_students", "type": "FLOAT", "mode": "NULLABLE"},
					{"name": "stats_female_male_ratio", "type": "STRING", "mode": "NULLABLE"},
					{"name": "aliases", "type": "STRING", "mode": "NULLABLE"},
					{"name": "subjects_offered", "type": "STRING", "mode": "NULLABLE"},
					{"name": "closed", "type": "BOOL", "mode": "NULLABLE"},
					{"name": "unaccredited", "type": "BOOL", "mode": "NULLABLE"},
					{"name": "year", "type": "INTEGER", "mode": "NULLABLE"}
				]
			},
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/rankings/output_*.parquet"],
            },
        },
    )
	
	CREATE_BQ_TBL_QUERY = (
			f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.rankings_table_partitioned \
			PARTITION BY RANGE_BUCKET(country_code, GENERATE_ARRAY(0, 1000, 100)) \
			AS \
			SELECT e.*, \
			c.country_code, c.region, c.sub_region, c.intermediate_region, c.region_code, c.sub_region_code \
			FROM {BIGQUERY_DATASET}.external_table e \
			join {BIGQUERY_DATASET}.country_lookup c \
			on e.location = c.name;"
	)
	bq_ext_2_part_task = BigQueryInsertJobOperator(
			task_id="bq_create_partitioned_table_task",
			configuration={
				"query": {
					"query": CREATE_BQ_TBL_QUERY,
					"useLegacySql": False,
				}
			}
	)
	
	
	wget_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task >> bq_ext_2_part_task