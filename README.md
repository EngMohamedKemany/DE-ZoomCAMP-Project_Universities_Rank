# DE-ZoomCAMP-Project_Universities_Rank

This project is the capstone project in the data engineering zoomcamp taken in a duration of 9 weeks.
I chose a mock data just to show what could be done by my pipeline. In other words any data could be run in the same pipeline as long as it is of the same nature.

My data is world universities ranking in comma sep value format from 2011 to 2023(latest)
so the data ofcourse needed to be pulled yearly and that was dhandled by Airflow.
The data needed cleaning and updates handling, some done in the dag and the rest was done by the transformation tool.
The dag transformations was used because some of the data was not accepted by the warehouse used (Big query)
Eventually, the data is pulled into looker dashboards and stats was run as charts.

The project in steps:

Credditentials from GCP is added to the workflow

Terraform is used to initialize the GCP.

Docker used for Airflow containerization.

Airflow is used as follows:
  - wget the data of the working year
  - The csv file is parquetized ( from .csv to .parquet) for size purposes ( irrelevant to the project itself)
  - data is uploaded to the bucket
  - external table created "with specified schema" from the bucket into BigQuery ( schema used to fix extra cols added from 2016)
  - Partitoned table by region (Asia, Africa, ...etc) created joined by a lookup table uploaded manually for universities location

DBT is used as follows:
  - The data is pulled from the DWH
  - Two staging views made from two continents (Asia - Africa)
  - One Fact table made ( Middle East Universities )
  - Tests for uniqe and not null conducted
  - Changes with macros ( 2 macros used)
     - one for diversity in each university 
     - The other to fix an issue with the ranking starting from 2017
     
The Dashboard:
https://lookerstudio.google.com/reporting/4b58a545-2be0-41d1-9b97-0b51c1b1314c

The room for improvements:
  - CICD pipelin
