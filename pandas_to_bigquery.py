from google.cloud import bigquery
import pandas

client = bigquery.Client()
df = pandas.read_excel('file.xlsx')

# Set dataset and table name
table_id = 'dataset.tablename'

# Set job config. In this example old data will be removed and new data will be added.
job_config = bigquery.LoadJobConfig(
    WRITE_TRUNCATE='WRITE_TRUNCATE')

job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)
