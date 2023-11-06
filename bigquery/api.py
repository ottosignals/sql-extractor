from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from bigquery.mysql_types_map import mysql_types

class BigQueryApi():
  def __init__(self):
    self._client = bigquery.Client()

  def insert(self, 
             project, 
             dataset, 
             table, 
             data, 
             data_format='json', 
             time_partitioning=None, 
             schema=None, 
             write_disposition=bigquery.WriteDisposition.WRITE_APPEND):
    
    dataset_id = bigquery.Dataset(f"{project}.{dataset}")
    dataset_id.location = 'EU'
    self._client.create_dataset(dataset_id, exists_ok=True)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = write_disposition
    if time_partitioning:
      job_config.time_partitioning = time_partitioning
    if schema:
        job_config.schema = schema
    # job_config.ignore_unknown_values = True

    table_id = f"{project}.{dataset}.{table}"
    
    if data_format == 'json':
      job = self._client.load_table_from_json(
        data, table_id, job_config=job_config
      )
    elif data_format == 'json_file':
      job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
      job = self._client.load_table_from_file(
        data, table_id, job_config=job_config
      )

    try:
      job.result()
    except BadRequest as e:
      for e in job.errors:
        print(f"BigQuery error: {e['message']}")
        raise Exception("BigQuery error")
      
  def copy(self,
           src_proj, src_dataset, src_table,
           dest_table, dest_proj=None, dest_dataset=None):
    
    if not dest_proj:
      dest_proj = src_proj
    
    if not dest_dataset:
      dest_dataset = src_dataset
    
    src = f"{src_proj}.{src_dataset}.{src_table}"
    dest = f"{dest_proj}.{dest_dataset}.{dest_table}"

    job_config = bigquery.CopyJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job = self._client.copy_table(src, dest, job_config=job_config)

    try:
      job.result()
    except BadRequest as e:
      for e in job.errors:
        print(f"BigQuery error: {e['message']}")
        raise Exception("BigQuery error")
      
  def delete(self, project, dataset, table):
    table_id = f"{project}.{dataset}.{table}"
    job = self._client.delete_table(table_id, not_found_ok=True)  # Make an API request.

    try:
      job.result()
    except BadRequest as e:
      for e in job.errors:
        print(f"BigQuery error: {e['message']}")
        raise Exception("BigQuery error")
    
      
  def schema_from_mysql(self, schema):
    bq_schema = [bigquery.SchemaField(col.get('name'), mysql_types.get(col.get('type', '').lower(), 'STRING'), mode='NULLABLE') for col in schema]
    print(f"BQ schema: {bq_schema}")
    return bq_schema