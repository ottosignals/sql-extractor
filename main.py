from database.api import MySQL
from bigquery.api import BigQueryApi
import os
import io
import json


def run():
    BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID')
    BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID')
    BQ_TABLE_ID = os.environ.get('BQ_TABLE_ID')
    BQ_WRITE_MODE = os.environ.get('BQ_WRITE_MODE')

    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_DATABASE = os.getenv("DB_DATABASE")

    QUERY = os.getenv("QUERY")
    QUERY_PAGE_SIZE = int(os.getenv("QUERY_PAGE_SIZE", 1000))

    db = MySQL(user=DB_USER,
               password=DB_PASSWORD,
               host=DB_HOST,
               port=DB_PORT,
               database=DB_DATABASE
               )
    bq = BigQueryApi()

    schema = None
    rows = []

    db.pagination_init(QUERY, QUERY_PAGE_SIZE)
    schema = bq.schema_from_mysql(db.get_table_schema_from_query(QUERY))

    first_cycle = True
    while not db.pagination_end:
        print(f"Downloading data from '{DB_HOST}':'{DB_PORT}'")
        rows = db.pagination_next()
        print(f"Downloaded '{len(rows)}' rows of data")
        print(f"Inserting downloaded data to BigQuery")

        data_str = "\n".join(json.dumps(item, ensure_ascii=False, default=str) for item in rows)
        encoded_str = data_str.encode()
        data_file = io.BytesIO(encoded_str)

        if not BQ_WRITE_MODE or BQ_WRITE_MODE=='':
            write_disposition = "WRITE_TRUNCATE" if first_cycle else "WRITE_APPEND" # always delete the old data
        else:
            write_disposition = BQ_WRITE_MODE

        bq.insert(BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID, data_file, data_format='json_file', schema=schema, write_disposition=write_disposition)
        print(f"Data has been inserted to BigQuery")
        
        first_cycle = False

run()