import dlt
from dlt.sources.sql_database import sql_database

def full_export_tables():
    source = sql_database().with_resources("customer")
    
    pipeline = dlt.pipeline(
        pipeline_name='full_export_from_mysql_to_bigquery',
        destination='bigquery',
        dataset_name='dev_source'
    )

    load_info = pipeline.run(source, write_disposition='merge')
    print(load_info)

if __name__ == '__main__':
    full_export_tables()