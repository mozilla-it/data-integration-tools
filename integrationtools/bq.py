from google.cloud import bigquery
import time

class BigQueryLoader:
    """
    Here lies some functions for making interacting with BigQuery (and GCS) a little easier
    in the service of Airflow-based ETL jobs.
    """
    @staticmethod
    def wait_for_job(job):
        while True:
            job.reload()
            if job.state == 'DONE':
                if job.error_result:
                    raise RuntimeError(job.errors)
                break
            time.sleep(1)
        job.result()
        if job.errors is not None:
            print(job.errors)
        print("Job finished.")

    @staticmethod
    def extract_table_to_gcs(destination_uri,dataset,table):
        client = bigquery.Client()
        table_ref = client.dataset(dataset).table(table)
        job = client.extract_table(table_ref,destination_uri,location="US")
        BigQueryLoader.wait_for_job(job)
        job.result()

    @staticmethod
    def download_gcs_file(bucket_name,remote_path,local_path):
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(remote_path)
        blob.download_to_filename(local_path)

    @staticmethod
    def upload_file_to_gcs(bucket_name,local_path,remote_path):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(remote_path)
        blob.upload_from_filename(local_path)

    @staticmethod
    def filter_query_to_table(query,dst_table,write_disposition="WRITE_TRUNCATE"):
        job_config = bigquery.QueryJobConfig(destination=dst_table,write_disposition=write_disposition)
        job = client.query(query, job_config=job_config)
        BigQueryLoader.wait_for_job(job)

    @staticmethod
    def load_csv_to_bigquery(filename,dataset,table,**kwargs):
        client = bigquery.Client()
        table_ref = client.dataset(dataset).table(table)

        write_disposition = kwargs.get('write_disposition','WRITE_TRUNCATE')
        ignore_unknown_values = kwargs.get('ignore_unknown_values',True)
        skip_leading_rows = kwargs.get('skip_leading_rows',1) # number of rows to skip, good for skipping header
        autodetect_schema = kwargs.get('autodetect_schema',False)
        max_bad_records = kwargs.get('max_bad_records',100)
        allow_jagged_rows = kwargs.get('allow_jagged_rows',True)
        source_format = kwargs.get('source_format','CSV')

        job_config = bigquery.LoadJobConfig()

        job_config.write_disposition = getattr(bigquery.WriteDisposition, write_disposition)

        job_config.ignore_unknown_values = ignore_unknown_values
        job_config.skip_leading_rows = skip_leading_rows
        job_config.autodetect = autodetect_schema
        job_config.max_bad_records = max_bad_records
        job_config.allow_jagged_rows = allow_jagged_rows
        
        job_config.source_format = getattr(bigquery.SourceFormat,source_format)

        fp = open(filename,"rb")
        load_job = client.load_table_from_file(fp, table_ref, job_config=job_config)
        print("Starting job {}".format(load_job.job_id))
        
        while True:
            load_job.reload()
            if load_job.state == 'DONE':
                if load_job.error_result:
                    raise RuntimeError(load_job.errors)
                break
            time.sleep(1)
        load_job.result()  # Waits for table load to complete.
        print(load_job.errors)
        print("Job finished.")

        fp.close()
        return True

