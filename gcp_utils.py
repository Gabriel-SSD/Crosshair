import logging
import json

from dotenv import load_dotenv
from google.cloud import storage, bigquery
from google.cloud.exceptions import GoogleCloudError

load_dotenv()

class GCSManager:
    def __init__(self):
        self.client = storage.Client()

    def save_to_gcs(self, data: dict, bucket_path: str, file_name: str, sufix: str = None, ensure_ascii: bool = True) -> None:
        if not isinstance(data, dict):
            raise ValueError("The 'data' parameter must be a dictionary (JSON object).")

        bucket_name, *path_parts = bucket_path.split('/', 1)
        path = path_parts[0] if path_parts else ''
        bucket = self.client.bucket(bucket_name)

        if not bucket.exists():
            raise ValueError(f"The bucket '{bucket_name}' does not exist.")

        file_name_with_sufix = f"{file_name}_{sufix}.json" if sufix else f"{file_name}.json"
        blob = bucket.blob(f"{path}/{file_name_with_sufix}" if path else file_name_with_sufix)

        with blob.open("w", encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=ensure_ascii)
        logging.info(f"File successfully saved to GCS: gs://{bucket_name}/{file_name_with_sufix}")

    def clear_path(self, full_path: str) -> int:
        bucket_name, path_prefix = full_path.split("/", 1)
        blobs = self.client.list_blobs(bucket_name, prefix=path_prefix)

        deleted_count = 0
        for blob in blobs:
            blob.delete()
            deleted_count += 1

        return deleted_count

    def read_file(self, bucket_path: str, file_name: str, sufix: str = None) -> dict:
        bucket_name, *path_parts = bucket_path.split('/', 1)
        path = path_parts[0] if path_parts else ''
        bucket = self.client.bucket(bucket_name)

        if not bucket.exists():
            raise ValueError(f"The bucket '{bucket_name}' does not exist.")

        file_name_with_sufix = f"{file_name}_{sufix}.json" if sufix else f"{file_name}.json"
        blob = bucket.blob(f"{path}/{file_name_with_sufix}" if path else file_name_with_sufix)

        try:
            json_content = blob.download_as_text()
            data = json.loads(json_content)
            logging.info(f"File successfully read from GCS: gs://{bucket_name}/{file_name_with_sufix}")
            return data
        except Exception as e:
            logging.error(f"Error reading file from GCS: gs://{bucket_name}/{file_name_with_sufix}. Error: {e}")
            raise

    def read_folder(self, bucket_path: str, file_extension: str = ".json") -> dict:
        bucket_name, path_prefix = bucket_path.split("/", 1)
        bucket = self.client.bucket(bucket_name)

        if not bucket.exists():
            raise ValueError(f"The bucket '{bucket_name}' does not exist.")

        blobs = self.client.list_blobs(bucket_name, prefix=path_prefix)

        all_data = {}
        for blob in blobs:
            if blob.name.endswith(file_extension):
                try:
                    json_content = blob.download_as_text()
                    data = json.loads(json_content)
                    all_data[blob.name] = data
                    logging.info(f"File successfully read from GCS: gs://{bucket_name}/{blob.name}")
                except Exception as e:
                    logging.error(f"Error reading file from GCS: gs://{bucket_name}/{blob.name}. Error: {e}")

        return all_data

    def list_files_in_folder(self, folder_path: str) -> list:
        bucket_name, path_prefix = folder_path.split("/", 1)
        bucket = self.client.bucket(bucket_name)
        return [str(blob.name) for blob in bucket.list_blobs(prefix=path_prefix)]

    def blob_exists(self, bucket_path: str, file_name: str, sufix: str = None) -> bool:
        bucket_name, *path_parts = bucket_path.split('/', 1)
        path = path_parts[0] if path_parts else ''
        bucket = self.client.bucket(bucket_name)

        if not bucket.exists():
            raise ValueError(f"The bucket '{bucket_name}' does not exist.")

        file_name_with_sufix = f"{file_name}_{sufix}.json" if sufix else f"{file_name}.json"
        blob = bucket.blob(f"{path}/{file_name_with_sufix}" if path else file_name_with_sufix)

        return blob.exists()


class BQManager:
    def __init__(self):
        self.client = bigquery.Client()


    def execute_query(self, query_filename: str, query_name: str, params: dict = None):
        try:
            with open(query_filename, 'r') as file:
                query = file.read()
            if params:
                query = query.format(**params)
            logging.info(f"Executing {query_name} query...")
            job = self.client.query(query)
            result = job.result()
            logging.info(f"Query {query_name} executed successfully. {result.total_rows} rows affected.")
            return [dict(row) for row in result]
        except GoogleCloudError as e:
            logging.error(f"An error occurred while executing the query {query_name}: {e}")
            raise
        except FileNotFoundError:
            logging.error(f"Query file {query_filename} not found.")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise

    def load_data_from_gcs(self, uri: str, dataset: str, table: str, truncate: bool = False) -> None:

        if truncate:
            write_type = bigquery.WriteDisposition.WRITE_TRUNCATE
            autodetect = True
        else:
            write_type = bigquery.WriteDisposition.WRITE_APPEND
            autodetect = False
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=autodetect,
            write_disposition=write_type,
        )

        try:
            uri = f"gs://{uri}"
            logging.info(f"Loading data from {uri} to BigQuery table {dataset}.{table}...")
            load_job = self.client.load_table_from_uri(uri, f"{dataset}.{table}", job_config=job_config)
            load_job.result()
            logging.info(f"Data successfully loaded from {uri} to BigQuery table {dataset}.{table}.")
        except GoogleCloudError as e:
            logging.error(f"Failed to load data from {uri}: {e}")
            raise
