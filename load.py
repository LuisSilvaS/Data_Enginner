from pymongo import MongoClient
import os
import json
import time


class MongoDBIngestor:
    def __init__(self, db_host, db_port, db_name):
        self.client = MongoClient(db_host, db_port)
        self.db = self.client[db_name]

    def ingest_data_from_directory(self, directory):
        total_ingestion_time = 0

        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                file_path = os.path.join(directory, filename)
                if not self.is_file_ingested(file_path):
                    ingestion_time = self.ingest_data_from_file(file_path)
                    total_ingestion_time += ingestion_time

        print(f"Total ingestion time: {total_ingestion_time} seconds")

    def ingest_data_from_file(self, file_path):
        with open(file_path, encoding='utf-8') as json_file:
            data = json.load(json_file)
            data['file_path'] = file_path  # Add the 'file_path' field to the data

            if self.is_file_ingested(file_path):
                print(f"Skipping ingestion for {file_path}. File already ingested.")
                return 0

            start_time = time.time()
            self.db.produtos_db.insert_one(data)
            end_time = time.time()

            ingestion_time = end_time - start_time
            print(f"Ingestion time for {file_path}: {ingestion_time} seconds")

            return ingestion_time

    def is_file_ingested(self, file_path):
        return self.db.produtos_db.count_documents({"file_path": file_path}) > 0


# Define MongoDB connection details
db_host = 'localhost'
db_port = 27017
db_name = 'db'

# Create MongoDBIngestor instance
ingestor = MongoDBIngestor(db_host, db_port, db_name)

# Set the directory path containing JSON files
directory_path = 'data/datatest/root/workspace/data-eng-test'

# Ingest data from the directory
ingestor.ingest_data_from_directory(directory_path)
