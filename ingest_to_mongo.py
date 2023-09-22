from pymongo import MongoClient
import os
import json
import time

class MongoDBDataIngestor:
    def __init__(self, db_name, collection_name, json_folder_path):
        self.db_name = db_name
        self.collection_name = collection_name
        self.json_folder_path = json_folder_path

    def connect_to_mongodb(self):
        self.client = MongoClient('localhost', 27017)
        self.database = self.client[self.db_name]
        self.collection = self.database[self.collection_name]

    def ingest_data_from_json_files(self):
        start_time = time.time()

        for file_name in os.listdir(self.json_folder_path):
            if file_name.endswith('.json'):
                file_path = os.path.join(self.json_folder_path, file_name)
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                    self.collection.insert_one(data)

        total_time = time.time() - start_time
        return total_time

    def close_connection(self):
        self.client.close()

if __name__ == "__main__":
    db_name = 'db'  # Nome do banco de dados
    collection_name = 'product_d'  # Nome da coleção
    json_folder_path = 'data/datatest/root/workspace/data-eng-test'  # Caminho para a pasta com arquivos JSON

    data_ingestor = MongoDBDataIngestor(db_name, collection_name, json_folder_path)
    data_ingestor.connect_to_mongodb()

    total_ingestion_time = data_ingestor.ingest_data_from_json_files()

    data_ingestor.close_connection()

    print(f"Tempo total de ingestão: {total_ingestion_time} segundos")
