import unittest
from pymongo import MongoClient
from load import MongoDBIngestor
import json
import os

# Define a classe de teste para MongoDBIngestor
class MongoDBIngestorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Configuração inicial para os testes
        db_host = 'localhost'
        db_port = 27017
        db_name = 'testdb'
        cls.ingestor = MongoDBIngestor(db_host, db_port, db_name)

    def setUp(self):
        # Configuração adicional para cada teste (se necessário)
        self.db = self.ingestor.db
        self.collection = self.db.produtos
        self.collection.delete_many({})  # Limpar a coleção antes de cada teste

    def test_ingest_data_from_file(self):
        # Define um arquivo de teste
        file_path = 'test_file.json'

        # Define os dados do arquivo de teste
        data = {
            "field1": "value1",
            "field2": "value2"
        }

        # Cria o arquivo de teste
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file)

        # Executa o método de ingestão
        ingestion_time = self.ingestor.ingest_data_from_file(file_path)

        # Verifica se os dados foram inseridos corretamente
        self.assertEqual(self.collection.count_documents({}), 1)
        document = self.collection.find_one()
        self.assertEqual(document['field1'], 'value1')
        self.assertEqual(document['field2'], 'value2')

        # Verifica se o tempo de ingestão é maior que zero
        self.assertGreater(ingestion_time, 0)

        # Remove o arquivo de teste
        os.remove(file_path)

    def test_is_file_ingested(self):
        # Define um arquivo de teste
        file_path = 'test_file.json'

        # Define os dados do arquivo de teste
        data = {
            "field1": "value1",
            "field2": "value2"
        }

        # Insere os dados de teste no banco de dados
        self.collection.insert_one(data)

        # Verifica se o arquivo já foi inserido
        self.assertTrue(self.ingestor.is_file_ingested(file_path))

        # Remove o documento de teste
        self.collection.delete_many({})

        # Verifica se o arquivo não foi inserido novamente
        self.assertFalse(self.ingestor.is_file_ingested(file_path))

        # Remove o arquivo de teste
        os.remove(file_path)

    @classmethod
    def tearDownClass(cls):
        # Limpeza após todos os testes
        cls.ingestor.client.drop_database(cls.ingestor.db)

# Executa os testes
if __name__ == '__main__':
    unittest.main()
