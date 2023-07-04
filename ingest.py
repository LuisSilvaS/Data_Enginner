from pymongo import MongoClient
import os
import json
import time


caminho_dos_json = 'data/datatest/root/workspace/data-eng-test'

# Conectar ao MongoDB
cliente = MongoClient('localhost', 27017)
banco_dados = cliente['db']  # substitua pelo nome do seu banco de dados
colecao = banco_dados['product_d']  # substitua pelo nome da sua coleção

# Medir o tempo de ingestão total
inicio = time.time()

# Iterar sobre os arquivos JSON
for nome_arquivo in os.listdir(caminho_dos_json):
    if nome_arquivo.endswith('.json'):
        caminho_arquivo = os.path.join(caminho_dos_json, nome_arquivo)
        with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo_json:  # Especificar o encoding como 'utf-8'
            dados = json.load(arquivo_json)
            colecao.insert_one(dados)

# Calcular o tempo total de ingestão
tempo_total = time.time() - inicio
print(f"Tempo total de ingestão: {tempo_total} segundos")