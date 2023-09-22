from pymongo import MongoClient
import pandas as pd

# Conectar-se ao servidor MongoDB
client = MongoClient('localhost', 27017)

# Acessar o banco de dados desejado
db = client['db']

# Acessar a coleção de produtos
colecao_produtos = db['product_d']

# Agrupar por idRetailerSKU e calcular a maior indisponibilidade
pipeline = [
    {
        '$unwind': '$assortment'
    },
    {
        '$lookup': {
            'from': 'images',
            'localField': 'assortment.idRetailerSKU',
            'foreignField': 'idRetailerSKU',
            'as': 'related_images'
        }
    },
    {
        '$group': {
            '_id': '$assortment.idRetailerSKU',
            'retailerTitle': {'$first': '$assortment.retailerTitle'},
            'retailerFinalUrl': {'$first': '$assortment.retailerFinalUrl'},
            'screenshot': {'$first': '$assortment.screenshot'},
            'retailerTitleFoundWords': {'$first': '$assortment.retailerTitleFoundWords'},
            'maxUnavailable': {'$sum': {'$cond': [{'$eq': ['$assortment.unavailable', True]}, 1, 0]}}
        }
    },
    {
        '$sort': {
            'maxUnavailable': -1
        }
    },
    {
        '$limit': 1000
    }
]

# Executar a consulta
resultado = colecao_produtos.aggregate(pipeline)

# Criar um DataFrame a partir dos resultados
df = pd.DataFrame(list(resultado))

# Salvar o DataFrame em um arquivo CSV
csv_file = 'maior_indisponibilidade.csv'
df.to_csv(csv_file, index=False, encoding='utf-8')

print(f"Os resultados foram salvos em: '{csv_file}'")
