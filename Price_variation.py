from pymongo import MongoClient
import pandas as pd

# Conectar-se ao servidor MongoDB
client = MongoClient('localhost', 27017)

# Acessar o banco de dados desejado
db = client['db']

# Acessar a coleção de produtos
colecao_produtos = db['product_d']

# Agrupar por idRetailerSKU e calcular a maior variação de preço
pipeline = [
    {
        '$unwind': '$assortment'
    },
    {
        '$group': {
            '_id': '$assortment.idRetailerSKU',
            'retailerTitle': {'$first': '$assortment.retailerTitle'},
            'maxPriceVariation': {'$max': '$assortment.priceVariation'},
            'FinalUrl': {'$first': '$assortment.retailerFinalUrl'},
            'screenshot': {'$first': '$assortment.screenshot'}
        }
    },
    {
        '$sort': {
            'maxPriceVariation': -1,
        }
    },
    {
        '$limit': 100
    }
]

# Executar a consulta
resultado = colecao_produtos.aggregate(pipeline)

# Criar um DataFrame a partir dos resultados
df = pd.DataFrame(list(resultado))

# Salvar o DataFrame em um arquivo CSV
csv_file = 'maxPriceVariation.csv'
df.to_csv(csv_file, index=False)

print(f"Os resultados foram salvos em: '{csv_file}'")

