from pymongo import MongoClient

# Conectar-se ao servidor MongoDB
client = MongoClient('localhost', 27017)

# Acessar o banco de dados desejado
db = client['db']

# Acessar a coleção de produtos
colecao_produtos = db['product_db']

# Agrupar por idRetailerSKU e calcular a maior indisponibilidade
pipeline = [
    {
        '$unwind': '$assortment'
    },
    {
        '$group': {
            '_id': '$assortment.idRetailerSKU',
            'retailerTitle': {'$first': '$assortment.retailerTitle'},
            'maxUnavailable': {'$sum': {'$cond': [{'$eq': ['$assortment.available', False]}, 1, 0]}}
        }
    },
    {
        '$sort': {
            'maxUnavailable': -1
        }
    },
    {
        '$limit': 10
    }
]

# Executar a consulta
resultado = colecao_produtos.aggregate(pipeline)

# Imprimir os resultados
print("Produtos com maior indisponibilidade:")
for produto in resultado:
    print("ID do Produto:", produto['_id'])
    print("Título do Varejista:", produto['retailerTitle'])
    print("Maior Indisponibilidade:", produto['maxUnavailable'])
    print("-------------------------------------")
