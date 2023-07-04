from pymongo import MongoClient

# Conectar-se ao servidor MongoDB
client = MongoClient('localhost', 27017)

# Acessar o banco de dados desejado
db = client['db']

# Acessar a coleção de produtos
colecao_produtos = db['product_db']

# Agrupar por idRetailerSKU e calcular a maior variação de preço
pipeline = [
    {
        '$unwind': '$assortment'
    },
    {
        '$group': {
            '_id': '$assortment.idRetailerSKU',
            'retailerTitle': {'$first': '$assortment.retailerTitle'},
            'maxPriceVariation': {'$max': '$assortment.priceVariation'}
        }
    },
    {
        '$sort': {
            'maxPriceVariation': -1,
        }
    },
    {
        '$limit': 11
    }
]

# Executar a consulta
resultado = colecao_produtos.aggregate(pipeline)

# Imprimir os resultados
print("Produtos com maior variação de preço:")
for produto in resultado:
    print("ID do Produto:", produto['_id'])
    print("Título do Varejista:", produto['retailerTitle'])
    print("Maior Variação de Preço:", produto['maxPriceVariation'])
    print("-------------------------------------")


