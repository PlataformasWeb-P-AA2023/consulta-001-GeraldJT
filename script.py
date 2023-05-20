import pandas as pd
from pymongo import MongoClient

# Conexión a la base de datos MongoDB
client = MongoClient('mongodb://localhost:27017/')
database = client['tennis_db']
collection = database['tournaments']

# Lectura del archivo CSV
data = pd.read_csv('atp_tennis.csv')

# Convertir los datos del DataFrame a formato de lista de diccionarios
tournaments = data.to_dict('records')

# Insertar los datos en la colección de MongoDB
collection.insert_many(tournaments)

# Consulta en MongoDB para obtener los campeonatos por país
pipeline = [
    {"$group": {"_id": "$Winner", "count": {"$sum": 1}}}
]

result = collection.aggregate(pipeline)

# Imprimir los campeonatos por país
for doc in result:
    print(f"País: {doc['_id']}, Campeonatos: {doc['count']}")
