import time
import json
import random
from kafka import KafkaProducer

clases = ['TES UVR', 'TES Pesos TF', 'Acciones alta liquidez',
          'Fondos de capital privado', 'Bonos', 'Fondos indices accionarios']
monedas = ['PESO COLOMBIANO', 'DOLAR AMERICANO', 'EURO', 'UVR']

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    evento = {
        "clase": random.choice(clases),
        "moneda": random.choice(monedas),
        "valor_mercado": round(random.uniform(1e8, 5e10), 2),
        "timestamp": int(time.time())
    }
    producer.send('inversiones', value=evento)
    print(f"Sent: {evento}")
    time.sleep(1)
