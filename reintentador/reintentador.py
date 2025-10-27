import time
import logging
import os
import json
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Reintentador:
    def __init__(self):
        logger.info("Esperando Kafka...")
        time.sleep(15)
        
        self.consumer = KafkaConsumer(
            'errores-sobrecarga',
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id='reintentador'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        logger.info("✓ Reintentador listo")
    
    def run(self):
        logger.info("🚀 Esperando errores para reintentar...")
        for mensaje in self.consumer:
            data = mensaje.value
            espera = data.get('espera_segundos', 2)
            logger.info(f"⏳ Esperando {espera}s...")
            time.sleep(espera)
            self.producer.send('preguntas-nuevas', value=data)
            self.producer.flush()
            logger.info("♻️  Reenviado")

if __name__ == "__main__":
    Reintentador().run()
