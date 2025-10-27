import json
import time
import logging
import os
from kafka import KafkaConsumer, KafkaProducer
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaConsumerService:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.puntuacion_url = os.getenv('PUNTUACION_URL', 'http://puntuacion:8002')
        self.max_reintentos = int(os.getenv('MAX_REINTENTOS', '3'))
        
        logger.info("Esperando a que Kafka esté disponible...")
        time.sleep(10)
        
        self.consumer = KafkaConsumer(
            'preguntas-nuevas',
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id='llm-processors',
            auto_offset_reset='earliest'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        logger.info("✓ Kafka Consumer inicializado")
    
    def procesar_pregunta(self, mensaje):
        try:
            pregunta = mensaje.get('pregunta')
            respuesta_original = mensaje.get('respuesta_original')
            intento = mensaje.get('intento', 1)
            
            logger.info(f"📝 Procesando (intento {intento}): {pregunta[:50]}...")
            
            response = requests.post(
                f"{self.puntuacion_url}/solo-generar",
                json={"pregunta": pregunta},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                resultado = {
                    'pregunta': pregunta,
                    'respuesta_original': respuesta_original,
                    'respuesta_llm': data.get('respuesta_llm'),
                    'intento': intento
                }
                self.producer.send('respuestas-exitosas', value=resultado)
                self.producer.flush()
                logger.info("✓ Respuesta enviada a 'respuestas-exitosas'")
                
            elif response.status_code == 429 and intento < self.max_reintentos:
                logger.warning("⚠️  Rate limit, reenviando...")
                mensaje['intento'] = intento + 1
                mensaje['espera_segundos'] = 2 ** intento
                self.producer.send('errores-sobrecarga', value=mensaje)
                self.producer.flush()
                
        except Exception as e:
            logger.error(f"✗ Error: {str(e)}")
    
    def run(self):
        logger.info("🚀 Esperando mensajes...")
        for mensaje in self.consumer:
            self.procesar_pregunta(mensaje.value)

if __name__ == "__main__":
    service = KafkaConsumerService()
    service.run()
