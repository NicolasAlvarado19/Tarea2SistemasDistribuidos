import json
import logging
import os
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import requests

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PuntuacionConsumer:
    """
    Consumer que evalúa respuestas exitosas y decide si validarlas o reintentarlas
    """
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.puntuacion_url = os.getenv('PUNTUACION_URL', 'http://puntuacion:8002')
        self.umbral_minimo = float(os.getenv('UMBRAL_SCORE', '0.6'))
        self.max_reintentos = int(os.getenv('MAX_REINTENTOS_SCORE', '2'))
        
        # Esperar a que Kafka y Puntuación estén disponibles
        self._wait_for_kafka()
        self._wait_for_puntuacion()
        
        # Consumer
        self.consumer = KafkaConsumer(
            'respuestas-exitosas',
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id='puntuacion-consumer-group',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        logger.info(" Consumer conectado a 'respuestas-exitosas'")
        
        # Producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(" Producer conectado")
        
        # Métricas
        self.mensajes_procesados = 0
        self.respuestas_validadas = 0
        self.respuestas_rechazadas = 0
        self.reintentos_enviados = 0
        
    def _wait_for_kafka(self, max_attempts=30):
        """Espera a que Kafka esté disponible"""
        import socket
        kafka_host = self.bootstrap_servers.split(':')[0]
        kafka_port = int(self.bootstrap_servers.split(':')[1])
        
        for attempt in range(max_attempts):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex((kafka_host, kafka_port))
                sock.close()
                
                if result == 0:
                    logger.info(f" Kafka disponible en {self.bootstrap_servers}")
                    time.sleep(5)
                    return
            except Exception:
                pass
            
            logger.info(f" Esperando Kafka... ({attempt + 1}/{max_attempts})")
            time.sleep(2)
        
        raise Exception(" No se pudo conectar a Kafka")
    
    def _wait_for_puntuacion(self, max_attempts=30):
        """Espera a que el servicio de puntuación esté disponible"""
        for attempt in range(max_attempts):
            try:
                response = requests.get(f"{self.puntuacion_url}/salud", timeout=2)
                if response.status_code == 200:
                    logger.info(f" Servicio de puntuación disponible")
                    return
            except Exception:
                pass
            
            logger.info(f" Esperando servicio de puntuación... ({attempt + 1}/{max_attempts})")
            time.sleep(2)
        
        logger.warning(" Servicio de puntuación no responde, continuando de todas formas")
    
    def calcular_score(self, pregunta: str, respuesta_original: str, respuesta_llm: str) -> float:
        """
        Llama a la API de puntuación para calcular el score
        """
        try:
            # Usar el endpoint que solo calcula similitud
            payload = {
                "texto1": respuesta_original,
                "texto2": respuesta_llm
            }
            
            response = requests.post(
                f"{self.puntuacion_url}/solo-similitud",
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                return float(data.get('similitud', 0.0))
            else:
                logger.error(f" Error en API de puntuación: {response.status_code}")
                return 0.0
                
        except Exception as e:
            logger.error(f" Error calculando score: {e}")
            return 0.0
    
    def procesar_respuesta(self, mensaje):
        """
        Procesa una respuesta, calcula score y decide qué hacer
        """
        try:
            pregunta = mensaje.get('pregunta', '')
            respuesta_yahoo = mensaje.get('respuesta_original', '')
            respuesta_llm = mensaje.get('respuesta_llm', '')
            intento_actual = mensaje.get('intento', 1)
            
            # Calcular score
            score = self.calcular_score(pregunta, respuesta_yahoo, respuesta_llm)
            
            logger.info(f" Score calculado: {score:.4f} | Umbral: {self.umbral_minimo}")
            
            # Decisión basada en score
            if score >= self.umbral_minimo:
                #  Score aceptable - enviar a resultados validados
                resultado = {
                    'pregunta': pregunta,
                    'respuesta_original': respuesta_yahoo,
                    'respuesta_llm': respuesta_llm,
                    'score': score,
                    'intentos_realizados': intento_actual
                }
                
                self.producer.send('resultados-validados', resultado)
                self.producer.flush()
                
                self.respuestas_validadas += 1
                logger.info(f" VALIDADA (score={score:.4f}) - Enviada a 'resultados-validados'")
                
            else:
                #  Score bajo - verificar reintentos
                if intento_actual < self.max_reintentos:
                    # Reenviar a preguntas-nuevas
                    mensaje_reintento = {
                        'pregunta': pregunta,
                        'respuesta_original': respuesta_yahoo,
                        'intento': intento_actual + 1,
                        'motivo_reintento': f'score_bajo_{score:.4f}'
                    }
                    
                    self.producer.send('preguntas-nuevas', mensaje_reintento)
                    self.producer.flush()
                    
                    self.reintentos_enviados += 1
                    logger.warning(f" REINTENTO {intento_actual + 1}/{self.max_reintentos} (score={score:.4f})")
                else:
                    # Agotados los reintentos - descartar
                    self.respuestas_rechazadas += 1
                    logger.error(f" DESCARTADA - Reintentos agotados (score={score:.4f})")
            
        except Exception as e:
            logger.error(f" Error procesando respuesta: {e}")
    
    def iniciar(self):
        """
        Inicia el consumer
        """
        logger.info("=" * 80)
        logger.info(" CONSUMER DE PUNTUACIÓN INICIADO")
        logger.info("=" * 80)
        logger.info(f" Umbral mínimo: {self.umbral_minimo}")
        logger.info(f" Máximo reintentos: {self.max_reintentos}")
        logger.info(" Esperando mensajes de 'respuestas-exitosas'...")
        logger.info("=" * 80)
        
        try:
            for mensaje in self.consumer:
                self.mensajes_procesados += 1
                self.procesar_respuesta(mensaje.value)
                
                # Mostrar métricas cada 10 mensajes
                if self.mensajes_procesados % 10 == 0:
                    self._mostrar_metricas()
                    
        except KeyboardInterrupt:
            logger.info("\n Interrumpido por el usuario")
        finally:
            self._mostrar_metricas()
            self.consumer.close()
            self.producer.close()
    
    def _mostrar_metricas(self):
        """Muestra métricas del servicio"""
        logger.info("=" * 80)
        logger.info(" MÉTRICAS DEL CONSUMER DE PUNTUACIÓN")
        logger.info("=" * 80)
        logger.info(f"Mensajes procesados: {self.mensajes_procesados}")
        logger.info(f" Validadas: {self.respuestas_validadas}")
        logger.info(f" Reintentos enviados: {self.reintentos_enviados}")
        logger.info(f" Descartadas: {self.respuestas_rechazadas}")
        
        if self.mensajes_procesados > 0:
            tasa_validacion = (self.respuestas_validadas / self.mensajes_procesados) * 100
            logger.info(f"📈 Tasa de validación: {tasa_validacion:.1f}%")
        
        logger.info("=" * 80)

if __name__ == "__main__":
    consumer = PuntuacionConsumer()
    consumer.iniciar()