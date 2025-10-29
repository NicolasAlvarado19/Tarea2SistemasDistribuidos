import json
import logging
import os
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Reintentador:
    """
    Servicio que procesa errores de cuota y sobrecarga con delays apropiados
    """
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.max_reintentos = int(os.getenv('MAX_REINTENTOS', '3'))
        
        # Esperar a Kafka
        self._wait_for_kafka()
        
        # Consumer de AMBOS topics de errores
        self.consumer = KafkaConsumer(
            'errores-cuota',      
            'errores-sobrecarga',  
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id='reintentador-group',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        logger.info(" Consumer conectado a topics de errores")
        
        # Producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(" Producer conectado")
        
        # Métricas
        self.mensajes_procesados = 0
        self.reintentos_enviados = 0
        self.descartados = 0
        
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
                    logger.info(" Kafka disponible")
                    time.sleep(5)
                    return
            except Exception:
                pass
            logger.info(f" Esperando Kafka... ({attempt + 1}/{max_attempts})")
            time.sleep(2)
        raise Exception(" No se pudo conectar a Kafka")
    
    def procesar_error(self, mensaje, topic):
        """Procesa un mensaje de error con delay apropiado"""
        try:
            self.mensajes_procesados += 1
            
            pregunta = mensaje.get('pregunta', '')
            respuesta_original = mensaje.get('respuesta_original', '')
            intento_actual = mensaje.get('intento', 1)
            
            retry_delay = mensaje.get('retry_delay', 60)
            
            if isinstance(retry_delay, dict):
                retry_delay = retry_delay.get('seconds', 60)
            
            logger.info(f"  Error en {topic}")
            logger.info(f"   Pregunta: {pregunta[:50]}...")
            logger.info(f"   Intento: {intento_actual}/{self.max_reintentos}")
            logger.info(f"   Delay requerido: {retry_delay}s")
            
            # Verificar límite de reintentos
            if intento_actual >= self.max_reintentos:
                self.descartados += 1
                logger.error(f" DESCARTADO - Reintentos agotados")
                return
            
            logger.info(f" Esperando {retry_delay}s antes de reintentar...")
            time.sleep(retry_delay)
            
            # Reenviar a preguntas-nuevas
            mensaje_reintento = {
                'pregunta': pregunta,
                'respuesta_original': respuesta_original,
                'intento': intento_actual + 1,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            self.producer.send('preguntas-nuevas', mensaje_reintento)
            self.producer.flush()
            
            self.reintentos_enviados += 1
            logger.info(f" REINTENTO {intento_actual + 1} enviado después de esperar")
            
        except Exception as e:
            logger.error(f" Error procesando mensaje: {e}")
    
    def iniciar(self):
        """Inicia el reintentador"""
        logger.info("=" * 80)
        logger.info(" REINTENTADOR INICIADO")
        logger.info("=" * 80)
        logger.info(f" Máximo reintentos: {self.max_reintentos}")
        logger.info(" Esperando mensajes de: errores-cuota, errores-sobrecarga")
        logger.info("=" * 80)
        
        try:
            for mensaje in self.consumer:
                topic = mensaje.topic
                self.procesar_error(mensaje.value, topic)
                
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
        logger.info(" MÉTRICAS DEL REINTENTADOR")
        logger.info("=" * 80)
        logger.info(f"Mensajes procesados: {self.mensajes_procesados}")
        logger.info(f" Reintentos enviados: {self.reintentos_enviados}")
        logger.info(f" Descartados: {self.descartados}")
        logger.info("=" * 80)

if __name__ == "__main__":
    reintentador = Reintentador()
    reintentador.iniciar()