import json
import logging
import os
import time
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AlmacenamientoConsumer:
    """
    Consumer que persiste resultados validados en PostgreSQL
    """
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        
        # Configuración de PostgreSQL
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('POSTGRES_DB', 'tarea2_db'),
            'user': os.getenv('POSTGRES_USER', 'tarea2_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'tarea2_password')
        }
        
        # Esperar a servicios
        self._wait_for_kafka()
        self._wait_for_postgres()
        
        # Inicializar tabla
        self._init_table()
        
        # Consumer
        self.consumer = KafkaConsumer(
            'resultados-validados',
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            group_id='almacenamiento-consumer-group',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        logger.info("✅ Consumer conectado a 'resultados-validados'")
        
        # Métricas
        self.registros_guardados = 0
        self.errores = 0
        
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
                    logger.info(f"✅ Kafka disponible")
                    time.sleep(5)
                    return
            except Exception:
                pass
            
            logger.info(f"⏳ Esperando Kafka... ({attempt + 1}/{max_attempts})")
            time.sleep(2)
        
        raise Exception("❌ No se pudo conectar a Kafka")
    
    def _wait_for_postgres(self, max_attempts=30):
        """Espera a que PostgreSQL esté disponible"""
        for attempt in range(max_attempts):
            try:
                conn = psycopg2.connect(**self.db_config)
                conn.close()
                logger.info("✅ PostgreSQL disponible")
                return
            except Exception:
                logger.info(f"⏳ Esperando PostgreSQL... ({attempt + 1}/{max_attempts})")
                time.sleep(2)
        
        raise Exception("❌ No se pudo conectar a PostgreSQL")
    
    def _init_table(self):
        """Crea la tabla si no existe"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS resultados_validados (
                    id SERIAL PRIMARY KEY,
                    pregunta TEXT NOT NULL,
                    respuesta_original TEXT NOT NULL,
                    respuesta_llm TEXT NOT NULL,
                    score FLOAT NOT NULL,
                    intentos_realizados INTEGER NOT NULL,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(pregunta)
                )
            """)
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info("✅ Tabla 'resultados_validados' lista")
            
        except Exception as e:
            logger.error(f"❌ Error creando tabla: {e}")
            raise
    
    def guardar_resultado(self, mensaje):
        """
        Guarda un resultado en PostgreSQL
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            pregunta = mensaje.get('pregunta', '')
            respuesta_original = mensaje.get('respuesta_original', '')
            respuesta_llm = mensaje.get('respuesta_llm', '')
            score = float(mensaje.get('score', 0.0))
            intentos = int(mensaje.get('intentos_realizados', 1))
            
            # Insert con ON CONFLICT para evitar duplicados
            cur.execute("""
                INSERT INTO resultados_validados 
                (pregunta, respuesta_original, respuesta_llm, score, intentos_realizados)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (pregunta) DO UPDATE SET
                    respuesta_llm = EXCLUDED.respuesta_llm,
                    score = EXCLUDED.score,
                    intentos_realizados = EXCLUDED.intentos_realizados,
                    fecha_creacion = CURRENT_TIMESTAMP
            """, (pregunta, respuesta_original, respuesta_llm, score, intentos))
            
            conn.commit()
            cur.close()
            conn.close()
            
            self.registros_guardados += 1
            logger.info(f"✅ Guardado: {pregunta[:50]}... (score={score:.4f}, intentos={intentos})")
            
        except Exception as e:
            self.errores += 1
            logger.error(f"❌ Error guardando: {e}")
    
    def iniciar(self):
        """
        Inicia el consumer
        """
        logger.info("=" * 80)
        logger.info("🚀 CONSUMER DE ALMACENAMIENTO INICIADO")
        logger.info("=" * 80)
        logger.info("📥 Esperando mensajes de 'resultados-validados'...")
        logger.info("=" * 80)
        
        try:
            for mensaje in self.consumer:
                self.guardar_resultado(mensaje.value)
                
                # Mostrar métricas cada 10 registros
                if self.registros_guardados % 10 == 0:
                    self._mostrar_metricas()
                    
        except KeyboardInterrupt:
            logger.info("\n⚠️ Interrumpido por el usuario")
        finally:
            self._mostrar_metricas()
            self.consumer.close()
    
    def _mostrar_metricas(self):
        """Muestra métricas del servicio"""
        logger.info("=" * 80)
        logger.info("📊 MÉTRICAS DEL ALMACENAMIENTO")
        logger.info("=" * 80)
        logger.info(f"✅ Registros guardados: {self.registros_guardados}")
        logger.info(f"❌ Errores: {self.errores}")
        logger.info("=" * 80)

if __name__ == "__main__":
    consumer = AlmacenamientoConsumer()
    consumer.iniciar()