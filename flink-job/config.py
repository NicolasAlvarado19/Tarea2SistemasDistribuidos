import os
from dotenv import load_dotenv

load_dotenv()

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

# Tópicos de Kafka
TOPIC_RESPUESTAS_EXITOSAS = 'respuestas-exitosas'
TOPIC_RESULTADOS_VALIDADOS = 'resultados-validados'
TOPIC_PREGUNTAS_NUEVAS = 'preguntas-nuevas'

# Configuración de calidad
UMBRAL_SCORE = float(os.getenv('UMBRAL_SCORE', '0.6'))
MAX_REINTENTOS = int(os.getenv('MAX_REINTENTOS', '2'))

# Configuración de PostgreSQL
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'yahoo_qa')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'admin123')

# Configuración de Flink
FLINK_PARALLELISM = int(os.getenv('FLINK_PARALLELISM', '1'))
FLINK_CHECKPOINT_INTERVAL = int(os.getenv('FLINK_CHECKPOINT_INTERVAL', '10000'))  # 10 segundos