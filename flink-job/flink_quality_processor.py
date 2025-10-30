#!/usr/bin/env python3
"""
Apache Flink Job: Procesamiento de Calidad de Respuestas
Consume respuestas del LLM, calcula score, y toma decisiones basadas en umbral.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.common import WatermarkStrategy

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

import config

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ScoreCalculator(MapFunction):
    """
    Función que calcula el score de similitud entre respuestas.
    """
    
    def __init__(self):
        self.model = None
        
    def open(self, runtime_context):
        """Inicializar el modelo al abrir el worker."""
        logger.info("Inicializando modelo de embeddings...")
        self.model = SentenceTransformer('paraphrase-MiniLM-L6-v2')
        logger.info("Modelo inicializado correctamente")
    
    def map(self, value: str) -> str:
        """
        Procesa cada mensaje y calcula el score.
        
        Args:
            value: JSON string con la respuesta del LLM
            
        Returns:
            JSON string con el score calculado
        """
        try:
            data = json.loads(value)
            
            # Extraer respuestas
            respuesta_llm = data.get('respuesta_llm', '')
            respuesta_yahoo = data.get('respuesta_yahoo', '')
            
            if not respuesta_llm or not respuesta_yahoo:
                logger.warning(f"Respuestas vacías para pregunta_id: {data.get('pregunta_id')}")
                data['score'] = 0.0
                data['error'] = 'respuestas_vacias'
                return json.dumps(data)
            
            # Calcular embeddings
            embedding_llm = self.model.encode([respuesta_llm])
            embedding_yahoo = self.model.encode([respuesta_yahoo])
            
            # Calcular similitud coseno
            score = float(cosine_similarity(embedding_llm, embedding_yahoo)[0][0])
            
            # Agregar score al mensaje
            data['score'] = round(score, 4)
            data['timestamp_score'] = datetime.now().isoformat()
            
            logger.info(f"Score calculado: {score:.4f} para pregunta_id: {data.get('pregunta_id')}")
            
            return json.dumps(data)
            
        except Exception as e:
            logger.error(f"Error calculando score: {str(e)}")
            data = json.loads(value) if isinstance(value, str) else value
            data['score'] = 0.0
            data['error'] = str(e)
            return json.dumps(data)


class QualityFilter(FilterFunction):
    """
    Filtra mensajes basándose en el umbral de calidad.
    """
    
    def __init__(self, umbral: float, filter_high: bool = True):
        """
        Args:
            umbral: Umbral de score
            filter_high: True para score >= umbral, False para score < umbral
        """
        self.umbral = umbral
        self.filter_high = filter_high
    
    def filter(self, value: str) -> bool:
        """
        Determina si el mensaje pasa el filtro.
        
        Args:
            value: JSON string con score
            
        Returns:
            True si pasa el filtro, False caso contrario
        """
        try:
            data = json.loads(value)
            score = data.get('score', 0.0)
            
            if self.filter_high:
                # Filtrar scores altos (validados)
                result = score >= self.umbral
            else:
                # Filtrar scores bajos (reintentar)
                result = score < self.umbral
            
            return result
            
        except Exception as e:
            logger.error(f"Error en filtro: {str(e)}")
            return False


class RetryMapper(MapFunction):
    """
    Prepara mensajes para reintento, incrementando contador.
    """
    
    def map(self, value: str) -> str:
        """
        Incrementa contador de reintentos y prepara mensaje.
        
        Args:
            value: JSON string con datos de la pregunta
            
        Returns:
            JSON string preparado para reintento
        """
        try:
            data = json.loads(value)
            
            # Incrementar contador de reintentos
            reintentos = data.get('reintentos', 0)
            data['reintentos'] = reintentos + 1
            
            # Agregar timestamp de reintento
            data['timestamp_reintento'] = datetime.now().isoformat()
            data['motivo_reintento'] = 'score_bajo'
            
            # Log
            logger.info(
                f"Preparando reintento {data['reintentos']}/{config.MAX_REINTENTOS} "
                f"para pregunta_id: {data.get('pregunta_id')} (score: {data.get('score')})"
            )
            
            return json.dumps(data)
            
        except Exception as e:
            logger.error(f"Error preparando reintento: {str(e)}")
            return value


class MaxRetryFilter(FilterFunction):
    """
    Filtra mensajes que no han excedido el máximo de reintentos.
    """
    
    def filter(self, value: str) -> bool:
        """
        Verifica si no se ha excedido el límite de reintentos.
        
        Args:
            value: JSON string con contador de reintentos
            
        Returns:
            True si puede reintentarse, False si se excedió el límite
        """
        try:
            data = json.loads(value)
            reintentos = data.get('reintentos', 0)
            puede_reintentar = reintentos < config.MAX_REINTENTOS
            
            if not puede_reintentar:
                logger.warning(
                    f"Pregunta_id {data.get('pregunta_id')} excedió máximo de reintentos "
                    f"({reintentos}/{config.MAX_REINTENTOS}). Score final: {data.get('score')}"
                )
            
            return puede_reintentar
            
        except Exception as e:
            logger.error(f"Error verificando reintentos: {str(e)}")
            return False


def create_kafka_source(topic: str) -> KafkaSource:
    """
    Crea un KafkaSource para consumir mensajes.
    
    Args:
        topic: Nombre del tópico de Kafka
        
    Returns:
        KafkaSource configurado
    """
    return KafkaSource.builder() \
        .set_bootstrap_servers(config.KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(topic) \
        .set_group_id(f'flink-consumer-{topic}') \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


def create_kafka_sink(topic: str) -> KafkaSink:
    """
    Crea un KafkaSink para producir mensajes.
    
    Args:
        topic: Nombre del tópico de Kafka
        
    Returns:
        KafkaSink configurado
    """
    return KafkaSink.builder() \
        .set_bootstrap_servers(config.KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()


def main():
    """
    Función principal que configura y ejecuta el job de Flink.
    """
    logger.info("=" * 80)
    logger.info("Iniciando Flink Quality Processor Job")
    logger.info("=" * 80)
    logger.info(f"Kafka Bootstrap Servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Umbral de Score: {config.UMBRAL_SCORE}")
    logger.info(f"Máximo de Reintentos: {config.MAX_REINTENTOS}")
    logger.info(f"Paralelismo: {config.FLINK_PARALLELISM}")
    logger.info("=" * 80)
    
    # Crear entorno de ejecución
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.FLINK_PARALLELISM)
    
    # Habilitar checkpointing para tolerancia a fallos
    env.enable_checkpointing(config.FLINK_CHECKPOINT_INTERVAL)
    
    logger.info("Entorno de Flink configurado")
    
    # ========== STREAM PRINCIPAL: Respuestas Exitosas del LLM ==========
    
    # 1. Consumir respuestas exitosas de Kafka
    kafka_source = create_kafka_source(config.TOPIC_RESPUESTAS_EXITOSAS)
    
    respuestas_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source - Respuestas Exitosas"
    )
    
    logger.info(f"Stream source creado para tópico: {config.TOPIC_RESPUESTAS_EXITOSAS}")
    
    # 2. Calcular score de calidad
    scored_stream = respuestas_stream.map(
        ScoreCalculator(),
        output_type=Types.STRING()
    )
    
    logger.info("Score calculator configurado")
    
    # ========== BRANCH 1: Respuestas de Alta Calidad (score >= umbral) ==========
    
    # Filtrar respuestas con score alto
    high_quality_stream = scored_stream.filter(
        QualityFilter(config.UMBRAL_SCORE, filter_high=True)
    )
    
    # Enviar a tópico de resultados validados
    high_quality_sink = create_kafka_sink(config.TOPIC_RESULTADOS_VALIDADOS)
    high_quality_stream.sink_to(high_quality_sink).name("Sink - Resultados Validados")
    
    logger.info(f"Branch 1: Resultados validados -> {config.TOPIC_RESULTADOS_VALIDADOS}")
    
    # ========== BRANCH 2: Respuestas de Baja Calidad (score < umbral) ==========
    
    # Filtrar respuestas con score bajo
    low_quality_stream = scored_stream.filter(
        QualityFilter(config.UMBRAL_SCORE, filter_high=False)
    )
    
    # Preparar para reintento
    retry_stream = low_quality_stream.map(
        RetryMapper(),
        output_type=Types.STRING()
    )
    
    # Filtrar solo las que no han excedido el máximo de reintentos
    valid_retry_stream = retry_stream.filter(MaxRetryFilter())
    
    # Reinyectar al tópico de preguntas nuevas
    retry_sink = create_kafka_sink(config.TOPIC_PREGUNTAS_NUEVAS)
    valid_retry_stream.sink_to(retry_sink).name("Sink - Reintentos")
    
    logger.info(f"Branch 2: Reintentos -> {config.TOPIC_PREGUNTAS_NUEVAS}")
    
    # ========== EJECUTAR JOB ==========
    
    logger.info("=" * 80)
    logger.info("Iniciando ejecución del job de Flink...")
    logger.info("El job procesará mensajes continuamente.")
    logger.info("Presiona Ctrl+C para detener.")
    logger.info("=" * 80)
    
    try:
        env.execute("Flink Quality Processor Job")
    except KeyboardInterrupt:
        logger.info("Job interrumpido por el usuario")
    except Exception as e:
        logger.error(f"Error ejecutando job: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    main()