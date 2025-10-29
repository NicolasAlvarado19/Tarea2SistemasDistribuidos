import time
import numpy as np
import pandas as pd
import os
import logging
import sys
import json
from kafka import KafkaProducer
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class GeneradorTrafico:
   
    def __init__(self, distribucion='poisson', tasa=10):
        self.distribucion = distribucion
        self.tasa = tasa  
        self.url_almacenamiento = os.getenv('URL_ALMACENAMIENTO', 'http://almacenamiento:8000')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        
        logger.info("Cargando dataset de preguntas...")
        ruta_dataset = '/datos/preguntas_10k.csv'
        self.df_preguntas = pd.read_csv(ruta_dataset)
        logger.info(f"Dataset cargado: {len(self.df_preguntas)} preguntas disponibles")
        
        # Inicializar productor de Kafka
        logger.info("Conectando a Kafka...")
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info(" Conectado a Kafka exitosamente")
        
        # Métricas
        self.consultas_enviadas = 0
        self.consultas_existentes = 0
        self.consultas_nuevas = 0
        self.consultas_fallidas = 0
    
    def obtener_tiempo_entre_llegadas(self):
        if self.distribucion == 'poisson':
            return np.random.exponential(60 / self.tasa)
        elif self.distribucion == 'uniforme':
            return 60 / self.tasa
        elif self.distribucion == 'rafagas':
            if np.random.rand() < 0.8:
                return np.random.exponential(120 / self.tasa)
            else:
                return np.random.exponential(10 / self.tasa)
        else:
            return np.random.exponential(60 / self.tasa)
    
    def seleccionar_pregunta(self):
        indice = np.random.zipf(1.5) - 1
        indice = min(indice, len(self.df_preguntas) - 1)
        fila = self.df_preguntas.iloc[indice]
        return fila['pregunta'], fila['respuesta']
    
    def verificar_pregunta_existe(self, pregunta):
        """
        Consulta la BD para verificar si la pregunta ya fue procesada
        """
        try:
            response = requests.get(
                f'{self.url_almacenamiento}/consultar',
                params={"pregunta": pregunta},
                timeout=5
            )
            
            if response.status_code == 200:
                datos = response.json()
                return datos.get('existe', False), datos.get('veces_consultada', 0)
            
            return False, 0
            
        except Exception as e:
            logger.error(f"Error al verificar pregunta en BD: {str(e)}")
            return False, 0
    
    def actualizar_contador_pregunta(self, pregunta):
        """
        Actualiza el contador de veces consultada en la BD
        """
        try:
            requests.post(
                f'{self.url_almacenamiento}/actualizar-contador',
                json={"pregunta": pregunta},
                timeout=5
            )
            logger.info(" Contador actualizado en BD")
            return True
        except Exception as e:
            logger.error(f"Error al actualizar contador: {str(e)}")
            return False
    
    def enviar_a_kafka(self, pregunta, respuesta_original):
        """
        Envía la pregunta al topic de Kafka para procesamiento asíncrono
        """
        try:
            mensaje = {
                "pregunta": pregunta,
                "respuesta_original": respuesta_original,
                "timestamp": time.time(),
                "intentos": 0
            }
            
            future = self.producer.send('preguntas-nuevas', value=mensaje)
            future.get(timeout=10)  # Esperar confirmación
            
            logger.info(" Pregunta enviada a Kafka (topic: preguntas-nuevas)")
            return True
            
        except Exception as e:
            logger.error(f" Error al enviar a Kafka: {str(e)}")
            return False
    
    def procesar_consulta(self, pregunta, respuesta_original):
        """
        Flujo asíncrono: Consulta BD → Si existe actualiza, si no envía a Kafka
        """
        try:
            logger.info(f" Verificando en BD: {pregunta[:50]}...")
            
            existe, veces = self.verificar_pregunta_existe(pregunta)
            
            if existe:
                logger.info(f" ENCONTRADA en BD (consultada {veces} veces) - Actualizando contador")
                self.consultas_existentes += 1
                self.actualizar_contador_pregunta(pregunta)
                return True
            else:
                logger.info(" NO ENCONTRADA - Enviando a Kafka para procesamiento")
                self.consultas_nuevas += 1
                return self.enviar_a_kafka(pregunta, respuesta_original)
        
        except Exception as error:
            logger.error(f" ERROR: {str(error)}")
            return False
    
    def ejecutar(self, num_consultas=500, duracion_minutos=None):
        logger.info("="*80)
        logger.info(" INICIANDO GENERADOR DE TRÁFICO - MODO ASÍNCRONO")
        logger.info("="*80)
        logger.info(f" Distribución: {self.distribucion}")
        logger.info(f"  Tasa: {self.tasa} consultas/minuto")
        logger.info(f" Total consultas: {num_consultas}")
        logger.info("="*80)
        
        tiempo_inicio = time.time()
        
        while self.consultas_enviadas < num_consultas:
            if duracion_minutos:
                tiempo_transcurrido = (time.time() - tiempo_inicio) / 60
                if tiempo_transcurrido > duracion_minutos:
                    logger.info(f" Tiempo límite alcanzado: {duracion_minutos} minutos")
                    break
            
            pregunta, respuesta = self.seleccionar_pregunta()
            exito = self.procesar_consulta(pregunta, respuesta)
            
            self.consultas_enviadas += 1
            
            if not exito:
                self.consultas_fallidas += 1
            
            if self.consultas_enviadas % 50 == 0:
                self.mostrar_progreso()
            
            tiempo_espera = self.obtener_tiempo_entre_llegadas()
            time.sleep(tiempo_espera)
        
        self.mostrar_resumen_final(tiempo_inicio)
        self.producer.close()
    
    def mostrar_progreso(self):
        logger.info("="*80)
        logger.info(f" PROGRESO: {self.consultas_enviadas} consultas procesadas")
        logger.info(f" Existentes en BD: {self.consultas_existentes}")
        logger.info(f" Nuevas (enviadas a Kafka): {self.consultas_nuevas}")
        logger.info(f" Fallidas: {self.consultas_fallidas}")
        
        if self.consultas_enviadas > 0:
            tasa_existentes = (self.consultas_existentes / self.consultas_enviadas) * 100
            logger.info(f" Tasa de preguntas ya procesadas: {tasa_existentes:.1f}%")
        
        logger.info("="*80)
    
    def mostrar_resumen_final(self, tiempo_inicio):
        tiempo_total = time.time() - tiempo_inicio
        
        logger.info("")
        logger.info("="*80)
        logger.info(" GENERACIÓN DE TRÁFICO COMPLETADA")
        logger.info("="*80)
        logger.info(f"  Tiempo total: {tiempo_total:.2f} segundos ({tiempo_total/60:.2f} minutos)")
        logger.info(f" Total consultas: {self.consultas_enviadas}")
        logger.info(f" Existentes en BD: {self.consultas_existentes}")
        logger.info(f" Nuevas (Kafka): {self.consultas_nuevas}")
        logger.info(f" Fallidas: {self.consultas_fallidas}")
        
        if self.consultas_enviadas > 0:
            tasa_exito = ((self.consultas_enviadas - self.consultas_fallidas) / self.consultas_enviadas) * 100
            tasa_nuevas = (self.consultas_nuevas / self.consultas_enviadas) * 100
            throughput = self.consultas_enviadas / tiempo_total
            
            logger.info(f" Tasa de éxito: {tasa_exito:.1f}%")
            logger.info(f" Tasa de preguntas nuevas: {tasa_nuevas:.1f}%")
            logger.info(f"⚡ Throughput: {throughput:.2f} consultas/segundo")
        
        logger.info("="*80)

if __name__ == "__main__":
    distribucion = sys.argv[1] if len(sys.argv) > 1 else 'poisson'
    tasa = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    num_consultas = int(sys.argv[3]) if len(sys.argv) > 3 else 500
    
    generador = GeneradorTrafico(distribucion=distribucion, tasa=tasa)
    generador.ejecutar(num_consultas=num_consultas)