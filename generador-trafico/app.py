
import requests
import time
import numpy as np
import pandas as pd
import os
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Crear el logger
logger = logging.getLogger(__name__)

class GeneradorTrafico:
   
    def __init__(self, distribucion='poisson', tasa=10):
    
        self.distribucion = distribucion
        self.tasa = tasa  
        self.url_almacenamiento = os.getenv('URL_ALMACENAMIENTO', 'http://almacenamiento:8000')
        self.url_cache = os.getenv('URL_CACHE', 'http://cache:8001')
        self.url_puntuacion = os.getenv('URL_PUNTUACION', 'http://puntuacion:8002')
        
        logger.info("Cargando dataset de preguntas...")
        
        ruta_dataset = '/datos/preguntas_10k.csv'
        self.df_preguntas = pd.read_csv(ruta_dataset)
        
        logger.info(f"Dataset cargado: {len(self.df_preguntas)} preguntas disponibles")
        
        self.consultas_enviadas = 0
        self.consultas_exitosas = 0
        self.consultas_fallidas = 0
        self.cache_hits = 0
        self.cache_misses = 0
    
    
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
    
    
    def procesar_consulta(self, pregunta, respuesta_original):
        try:
            logger.info(f"Verificando caché para: {pregunta[:50]}...")
            
            respuesta_cache = requests.post(
                f'{self.url_cache}/verificar',
                json={"texto_pregunta": pregunta},
                timeout=10
            )
            
            datos_cache = respuesta_cache.json()
    
            if datos_cache.get('encontrado', False):
                logger.info("CACHE HIT - Respuesta encontrada en caché")
                
                self.cache_hits += 1
                
                requests.post(
                    f'{self.url_almacenamiento}/almacenar',
                    json={
                        "texto_pregunta": pregunta,
                        "respuesta_original": respuesta_original
                    },
                    timeout=10
                )
                
                return True

            else:
                logger.info(" CACHE MISS - Procesando con LLM...")
                
                self.cache_misses += 1
                
               
                respuesta_puntuacion = requests.post(
                    f'{self.url_puntuacion}/generar-y-puntuar',
                    json={
                        "pregunta": pregunta,
                        "respuesta_original": respuesta_original
                    },
                    timeout=60  
                )
                
                datos_puntuacion = respuesta_puntuacion.json()
                
                logger.info(f"Similitud: {datos_puntuacion['puntuacion_similitud']:.3f}")
                
                requests.post(
                    f'{self.url_cache}/almacenar',
                    json={
                        "texto_pregunta": pregunta,
                        "respuesta_llm": datos_puntuacion['respuesta_llm'],
                        "puntuacion_similitud": datos_puntuacion['puntuacion_similitud']
                    },
                    timeout=10
                )
                
                requests.post(
                    f'{self.url_almacenamiento}/almacenar',
                    json={
                        "texto_pregunta": pregunta,
                        "respuesta_original": respuesta_original,
                        "respuesta_llm": datos_puntuacion['respuesta_llm'],
                        "puntuacion_similitud": datos_puntuacion['puntuacion_similitud']
                    },
                    timeout=10
                )
                
                return True
        
        except requests.exceptions.Timeout:
            logger.error(" TIMEOUT - El servicio tardó mucho")
            return False
        
        except requests.exceptions.ConnectionError:
           
            logger.error(" ERROR DE CONEXIÓN - No se pudo conectar al servicio")
            return False
        
        except Exception as error:
            logger.error(f"ERROR: {str(error)}")
            return False
    
    
    def ejecutar(self, num_consultas=500, duracion_minutos=None):
        
        
        logger.info("="*80)
        logger.info(" INICIANDO GENERADOR DE TRÁFICO")
        logger.info("="*80)
        logger.info(f" Distribución: {self.distribucion}")
        logger.info(f"Tasa: {self.tasa} consultas/minuto")
        logger.info(f"Total consultas: {num_consultas}")
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
            
            if exito:
                self.consultas_exitosas += 1
            else:
                self.consultas_fallidas += 1
            
            if self.consultas_enviadas % 50 == 0:
                self.mostrar_progreso()
            
            tiempo_espera = self.obtener_tiempo_entre_llegadas()
            time.sleep(tiempo_espera)
        
        self.mostrar_resumen_final(tiempo_inicio)
    
    
    def mostrar_progreso(self):
        
        logger.info("="*80)
        logger.info(f" PROGRESO: {self.consultas_enviadas} consultas enviadas")
        logger.info(f" Exitosas: {self.consultas_exitosas}")
        logger.info(f"Fallidas: {self.consultas_fallidas}")
        logger.info(f" Cache Hits: {self.cache_hits}")
        logger.info(f"Cache Misses: {self.cache_misses}")
        
        if self.consultas_enviadas > 0:
            tasa_hit = (self.cache_hits / self.consultas_enviadas) * 100
            logger.info(f"Tasa de acierto del caché: {tasa_hit:.1f}%")
        
        logger.info("="*80)
    
    
    def mostrar_resumen_final(self, tiempo_inicio):
        """Muestra el resumen final de la ejecución"""
        
        tiempo_total = time.time() - tiempo_inicio
        
        logger.info("")
        logger.info("="*80)
        logger.info("🏁 GENERACIÓN DE TRÁFICO COMPLETADA")
        logger.info("="*80)
        logger.info(f"Tiempo total: {tiempo_total:.2f} segundos ({tiempo_total/60:.2f} minutos)")
        logger.info(f"Total consultas: {self.consultas_enviadas}")
        logger.info(f"Exitosas: {self.consultas_exitosas}")
        logger.info(f"Fallidas: {self.consultas_fallidas}")
        logger.info(f"Cache Hits: {self.cache_hits}")
        logger.info(f"Cache Misses: {self.cache_misses}")
        
        if self.consultas_enviadas > 0:
            tasa_exito = (self.consultas_exitosas / self.consultas_enviadas) * 100
            tasa_hit = (self.cache_hits / self.consultas_enviadas) * 100
            throughput = self.consultas_enviadas / tiempo_total
            
            logger.info(f"Tasa de éxito: {tasa_exito:.1f}%")
            logger.info(f"Tasa de acierto del caché: {tasa_hit:.1f}%")
            logger.info(f"Throughput: {throughput:.2f} consultas/segundo")
        
        logger.info("="*80)

if __name__ == "__main__":
    
    distribucion = sys.argv[1] if len(sys.argv) > 1 else 'poisson'
    tasa = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    num_consultas = int(sys.argv[3]) if len(sys.argv) > 3 else 500
    
    generador = GeneradorTrafico(distribucion=distribucion, tasa=tasa)
    
    generador.ejecutar(num_consultas=num_consultas)