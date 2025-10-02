"""
GENERADOR DE TRÁFICO

Este servicio simula usuarios reales haciendo preguntas al sistema.
Es como un robot que hace de "muchos usuarios" consultando el sistema.

Funcionalidad:
1. Lee preguntas del dataset
2. Las envía al sistema siguiendo diferentes patrones de tráfico
3. Simula comportamiento real (algunas preguntas son más populares)
"""

# ============================================================================
# IMPORTACIONES
# ============================================================================

# requests: para hacer peticiones HTTP a los otros servicios
import requests

# time: para controlar el tiempo entre consultas
import time

# numpy: para generar números aleatorios con distribuciones
import numpy as np

# pandas: para leer el CSV con las preguntas
import pandas as pd

# Para leer variables de entorno
import os

# Para hacer logs informativos
import logging

# Para manejar argumentos de línea de comandos
import sys

# ============================================================================
# CONFIGURAR LOGGING
# ============================================================================

# Configurar el sistema de logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Crear el logger
logger = logging.getLogger(__name__)

# ============================================================================
# CLASE GENERADOR DE TRÁFICO
# ============================================================================

class GeneradorTrafico:
    """
    Clase que simula tráfico de usuarios consultando el sistema.
    """
    
    def __init__(self, distribucion='poisson', tasa=10):
        """
        Inicializa el generador de tráfico.
        
        Parámetros:
            distribucion: tipo de distribución ('poisson', 'uniforme', 'rafagas')
            tasa: consultas por minuto
        """
        
        # Guardar configuración
        self.distribucion = distribucion
        self.tasa = tasa  # consultas por minuto
        
        # URLs de los servicios
        # En Docker, los nombres de los servicios son sus hostnames
        self.url_almacenamiento = os.getenv('URL_ALMACENAMIENTO', 'http://almacenamiento:8000')
        self.url_cache = os.getenv('URL_CACHE', 'http://cache:8001')
        self.url_puntuacion = os.getenv('URL_PUNTUACION', 'http://puntuacion:8002')
        
        # Cargar el dataset de preguntas
        logger.info("Cargando dataset de preguntas...")
        
        # Leer el CSV con las 10,000 preguntas
        ruta_dataset = '/datos/preguntas_10k.csv'
        self.df_preguntas = pd.read_csv(ruta_dataset)
        
        logger.info(f"Dataset cargado: {len(self.df_preguntas)} preguntas disponibles")
        
        # Estadísticas
        self.consultas_enviadas = 0
        self.consultas_exitosas = 0
        self.consultas_fallidas = 0
        self.cache_hits = 0
        self.cache_misses = 0
    
    
    def obtener_tiempo_entre_llegadas(self):
        """
        Calcula cuánto tiempo esperar antes de la siguiente consulta.
        
        Dependiendo de la distribución elegida, genera tiempos diferentes:
        - Poisson: llegadas aleatorias (realista)
        - Uniforme: llegadas regulares (cada X segundos)
        - Ráfagas: períodos tranquilos + períodos intensos
        
        Retorna:
            float: segundos a esperar
        """
        
        if self.distribucion == 'poisson':
            # Distribución exponencial (proceso de Poisson)
            # Es como simular llegadas aleatorias de usuarios reales
            # Si tasa = 10 consultas/minuto, promedio = 6 segundos entre consultas
            return np.random.exponential(60 / self.tasa)
        
        elif self.distribucion == 'uniforme':
            # Distribución uniforme (llegadas regulares)
            # Siempre espera el mismo tiempo
            # Si tasa = 10 consultas/minuto, espera 6 segundos siempre
            return 60 / self.tasa
        
        elif self.distribucion == 'rafagas':
            # Distribución con ráfagas (bursty)
            # 80% del tiempo: tráfico lento (el doble de espera)
            # 20% del tiempo: tráfico muy rápido (muy poca espera)
            # Simula patrones reales: momentos tranquilos + picos de tráfico
            
            if np.random.rand() < 0.8:
                # 80% del tiempo: lento
                return np.random.exponential(120 / self.tasa)
            else:
                # 20% del tiempo: muy rápido
                return np.random.exponential(10 / self.tasa)
        
        else:
            # Por defecto, usar Poisson
            return np.random.exponential(60 / self.tasa)
    
    
    def seleccionar_pregunta(self):
        """
        Selecciona una pregunta del dataset.
        
        Usa distribución Zipf para simular popularidad:
        - Algunas preguntas son MUY populares (se repiten mucho)
        - Otras son menos populares
        
        Esto es realista: en un sistema real, ciertas preguntas
        se hacen mucho más que otras.
        
        Retorna:
            tuple: (pregunta, respuesta_original)
        """
        
        # Distribución Zipf para simular popularidad
        # Zipf: pocas cosas son muy populares, muchas cosas son poco populares
        # Ejemplo: en YouTube, pocos videos tienen millones de vistas,
        # muchos videos tienen pocas vistas
        
        # Generar un índice con Zipf
        # zipf(1.5) genera números donde valores pequeños son más probables
        indice = np.random.zipf(1.5) - 1
        
        # Asegurarse de que el índice esté dentro del rango
        indice = min(indice, len(self.df_preguntas) - 1)
        
        # Obtener la fila correspondiente
        fila = self.df_preguntas.iloc[indice]
        
        return fila['pregunta'], fila['mejor_respuesta']
    
    
    def procesar_consulta(self, pregunta, respuesta_original):
        """
        Procesa una consulta completa a través del pipeline.
        
        Flujo:
        1. Verificar en caché
        2. Si está → actualizar almacenamiento (cache hit)
        3. Si NO está → procesar con LLM y almacenar (cache miss)
        
        Parámetros:
            pregunta: texto de la pregunta
            respuesta_original: respuesta de Yahoo! Answers
        
        Retorna:
            bool: True si fue exitosa, False si hubo error
        """
        
        try:
            # ================================================================
            # PASO 1: Verificar si está en caché
            # ================================================================
            
            logger.info(f"Verificando caché para: {pregunta[:50]}...")
            
            respuesta_cache = requests.post(
                f'{self.url_cache}/verificar',
                json={"texto_pregunta": pregunta},
                timeout=10
            )
            
            datos_cache = respuesta_cache.json()
            
            # ================================================================
            # CASO A: CACHE HIT (la pregunta ya está en caché)
            # ================================================================
            
            if datos_cache.get('encontrado', False):
                logger.info("✅ CACHE HIT - Respuesta encontrada en caché")
                
                self.cache_hits += 1
                
                # Actualizar el almacenamiento con el contador
                requests.post(
                    f'{self.url_almacenamiento}/almacenar',
                    json={
                        "texto_pregunta": pregunta,
                        "respuesta_original": respuesta_original
                    },
                    timeout=10
                )
                
                return True
            
            # ================================================================
            # CASO B: CACHE MISS (la pregunta NO está en caché)
            # ================================================================
            
            else:
                logger.info("❌ CACHE MISS - Procesando con LLM...")
                
                self.cache_misses += 1
                
                # Paso 2: Generar respuesta con LLM y calcular similitud
                respuesta_puntuacion = requests.post(
                    f'{self.url_puntuacion}/generar-y-puntuar',
                    json={
                        "pregunta": pregunta,
                        "respuesta_original": respuesta_original
                    },
                    timeout=60  # Timeout más largo porque el LLM puede tardar
                )
                
                datos_puntuacion = respuesta_puntuacion.json()
                
                logger.info(f"📊 Similitud: {datos_puntuacion['puntuacion_similitud']:.3f}")
                
                # Paso 3: Almacenar en caché
                requests.post(
                    f'{self.url_cache}/almacenar',
                    json={
                        "texto_pregunta": pregunta,
                        "respuesta_llm": datos_puntuacion['respuesta_llm'],
                        "puntuacion_similitud": datos_puntuacion['puntuacion_similitud']
                    },
                    timeout=10
                )
                
                # Paso 4: Almacenar en base de datos
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
            # Error: el servicio tardó mucho en responder
            logger.error("⏱️ TIMEOUT - El servicio tardó mucho")
            return False
        
        except requests.exceptions.ConnectionError:
            # Error: no se pudo conectar al servicio
            logger.error("🔌 ERROR DE CONEXIÓN - No se pudo conectar al servicio")
            return False
        
        except Exception as error:
            # Cualquier otro error
            logger.error(f"❌ ERROR: {str(error)}")
            return False
    
    
    def ejecutar(self, num_consultas=500, duracion_minutos=None):
        """
        Ejecuta el generador de tráfico.
        
        Parámetros:
            num_consultas: cuántas consultas enviar (default: 500)
            duracion_minutos: límite de tiempo (opcional)
        """
        
        logger.info("="*80)
        logger.info("🚀 INICIANDO GENERADOR DE TRÁFICO")
        logger.info("="*80)
        logger.info(f"📊 Distribución: {self.distribucion}")
        logger.info(f"⚡ Tasa: {self.tasa} consultas/minuto")
        logger.info(f"🎯 Total consultas: {num_consultas}")
        logger.info("="*80)
        
        # Tiempo de inicio
        tiempo_inicio = time.time()
        
        # Bucle principal
        while self.consultas_enviadas < num_consultas:
            
            # Verificar límite de tiempo (si se especificó)
            if duracion_minutos:
                tiempo_transcurrido = (time.time() - tiempo_inicio) / 60
                if tiempo_transcurrido > duracion_minutos:
                    logger.info(f"⏱️ Tiempo límite alcanzado: {duracion_minutos} minutos")
                    break
            
            # Seleccionar una pregunta
            pregunta, respuesta = self.seleccionar_pregunta()
            
            # Procesar la consulta
            exito = self.procesar_consulta(pregunta, respuesta)
            
            # Actualizar estadísticas
            self.consultas_enviadas += 1
            
            if exito:
                self.consultas_exitosas += 1
            else:
                self.consultas_fallidas += 1
            
            # Mostrar progreso cada 50 consultas
            if self.consultas_enviadas % 50 == 0:
                self.mostrar_progreso()
            
            # Esperar antes de la siguiente consulta
            tiempo_espera = self.obtener_tiempo_entre_llegadas()
            time.sleep(tiempo_espera)
        
        # Mostrar resumen final
        self.mostrar_resumen_final(tiempo_inicio)
    
    
    def mostrar_progreso(self):
        """Muestra el progreso actual"""
        
        logger.info("="*80)
        logger.info(f"📈 PROGRESO: {self.consultas_enviadas} consultas enviadas")
        logger.info(f"✅ Exitosas: {self.consultas_exitosas}")
        logger.info(f"❌ Fallidas: {self.consultas_fallidas}")
        logger.info(f"💾 Cache Hits: {self.cache_hits}")
        logger.info(f"🔍 Cache Misses: {self.cache_misses}")
        
        # Calcular tasa de acierto del caché
        if self.consultas_enviadas > 0:
            tasa_hit = (self.cache_hits / self.consultas_enviadas) * 100
            logger.info(f"📊 Tasa de acierto del caché: {tasa_hit:.1f}%")
        
        logger.info("="*80)
    
    
    def mostrar_resumen_final(self, tiempo_inicio):
        """Muestra el resumen final de la ejecución"""
        
        tiempo_total = time.time() - tiempo_inicio
        
        logger.info("")
        logger.info("="*80)
        logger.info("🏁 GENERACIÓN DE TRÁFICO COMPLETADA")
        logger.info("="*80)
        logger.info(f"⏱️ Tiempo total: {tiempo_total:.2f} segundos ({tiempo_total/60:.2f} minutos)")
        logger.info(f"📊 Total consultas: {self.consultas_enviadas}")
        logger.info(f"✅ Exitosas: {self.consultas_exitosas}")
        logger.info(f"❌ Fallidas: {self.consultas_fallidas}")
        logger.info(f"💾 Cache Hits: {self.cache_hits}")
        logger.info(f"🔍 Cache Misses: {self.cache_misses}")
        
        if self.consultas_enviadas > 0:
            tasa_exito = (self.consultas_exitosas / self.consultas_enviadas) * 100
            tasa_hit = (self.cache_hits / self.consultas_enviadas) * 100
            throughput = self.consultas_enviadas / tiempo_total
            
            logger.info(f"📈 Tasa de éxito: {tasa_exito:.1f}%")
            logger.info(f"📊 Tasa de acierto del caché: {tasa_hit:.1f}%")
            logger.info(f"⚡ Throughput: {throughput:.2f} consultas/segundo")
        
        logger.info("="*80)


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    
    # Leer argumentos de línea de comandos
    distribucion = sys.argv[1] if len(sys.argv) > 1 else 'poisson'
    tasa = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    num_consultas = int(sys.argv[3]) if len(sys.argv) > 3 else 500
    
    # Crear el generador
    generador = GeneradorTrafico(distribucion=distribucion, tasa=tasa)
    
    # Ejecutar
    generador.ejecutar(num_consultas=num_consultas)