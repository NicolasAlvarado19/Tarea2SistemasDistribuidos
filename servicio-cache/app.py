"""
SERVICIO DE CACHÉ - REDIS

Este servicio funciona como una memoria rápida que guarda las respuestas
más consultadas para no tener que procesarlas nuevamente.

Piensa en esto como tu memoria a corto plazo:
- Si te preguntan "¿cuál es tu nombre?" 10 veces, no lo piensas 10 veces,
  respondes automáticamente porque ya lo tienes en la memoria.
"""

# ============================================================================
# IMPORTACIONES
# ============================================================================

# FastAPI: para crear la API REST
from fastapi import FastAPI, HTTPException

# Pydantic: para validar datos que recibimos
from pydantic import BaseModel

# Redis: cliente para conectarse a Redis (base de datos en memoria)
import redis

# JSON: para convertir objetos Python a texto y viceversa
import json

# hashlib: para crear "huellas digitales" únicas de las preguntas
import hashlib

# Para leer variables de entorno
import os

# Uvicorn: servidor web
import uvicorn

# ============================================================================
# CREAR LA APLICACIÓN FASTAPI
# ============================================================================

app = FastAPI(title="Servicio de Caché")

# ============================================================================
# CONEXIÓN A REDIS
# ============================================================================

# Leer configuración desde variables de entorno
# Redis es como una gran tabla de memoria RAM super rápida
host_redis = os.getenv('REDIS_HOST', 'localhost')  # Dirección del servidor Redis
puerto_redis = int(os.getenv('REDIS_PORT', 6379))  # Puerto (6379 es el estándar)

# Crear la conexión a Redis
# decode_responses=True hace que Redis devuelva texto en vez de bytes
cliente_redis = redis.Redis(
    host=host_redis,
    port=puerto_redis,
    decode_responses=True  # Importante: devuelve strings en vez de bytes
)

# ============================================================================
# CONFIGURACIÓN DEL CACHÉ
# ============================================================================

# Tamaño máximo del caché (número de preguntas que puede guardar)
# Esto es ajustable para los experimentos
TAMANIO_CACHE = int(os.getenv('TAMANIO_CACHE', 1000))

# Política de expulsión (qué hacer cuando el caché se llena)
# LRU = Least Recently Used (expulsa lo menos usado recientemente)
POLITICA_EXPULSION = os.getenv('POLITICA_EXPULSION', 'allkeys-lru')

# ============================================================================
# MODELOS DE DATOS PARA LA API
# ============================================================================

class ConsultaCache(BaseModel):
    """
    Datos que se envían para verificar si una pregunta está en caché
    """
    texto_pregunta: str  # La pregunta completa


class DatosAlmacenar(BaseModel):
    """
    Datos para guardar una respuesta en el caché
    """
    texto_pregunta: str      # La pregunta
    respuesta_llm: str       # La respuesta del LLM
    puntuacion_similitud: float  # El score de similitud


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def obtener_hash_pregunta(pregunta: str) -> str:
    """
    Convierte una pregunta en un código único (hash).
    
    ¿Por qué? Porque las preguntas pueden ser muy largas y Redis
    funciona mejor con claves cortas y únicas.
    
    Ejemplo:
        pregunta = "¿Cómo hacer un pastel de chocolate?"
        hash = "a3f5c89d1e2b..."
    
    Parámetros:
        pregunta: texto completo de la pregunta
    
    Retorna:
        string: código hash MD5 de 32 caracteres
    """
    
    # Convertir el texto a bytes (necesario para MD5)
    pregunta_bytes = pregunta.encode('utf-8')
    
    # Calcular el hash MD5 (una "huella digital" única)
    # MD5 siempre genera el mismo hash para el mismo texto
    hash_md5 = hashlib.md5(pregunta_bytes)
    
    # Convertir el hash a una cadena hexadecimal
    # Ejemplo: "a3f5c89d1e2b4f7c..."
    return hash_md5.hexdigest()


# ============================================================================
# ENDPOINT 1: VERIFICAR SI ESTÁ EN CACHÉ
# ============================================================================

@app.post("/verificar")
def verificar_cache(consulta: ConsultaCache):
    """
    Verifica si una pregunta ya está guardada en el caché.
    
    Flujo:
    1. Convertir la pregunta a un hash único
    2. Buscar ese hash en Redis
    3. Si existe → devolver los datos (CACHE HIT)
    4. Si no existe → devolver que no está (CACHE MISS)
    
    Parámetros:
        consulta: objeto con el texto_pregunta
    
    Retorna:
        dict con:
            - encontrado: True/False
            - datos: respuesta y score (si está en caché)
            - contador_accesos: cuántas veces se consultó
    """
    
    # Convertir la pregunta a un hash único
    # Ejemplo: "¿Cómo...?" → "a3f5c89d..."
    hash_pregunta = obtener_hash_pregunta(consulta.texto_pregunta)
    
    # Buscar en Redis usando el hash como clave
    # Es como buscar en un diccionario: datos = diccionario[hash]
    datos_en_cache = cliente_redis.get(hash_pregunta)
    
    # ========================================================================
    # CASO A: La pregunta SÍ está en caché (CACHE HIT)
    # ========================================================================
    if datos_en_cache:
        
        # Convertir el texto JSON a un objeto Python
        # Redis guarda todo como texto, necesitamos convertirlo
        datos = json.loads(datos_en_cache)
        
        # Incrementar el contador de accesos
        # Es como llevar la cuenta de cuántas veces se usa esta respuesta
        clave_contador = f"{hash_pregunta}:contador"
        cliente_redis.incr(clave_contador)  # incr = incrementar en 1
        
        # Obtener el valor actualizado del contador
        contador = int(cliente_redis.get(clave_contador) or 1)
        
        # Devolver que SÍ se encontró
        return {
            "encontrado": True,
            "datos": datos,
            "contador_accesos": contador,
            "mensaje": "CACHE HIT - Respuesta encontrada en caché"
        }
    
    # ========================================================================
    # CASO B: La pregunta NO está en caché (CACHE MISS)
    # ========================================================================
    else:
        return {
            "encontrado": False,
            "mensaje": "CACHE MISS - Respuesta no encontrada, procesar con LLM"
        }


# ============================================================================
# ENDPOINT 2: GUARDAR EN CACHÉ
# ============================================================================

@app.post("/almacenar")
def almacenar_en_cache(datos: DatosAlmacenar):
    """
    Guarda una respuesta en el caché para futuras consultas.
    
    Flujo:
    1. Convertir la pregunta a un hash
    2. Preparar los datos a guardar (respuesta + score)
    3. Guardar en Redis con el hash como clave
    4. Inicializar el contador en 1
    
    Parámetros:
        datos: objeto con pregunta, respuesta_llm y puntuacion_similitud
    
    Retorna:
        dict confirmando que se guardó
    """
    
    # Convertir la pregunta a un hash único
    hash_pregunta = obtener_hash_pregunta(datos.texto_pregunta)
    
    # Preparar los datos que vamos a guardar
    # Solo guardamos lo esencial: respuesta y score
    datos_a_guardar = {
        "respuesta_llm": datos.respuesta_llm,
        "puntuacion_similitud": datos.puntuacion_similitud
    }
    
    # Convertir el objeto Python a texto JSON
    # Redis solo puede guardar texto, no objetos
    datos_json = json.dumps(datos_a_guardar)
    
    # Guardar en Redis
    # Es como: diccionario[hash] = datos_json
    cliente_redis.set(hash_pregunta, datos_json)
    
    # Inicializar el contador de accesos en 1
    clave_contador = f"{hash_pregunta}:contador"
    cliente_redis.set(clave_contador, 1)
    
    # Confirmar que se guardó
    return {
        "estado": "guardado",
        "hash": hash_pregunta,
        "mensaje": "Respuesta almacenada en caché correctamente"
    }


# ============================================================================
# ENDPOINT 3: OBTENER ESTADÍSTICAS DEL CACHÉ
# ============================================================================

@app.get("/estadisticas")
def obtener_estadisticas():
    """
    Devuelve estadísticas sobre el rendimiento del caché.
    
    Esto es útil para los experimentos y el informe.
    
    Retorna:
        dict con:
            - total_claves: cuántas preguntas hay guardadas
            - hits: cuántas veces se encontró en caché
            - misses: cuántas veces NO se encontró
            - tasa_acierto: porcentaje de hits (0-100%)
    """
    
    # Obtener información de Redis
    # info('stats') devuelve estadísticas de uso
    info_redis = cliente_redis.info('stats')
    
    # Extraer las métricas importantes
    hits = info_redis.get('keyspace_hits', 0)      # Cuántas veces se encontró
    misses = info_redis.get('keyspace_misses', 0)  # Cuántas veces NO se encontró
    
    # Calcular la tasa de acierto (hit rate)
    # Fórmula: hits / (hits + misses) * 100
    # Si total es 0, evitamos división por cero
    total_consultas = hits + misses
    if total_consultas > 0:
        tasa_acierto = (hits / total_consultas) * 100
    else:
        tasa_acierto = 0.0
    
    # Contar cuántas claves (preguntas) hay guardadas
    # dbsize() devuelve el número total de claves en Redis
    total_claves = cliente_redis.dbsize()
    
    # Devolver todas las estadísticas
    return {
        "total_claves": total_claves,
        "hits": hits,
        "misses": misses,
        "tasa_acierto": round(tasa_acierto, 2),
        "total_consultas": total_consultas
    }


# ============================================================================
# ENDPOINT 4: LIMPIAR TODO EL CACHÉ
# ============================================================================

@app.delete("/limpiar")
def limpiar_cache():
    """
    Borra TODAS las claves del caché.
    
    ADVERTENCIA: Esta operación no se puede deshacer.
    Útil para empezar experimentos desde cero.
    
    Retorna:
        dict confirmando la limpieza
    """
    
    # flushdb() borra toda la base de datos actual de Redis
    cliente_redis.flushdb()
    
    return {
        "estado": "limpiado",
        "mensaje": "Caché completamente limpiado"
    }


# ============================================================================
# ENDPOINT 5: VERIFICAR SALUD DEL SERVICIO
# ============================================================================

@app.get("/salud")
def verificar_salud():
    """
    Verifica que el servicio y Redis estén funcionando.
    
    Intenta hacer un PING a Redis para confirmar la conexión.
    
    Retorna:
        dict con el estado del servicio
    """
    try:
        # Intentar hacer ping a Redis
        # Si responde, la conexión está OK
        cliente_redis.ping()
        
        return {
            "estado": "ok",
            "servicio": "cache",
            "redis_conectado": True
        }
    
    except Exception as error:
        # Si falla, devolver error
        raise HTTPException(
            status_code=500,
            detail=f"Error conectando a Redis: {str(error)}"
        )


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    # Leer el puerto desde variables de entorno
    puerto = int(os.getenv('PUERTO_CACHE', 8001))
    
    # Iniciar el servidor
    uvicorn.run(app, host="0.0.0.0", port=puerto)