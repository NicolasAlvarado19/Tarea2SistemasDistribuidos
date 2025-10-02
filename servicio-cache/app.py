from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
import hashlib
import os
import uvicorn

app = FastAPI(title="Servicio de Caché")

host_redis = os.getenv('REDIS_HOST', 'localhost')  
puerto_redis = int(os.getenv('REDIS_PORT', 6379))  

cliente_redis = redis.Redis(
    host=host_redis,
    port=puerto_redis,
    decode_responses=True  
)

TAMANIO_CACHE = int(os.getenv('TAMANIO_CACHE', 1000))

POLITICA_EXPULSION = os.getenv('POLITICA_EXPULSION', 'allkeys-lru')

class ConsultaCache(BaseModel):
    
    texto_pregunta: str  


class DatosAlmacenar(BaseModel):
    
    texto_pregunta: str      
    respuesta_llm: str      
    puntuacion_similitud: float  

def obtener_hash_pregunta(pregunta: str) -> str:
    pregunta_bytes = pregunta.encode('utf-8')
    
    hash_md5 = hashlib.md5(pregunta_bytes)
   
    return hash_md5.hexdigest()

@app.post("/verificar")
def verificar_cache(consulta: ConsultaCache):
    hash_pregunta = obtener_hash_pregunta(consulta.texto_pregunta)
    
    datos_en_cache = cliente_redis.get(hash_pregunta)
    if datos_en_cache:
        
        datos = json.loads(datos_en_cache)
        
        clave_contador = f"{hash_pregunta}:contador"
        cliente_redis.incr(clave_contador)  
        
        
        contador = int(cliente_redis.get(clave_contador) or 1)
        
        return {
            "encontrado": True,
            "datos": datos,
            "contador_accesos": contador,
            "mensaje": "CACHE HIT - Respuesta encontrada en caché"
        }
 
    else:
        return {
            "encontrado": False,
            "mensaje": "CACHE MISS - Respuesta no encontrada, procesar con LLM"
        }

@app.post("/almacenar")
def almacenar_en_cache(datos: DatosAlmacenar):
    hash_pregunta = obtener_hash_pregunta(datos.texto_pregunta)
    
    datos_a_guardar = {
        "respuesta_llm": datos.respuesta_llm,
        "puntuacion_similitud": datos.puntuacion_similitud
    }
    datos_json = json.dumps(datos_a_guardar)
 
    cliente_redis.set(hash_pregunta, datos_json)
 
    clave_contador = f"{hash_pregunta}:contador"
    cliente_redis.set(clave_contador, 1)
    
    return {
        "estado": "guardado",
        "hash": hash_pregunta,
        "mensaje": "Respuesta almacenada en caché correctamente"
    }

@app.get("/estadisticas")
def obtener_estadisticas():
   
    info_redis = cliente_redis.info('stats')
    
    hits = info_redis.get('keyspace_hits', 0)      
    misses = info_redis.get('keyspace_misses', 0)  
 
    total_consultas = hits + misses
    if total_consultas > 0:
        tasa_acierto = (hits / total_consultas) * 100
    else:
        tasa_acierto = 0.0
   
    total_claves = cliente_redis.dbsize()
    
   
    return {
        "total_claves": total_claves,
        "hits": hits,
        "misses": misses,
        "tasa_acierto": round(tasa_acierto, 2),
        "total_consultas": total_consultas
    }

@app.delete("/limpiar")
def limpiar_cache():
   
    cliente_redis.flushdb()
    
    return {
        "estado": "limpiado",
        "mensaje": "Caché completamente limpiado"
    }


@app.get("/salud")
def verificar_salud():

    try:
        cliente_redis.ping()
        
        return {
            "estado": "ok",
            "servicio": "cache",
            "redis_conectado": True
        }
    
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Error conectando a Redis: {str(error)}"
        )

if __name__ == "__main__":
    puerto = int(os.getenv('PUERTO_CACHE', 8001))
    uvicorn.run(app, host="0.0.0.0", port=puerto)