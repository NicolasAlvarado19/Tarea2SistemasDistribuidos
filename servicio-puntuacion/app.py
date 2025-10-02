from fastapi import FastAPI
from pydantic import BaseModel
import os, logging
import google.generativeai as genai
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import uvicorn


os.environ.setdefault("SENTENCE_TRANSFORMERS_HOME", "/tmp/sbert-cache")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("puntuacion")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise RuntimeError(" GEMINI_API_KEY no está configurada.")

USER_MODEL = (os.getenv("GEMINI_MODEL") or "").strip()  
EMBEDDINGS_MODEL_NAME = os.getenv("EMBEDDINGS_MODEL_NAME", "all-MiniLM-L6-v2")

genai.configure(api_key=GEMINI_API_KEY)

def _with_prefix(name: str) -> str:
    return name if name.startswith("models/") else f"models/{name}"

def _variants(name: str):
    """Genera variantes razonables del nombre (con/sin -latest, con prefijo)."""
    if not name:
        return []
    base = name.replace("models/", "")
    cands = {base, base.replace("-latest", "")}
    out = set()
    for c in cands:
        out.add(c)
        out.add(_with_prefix(c))
    return list(out)

def elegir_modelo() -> str:

    preferidos = []
    if USER_MODEL:
        preferidos += _variants(USER_MODEL)
    preferidos += _variants("gemini-1.5-flash-latest")
    preferidos += _variants("gemini-1.5-flash")
    preferidos += _variants("gemini-1.5-pro-latest")
    preferidos += _variants("gemini-1.5-pro")
    preferidos += _variants("gemini-1.0-pro")

   
    soportan = set()
    try:
        modelos = list(genai.list_models())
        soportan = {
            m.name for m in modelos
            if "generateContent" in getattr(m, "supported_generation_methods", [])
        }
        log.info("Modelos con generateContent disponibles: %s",
                 ", ".join(sorted(soportan)) or "(ninguno visible)")
    except Exception as e:
        log.warning("No pude listar modelos (%s). Intento directo por prueba.", e)

    if soportan:
        for c in preferidos:
            if c in soportan or _with_prefix(c) in soportan:
                elegido = c if c.startswith("models/") else _with_prefix(c)
                log.info("Modelo compatible (listado): %s", elegido)
                return elegido

    for c in preferidos:
        try:
            genai.GenerativeModel(c).generate_content("ping")
            log.warning("Modelo no listado pero funcional: %s", c)
            return c
        except Exception:
            pass

    log.warning("Ningún candidato funcionó. Uso 'models/gemini-1.0-pro'.")
    return "models/gemini-1.0-pro"

EFFECTIVE_MODEL = elegir_modelo()

try:
    modelo_gemini = genai.GenerativeModel(EFFECTIVE_MODEL)
    log.info("Gemini configurado. Modelo activo: %s", EFFECTIVE_MODEL)
except Exception as e:
    raise RuntimeError(f"Error inicializando Gemini con '{EFFECTIVE_MODEL}': {e}") from e

log.info("Cargando embeddings '%s'...", EMBEDDINGS_MODEL_NAME)
modelo_embeddings = SentenceTransformer(EMBEDDINGS_MODEL_NAME)
log.info("Embeddings cargados.")

app = FastAPI(title="Servicio de Puntuación")

class SolicitudPuntuacion(BaseModel):
    pregunta: str
    respuesta_original: str

class RespuestaPuntuacion(BaseModel):
    respuesta_llm: str
    puntuacion_similitud: float
    respuesta_original: str

def generar_respuesta_llm(pregunta: str) -> str:
    prompt = (
        "Responde de forma breve, correcta y útil. Si hay ambigüedad, aclárala.\n\n"
        f"Pregunta: {pregunta}\n\nRespuesta:"
    )
    try:
        resp = modelo_gemini.generate_content(prompt)
        texto = (getattr(resp, "text", "") or "").strip()
        return texto or "[ERROR: Gemini respondió vacío]"
    except Exception as e:
        log.error("Error en generate_content: %s", e)
        return f"[ERROR: No se pudo generar respuesta - {e}]"

def calcular_similitud(a: str, b: str) -> float:
    try:
        vecs = modelo_embeddings.encode([a, b])
        sim = float(cosine_similarity(vecs[0].reshape(1,-1), vecs[1].reshape(1,-1))[0][0])
        return max(0.0, min(1.0, sim))
    except Exception as e:
        log.error("Error al calcular similitud: %s", e)
        return 0.0

@app.post("/generar-y-puntuar", response_model=RespuestaPuntuacion)
def generar_y_puntuar(s: SolicitudPuntuacion):
    resp = generar_respuesta_llm(s.pregunta)
    score = calcular_similitud(s.respuesta_original, resp)
    return RespuestaPuntuacion(
        respuesta_llm=resp,
        puntuacion_similitud=score,
        respuesta_original=s.respuesta_original
    )

class BodySoloGenerar(BaseModel):
    pregunta: str

@app.post("/solo-generar")
def solo_generar(body: BodySoloGenerar):
    return {"pregunta": body.pregunta, "respuesta_llm": generar_respuesta_llm(body.pregunta)}

class BodySoloSimilitud(BaseModel):
    texto1: str
    texto2: str

@app.post("/solo-similitud")
def solo_calcular_similitud(body: BodySoloSimilitud):
    sim = calcular_similitud(body.texto1, body.texto2)
    return {
        "texto1": body.texto1[:120] + ("..." if len(body.texto1) > 120 else ""),
        "texto2": body.texto2[:120] + ("..." if len(body.texto2) > 120 else ""),
        "similitud": round(sim, 4),
    }

@app.get("/salud")
def salud():
    return {
        "estado": "ok",
        "servicio": "puntuacion",
        "gemini_model": EFFECTIVE_MODEL,
        "gemini_configurado": True,
        "modelo_embeddings": EMBEDDINGS_MODEL_NAME,
        "embeddings_cargado": True,
    }

if __name__ == "__main__":
    puerto = int(os.getenv("PUERTO_PUNTUACION", "8002"))
    log.info("=" * 80)
    log.info("INICIANDO SERVICIO DE PUNTUACIÓN")
    log.info(" Puerto: %s", puerto)
    log.info("LLM: %s", EFFECTIVE_MODEL)
    log.info("Embeddings: %s", EMBEDDINGS_MODEL_NAME)
    log.info("=" * 80)
    uvicorn.run(app, host="0.0.0.0", port=puerto)
