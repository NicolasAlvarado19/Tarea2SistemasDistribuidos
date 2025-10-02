"""
SERVICIO DE ALMACENAMIENTO
- Guarda la pregunta/respuesta original
- Llama al microservicio de puntuación para obtener:
    - respuesta_llm
    - puntuacion_similitud
- Persiste todo en PostgreSQL
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import os, requests
from datetime import datetime

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime, Text
)
from sqlalchemy.orm import declarative_base, sessionmaker

# -----------------------------
# Configuración de entorno
# -----------------------------
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "yahoo_qa")
PG_USER = os.getenv("POSTGRES_USER", "admin")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "admin123")

PUNTUACION_URL = os.getenv("PUNTUACION_URL", "http://puntuacion:8002")

DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# -----------------------------
# DB (SQLAlchemy)
# -----------------------------
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

class QAResultado(Base):
    __tablename__ = "qa_resultados"
    id = Column(Integer, primary_key=True, index=True)
    pregunta = Column(Text, nullable=False)
    respuesta_original = Column(Text, nullable=False)
    respuesta_llm = Column(Text, nullable=False)
    puntuacion_similitud = Column(Float, nullable=False)
    nivel = Column(String(16), nullable=False)  # alta | media | baja
    creado_en = Column(DateTime, default=datetime.utcnow, nullable=False)

Base.metadata.create_all(bind=engine)

# -----------------------------
# Utilidades
# -----------------------------
def nivel_similitud(s: float) -> str:
    if s >= 0.75: return "alta"
    if s >= 0.50: return "media"
    return "baja"

# -----------------------------
# API
# -----------------------------
app = FastAPI(title="Servicio de Almacenamiento")

class BodyGuardar(BaseModel):
    pregunta: str
    respuesta_original: str

class DTOResultado(BaseModel):
    id: int
    pregunta: str
    respuesta_original: str
    respuesta_llm: str
    puntuacion_similitud: float
    nivel: str
    creado_en: datetime

@app.get("/salud")
def salud():
    return {"estado": "ok", "servicio": "almacenamiento"}

@app.post("/guardar-qa", response_model=DTOResultado)
def guardar_qa(body: BodyGuardar):
    # 1) llamar a puntuación
    try:
        r = requests.post(
            f"{PUNTUACION_URL}/generar-y-puntuar",
            json={
                "pregunta": body.pregunta,
                "respuesta_original": body.respuesta_original,
            },
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Fallo llamando a puntuacion: {e}")

    resp_llm = data.get("respuesta_llm", "")
    sim = float(data.get("puntuacion_similitud", 0.0))
    lvl = nivel_similitud(sim)

    # 2) persistir
    db = SessionLocal()
    try:
        row = QAResultado(
            pregunta=body.pregunta,
            respuesta_original=body.respuesta_original,
            respuesta_llm=resp_llm,
            puntuacion_similitud=sim,
            nivel=lvl,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return DTOResultado(
            id=row.id,
            pregunta=row.pregunta,
            respuesta_original=row.respuesta_original,
            respuesta_llm=row.respuesta_llm,
            puntuacion_similitud=row.puntuacion_similitud,
            nivel=row.nivel,
            creado_en=row.creado_en,
        )
    finally:
        db.close()

@app.get("/qa/{qa_id}", response_model=DTOResultado)
def obtener_qa(qa_id: int):
    db = SessionLocal()
    try:
        row = db.get(QAResultado, qa_id)
        if not row:
            raise HTTPException(status_code=404, detail="No encontrado")
        return DTOResultado(
            id=row.id,
            pregunta=row.pregunta,
            respuesta_original=row.respuesta_original,
            respuesta_llm=row.respuesta_llm,
            puntuacion_similitud=row.puntuacion_similitud,
            nivel=row.nivel,
            creado_en=row.creado_en,
        )
    finally:
        db.close()

@app.get("/qa", response_model=List[DTOResultado])
def listar_qa(limit: int = 20, offset: int = 0):
    db = SessionLocal()
    try:
        q = db.query(QAResultado).order_by(QAResultado.id.desc()).offset(offset).limit(limit)
        out = []
        for r in q.all():
            out.append(DTOResultado(
                id=r.id,
                pregunta=r.pregunta,
                respuesta_original=r.respuesta_original,
                respuesta_llm=r.respuesta_llm,
                puntuacion_similitud=r.puntuacion_similitud,
                nivel=r.nivel,
                creado_en=r.creado_en,
            ))
        return out
    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn
    puerto = int(os.getenv("PUERTO_ALMACENAMIENTO", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=puerto)
