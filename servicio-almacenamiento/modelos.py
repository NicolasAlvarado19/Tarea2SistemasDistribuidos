from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from datetime import datetime
import os

Base = declarative_base()

class Pregunta(Base):
    __tablename__ = 'preguntas'
 
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    texto_pregunta = Column(Text, nullable=False, unique=True)
   
    respuesta_original = Column(Text, nullable=False)
    
    respuesta_llm = Column(Text, nullable=True)
   
    puntuacion_similitud = Column(Float, nullable=True)
    
    contador_accesos = Column(Integer, default=1)
    
    fecha_creacion = Column(DateTime, default=datetime.utcnow)
    
    fecha_actualizacion = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<Pregunta(id={self.id}, accesos={self.contador_accesos})>"

def crear_motor_bd():
    usuario = os.getenv('POSTGRES_USER', 'admin')
    
    contrasena = os.getenv('POSTGRES_PASSWORD', 'admin123')
    
    host = os.getenv('POSTGRES_HOST', 'localhost')
    
    base_datos = os.getenv('POSTGRES_DB', 'yahoo_qa')
    
    url_bd = f"postgresql://{usuario}:{contrasena}@{host}/{base_datos}"
    
    motor = create_engine(url_bd, echo=False)
    
    return motor


def crear_sesion():
    
    motor = crear_motor_bd()
    
    Sesion = sessionmaker(bind=motor)
    
    return Sesion()

def inicializar_bd():
    motor = crear_motor_bd()
    
    Base.metadata.create_all(motor)
    
    print("Base de datos inicializada correctamente")