"""
MODELOS DE BASE DE DATOS PARA POSTGRESQL

Este archivo define cómo se estructura la tabla de preguntas en PostgreSQL.
Es como crear el "molde" de cómo se guardarán los datos.
"""

# ============================================================================
# IMPORTACIONES (librerías que necesitamos)
# ============================================================================

# SQLAlchemy: librería para trabajar con bases de datos de forma fácil
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Para manejar fechas y horas
from datetime import datetime

# Para leer variables de entorno (como contraseñas)
import os

# ============================================================================
# CONFIGURACIÓN BASE
# ============================================================================

# Crear la clase base para todos los modelos
# Es como decirle a SQLAlchemy "todos los modelos heredarán de aquí"
Base = declarative_base()

# ============================================================================
# MODELO DE LA TABLA DE PREGUNTAS
# ============================================================================

class Pregunta(Base):
    """
    Esta clase define cómo se ve una fila en la tabla de preguntas.
    Cada objeto Pregunta = 1 fila en la base de datos
    """
    
    # Nombre de la tabla en PostgreSQL
    __tablename__ = 'preguntas'
    
    # ========================================================================
    # COLUMNAS DE LA TABLA (cada variable es una columna)
    # ========================================================================
    
    # ID: número único para cada pregunta (se auto-incrementa: 1, 2, 3...)
    # primary_key=True significa que es el identificador único
    # autoincrement=True significa que se genera automáticamente
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # El texto completo de la pregunta
    # Text permite textos largos (más de 255 caracteres)
    # nullable=False significa que NO puede estar vacío
    # unique=True significa que no puede haber dos preguntas iguales
    texto_pregunta = Column(Text, nullable=False, unique=True)
    
    # La mejor respuesta según Yahoo! Answers
    # nullable=False significa que SIEMPRE debe tener una respuesta
    respuesta_original = Column(Text, nullable=False)
    
    # La respuesta generada por el LLM (Gemini)
    # nullable=True significa que PUEDE estar vacío al principio
    respuesta_llm = Column(Text, nullable=True)
    
    # El puntaje de similitud (0.0 a 1.0)
    # Float es para números decimales
    # Puede ser None si aún no se calculó
    puntuacion_similitud = Column(Float, nullable=True)
    
    # Cuántas veces se ha consultado esta pregunta
    # default=1 significa que empieza en 1 la primera vez
    contador_accesos = Column(Integer, default=1)
    
    # Fecha y hora en que se creó el registro
    # datetime.utcnow obtiene la fecha/hora actual en UTC
    fecha_creacion = Column(DateTime, default=datetime.utcnow)
    
    # Fecha y hora de la última actualización
    # onupdate significa que se actualiza automáticamente cada vez que modificamos la fila
    fecha_actualizacion = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Método para mostrar información cuando hacemos print(pregunta)
    def __repr__(self):
        return f"<Pregunta(id={self.id}, accesos={self.contador_accesos})>"


# ============================================================================
# FUNCIÓN PARA CREAR LA CONEXIÓN A POSTGRESQL
# ============================================================================

def crear_motor_bd():
    """
    Esta función crea la "conexión" con PostgreSQL.
    Es como abrir la puerta para hablar con la base de datos.
    
    Retorna:
        motor: objeto que representa la conexión a PostgreSQL
    """
    
    # Leer las credenciales desde las variables de entorno (.env)
    # os.getenv('NOMBRE', 'valor_por_defecto') lee la variable o usa el valor por defecto
    
    # Usuario de PostgreSQL
    usuario = os.getenv('POSTGRES_USER', 'admin')
    
    # Contraseña de PostgreSQL
    contrasena = os.getenv('POSTGRES_PASSWORD', 'admin123')
    
    # Dirección del servidor (en Docker será 'postgres', local sería 'localhost')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    
    # Nombre de la base de datos
    base_datos = os.getenv('POSTGRES_DB', 'yahoo_qa')
    
    # Construir la URL de conexión
    # Formato: postgresql://usuario:contraseña@host/nombre_base_datos
    url_bd = f"postgresql://{usuario}:{contrasena}@{host}/{base_datos}"
    
    # Crear el motor (conexión) con SQLAlchemy
    # echo=False evita que imprima todos los SQL en la consola
    motor = create_engine(url_bd, echo=False)
    
    return motor


# ============================================================================
# FUNCIÓN PARA CREAR UNA SESIÓN (PARA HACER CONSULTAS)
# ============================================================================

def crear_sesion():
    """
    Crea una "sesión" para interactuar con la base de datos.
    Una sesión es como abrir un "cuaderno" donde escribes/lees datos.
    
    Retorna:
        sesion: objeto para hacer consultas INSERT, SELECT, UPDATE, DELETE
    """
    
    # Primero crear la conexión
    motor = crear_motor_bd()
    
    # Crear una fábrica de sesiones
    # bind=motor conecta la sesión al motor de PostgreSQL
    Sesion = sessionmaker(bind=motor)
    
    # Retornar una sesión nueva
    return Sesion()


# ============================================================================
# FUNCIÓN PARA INICIALIZAR LA BASE DE DATOS
# ============================================================================

def inicializar_bd():
    """
    Crea todas las tablas en PostgreSQL si no existen.
    Es como decirle a PostgreSQL: "Crea la tabla 'preguntas' con estas columnas"
    
    Si las tablas ya existen, no hace nada (no las borra ni duplica).
    """
    
    # Crear la conexión
    motor = crear_motor_bd()
    
    # Base.metadata.create_all() revisa todos los modelos (Pregunta)
    # y crea las tablas correspondientes si no existen
    Base.metadata.create_all(motor)
    
    # Mensaje de confirmación
    print("✅ Base de datos inicializada correctamente")