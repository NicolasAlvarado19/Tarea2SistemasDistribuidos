# ============================================================================
# DOCKERFILE PARA SERVICIO DE ALMACENAMIENTO
# ============================================================================

# Imagen base: Python 3.11 versión ligera
FROM python:3.11-slim

# Carpeta de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias del sistema operativo
# gcc: necesario para compilar psycopg2
# postgresql-client: herramientas de PostgreSQL
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copiar solo requirements.txt primero (para aprovechar cache de Docker)
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el código de la aplicación
COPY . .

# Puerto que usa el servicio
EXPOSE 8000

# Comando para iniciar el servicio
CMD ["python", "app.py"]
