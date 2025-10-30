FROM flink:1.18.0-scala_2.12-java11

# Instalar Python 3.9 y herramientas necesarias
USER root

RUN apt-get update && \
    apt-get install -y python3.9 python3-pip python3.9-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Crear symlink de python
RUN ln -sf /usr/bin/python3.9 /usr/bin/python && \
    ln -sf /usr/bin/python3.9 /usr/bin/python3

# Actualizar pip
RUN python -m pip install --upgrade pip

# Crear directorio de trabajo
WORKDIR /opt/flink-job

# Copiar requirements e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar archivos del job
COPY flink_quality_processor.py .
COPY config.py .
COPY start-flink-job.sh .

# Dar permisos de ejecución al script
RUN chmod +x start-flink-job.sh

# Crear directorio para checkpoints
RUN mkdir -p /tmp/flink-checkpoints && \
    chown -R flink:flink /tmp/flink-checkpoints

# Volver al usuario flink
USER flink

# Variables de entorno
ENV PYTHONUNBUFFERED=1

# Comando por defecto
CMD ["./start-flink-job.sh"]