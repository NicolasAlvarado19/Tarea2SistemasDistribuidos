#!/bin/bash

echo "=========================================="
echo "Flink Quality Processor - Inicio"
echo "=========================================="

# Esperar a que Kafka esté disponible
echo "Esperando a que Kafka esté disponible..."
sleep 30

# Verificar que Kafka está accesible
echo "Verificando conectividad con Kafka..."
timeout 10 bash -c 'until echo > /dev/tcp/kafka/9092; do sleep 1; done' 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✓ Kafka está disponible"
else
    echo "⚠ No se pudo conectar a Kafka, continuando de todos modos..."
fi

# Verificar que PostgreSQL está disponible
echo "Verificando conectividad con PostgreSQL..."
timeout 10 bash -c 'until echo > /dev/tcp/postgres/5432; do sleep 1; done' 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✓ PostgreSQL está disponible"
else
    echo "⚠ No se pudo conectar a PostgreSQL, continuando de todos modos..."
fi

echo "=========================================="
echo "Iniciando Flink Job..."
echo "=========================================="

# Ejecutar el job de Flink
python /opt/flink-job/flink_quality_processor.py

# Si el job falla, mantener el contenedor vivo para debugging
if [ $? -ne 0 ]; then
    echo "=========================================="
    echo "ERROR: El job de Flink falló"
    echo "Manteniendo contenedor vivo para debugging..."
    echo "=========================================="
    tail -f /dev/null
fi