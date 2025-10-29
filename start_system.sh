#!/bin/bash

echo "🚀 INICIANDO SISTEMA - TAREA 2 SISTEMAS DISTRIBUIDOS"
echo "=================================================="
echo ""

# Verificar que todos los servicios base estén corriendo
echo "1. Verificando servicios base..."
docker-compose ps

echo ""
echo "2. Reiniciando consumers..."
docker-compose restart kafka-consumer reintentador puntuacion-consumer almacenamiento

echo ""
echo "3. Esperando 10 segundos para que se estabilicen..."
sleep 10

echo ""
echo "4. Levantando generador de tráfico..."
docker-compose up -d generador-trafico

echo ""
echo "5. Sistema iniciado. Monitoreando logs..."
echo ""
echo "=================================================="
echo "📊 MONITOREO EN TIEMPO REAL"
echo "=================================================="
echo ""

# Mostrar logs en tiempo real
docker-compose logs -f generador-trafico kafka-consumer puntuacion-consumer almacenamiento
