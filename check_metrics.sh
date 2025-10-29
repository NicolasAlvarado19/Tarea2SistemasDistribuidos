#!/bin/bash

echo "=================================================="
echo "MÉTRICAS DEL SISTEMA"
echo "=================================================="
echo ""

echo "1. ESTADO DE SERVICIOS:"
docker-compose ps
echo ""

echo "2. TOPICS DE KAFKA:"
docker exec -it kafka-tarea2 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic preguntas-nuevas \
  --topic respuestas-exitosas \
  --topic resultados-validados \
  --topic errores-cuota
echo ""

echo "3. DATOS EN POSTGRESQL:"
docker exec -it postgres-tarea2 psql -U admin -d yahoo_qa -c "
SELECT 
    COUNT(*) as total_registros,
    ROUND(AVG(score)::numeric, 4) as score_promedio,
    ROUND(MIN(score)::numeric, 4) as score_minimo,
    ROUND(MAX(score)::numeric, 4) as score_maximo
FROM resultados_validados;
"
echo ""

echo "4. DISTRIBUCIÓN DE SCORES:"
docker exec -it postgres-tarea2 psql -U admin -d yahoo_qa -c "
SELECT 
    CASE 
        WHEN score >= 0.7 THEN '0.7-1.0 (Excelente)'
        WHEN score >= 0.6 THEN '0.6-0.7 (Bueno)'
        WHEN score >= 0.5 THEN '0.5-0.6 (Regular)'
        ELSE '<0.5 (Malo)'
    END as rango_score,
    COUNT(*) as cantidad,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM resultados_validados))::numeric, 2) as porcentaje
FROM resultados_validados
GROUP BY rango_score
ORDER BY rango_score DESC;
"
echo ""

echo "5. ÚLTIMAS 5 PREGUNTAS PROCESADAS:"
docker exec -it postgres-tarea2 psql -U admin -d yahoo_qa -c "
SELECT 
    LEFT(pregunta, 70) as pregunta,
    ROUND(score::numeric, 4) as score,
    fecha_creacion
FROM resultados_validados
ORDER BY fecha_creacion DESC
LIMIT 5;
"

echo ""
echo "=================================================="
