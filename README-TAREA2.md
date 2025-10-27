# Tarea 2: Sistema Distribuido con Kafka y Flink

## Cambios principales vs Tarea 1

### Arquitectura Asíncrona
- ✅ Apache Kafka para colas de mensajes
- ✅ Procesamiento desacoplado con consumidores
- ✅ Manejo de errores con reintentos automáticos
- ✅ Sistema de feedback loop (próximamente Flink)

### Componentes Nuevos

1. **Kafka Consumer** (`kafka-consumer/`)
   - Consume preguntas desde Kafka
   - Llama al LLM
   - Maneja errores 429 (rate limit)

2. **Reintentador** (`reintentador/`)
   - Reintenta preguntas fallidas
   - Exponential backoff

3. **Kafka UI** (puerto 8090)
   - Monitoreo visual de tópicos

### Tópicos de Kafka

- `preguntas-nuevas`: Preguntas sin procesar
- `respuestas-exitosas`: Respuestas del LLM OK
- `errores-sobrecarga`: Errores 429 para reintentar

## Despliegue
```bash
# Construir servicios
docker-compose build

# Levantar todo
docker-compose up -d

# Ver logs
docker-compose logs -f kafka-consumer
```

## Interfaces Web

- Almacenamiento: http://localhost:8000
- Cache: http://localhost:8001
- Puntuación: http://localhost:8002
- Kafka UI: http://localhost:8090

## Pruebas

Enviar una pregunta:
```bash
curl -X POST http://localhost:8000/guardar-qa \
  -H "Content-Type: application/json" \
  -d '{"pregunta": "What is AI?", "respuesta_original": "Artificial Intelligence is..."}'
```

Verificar en Kafka UI: http://localhost:8090
