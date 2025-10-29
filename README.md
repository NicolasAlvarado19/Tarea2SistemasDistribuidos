# Tarea 2 - Sistemas Distribuidos 2025-2
## Plataforma de Análisis de Preguntas y Respuestas con Procesamiento Asíncrono

### Integrantes
- Nicolas Alvarado Villanueva- 20387702-1

### Descripción
Sistema distribuido que procesa preguntas del dataset Yahoo! Answers, genera respuestas usando un LLM (Google Gemini), evalúa su calidad mediante similitud semántica, y persiste los resultados validados en PostgreSQL. Implementa procesamiento asíncrono con Apache Kafka para manejar errores de cuota y sobrecarga de forma resiliente.

---

## Arquitectura

### Componentes Principales

#### Infraestructura
- **PostgreSQL 15**: Base de datos principal
- **Redis 7**: Sistema de caché
- **Apache Kafka 7.5.0**: Bus de mensajes
- **Zookeeper**: Coordinación de Kafka
- **Kafka UI**: Interfaz de monitoreo

#### Servicios (APIs REST)
1. **Servicio de Almacenamiento** (Puerto 8000)
   - Gestiona persistencia en PostgreSQL
   - Consume resultados validados desde Kafka
   
2. **Servicio de Caché** (Puerto 8001)
   - Implementa caché LRU con Redis
   - Optimiza consultas repetidas

3. **Servicio de Puntuación** (Puerto 8002)
   - Genera respuestas con Google Gemini
   - Calcula similitud semántica con embeddings

#### Servicios de Procesamiento (Kafka Consumers)
1. **Generador de Tráfico**
   - Selecciona preguntas del dataset
   - Verifica existencia en BD
   - Envía a Kafka si no existe

2. **Kafka Consumer (LLM)**
   - Procesa preguntas desde Kafka
   - Llama a Google Gemini
   - Maneja errores de cuota/sobrecarga

3. **Puntuación Consumer**
   - Calcula score de similitud
   - Valida si cumple umbral (0.6)
   - Reintenta si score bajo

4. **Almacenamiento Consumer**
   - Persiste resultados validados
   - Evita duplicados

5. **Reintentador**
   - Procesa errores con delays
   - Respeta límites de reintentos

---

## Flujo de Datos
```
┌─────────────────────┐
│ Generador Tráfico   │
│ (Poisson λ=0.0033)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   PostgreSQL        │ ◄────────┐
│   ¿Existe pregunta? │          │
└──────────┬──────────┘          │
           │                     │
      NO   │   SÍ                │
           ▼    │                │
┌─────────────────────┐          │
│ Kafka: preguntas-   │          │
│        nuevas       │          │
└──────────┬──────────┘          │
           │                     │
           ▼                     │
┌─────────────────────┐          │
│ Kafka Consumer      │          │
│ (Gemini LLM)        │          │
└──────────┬──────────┘          │
           │                     │
      ┌────┴────┐                │
      │         │                │
  Éxito      Error              │
      │         │                │
      ▼         ▼                │
┌─────────┐ ┌─────────┐          │
│respues- │ │errores- │          │
│tas-     │ │cuota/   │          │
│exitosas │ │sobrecarga│         │
└────┬────┘ └────┬────┘          │
     │           │               │
     │           ▼               │
     │    ┌─────────────┐        │
     │    │Reintentador │        │
     │    │(delays)     │        │
     │    └──────┬──────┘        │
     │           │               │
     │           └───────────────┘
     │                    (retry)
     ▼
┌─────────────────────┐
│ Puntuación Consumer │
│ (Score similitud)   │
└──────────┬──────────┘
           │
      ┌────┴────┐
      │         │
 score≥0.6  score<0.6
      │         │
      ▼         └──────────┐
┌─────────────────────┐    │
│ resultados-         │    │
│ validados           │    │
└──────────┬──────────┘    │
           │               │
           ▼               │
┌─────────────────────┐    │
│ Almacenamiento      │    │
│ Consumer            │    │
│ (PostgreSQL)        │    │
└─────────────────────┘    │
                           │
                    (reintento)
```

---

## Topics de Kafka

| Topic | Descripción | Productores | Consumidores |
|-------|-------------|-------------|--------------|
| `preguntas-nuevas` | Preguntas pendientes | Generador, Reintentador | Kafka Consumer |
| `respuestas-exitosas` | Respuestas del LLM | Kafka Consumer | Puntuación Consumer |
| `resultados-validados` | Respuestas validadas | Puntuación Consumer | Almacenamiento Consumer |
| `errores-cuota` | Errores de límite | Kafka Consumer | Reintentador |
| `errores-sobrecarga` | Errores de sobrecarga | Kafka Consumer | Reintentador |

---

## Tecnologías

### Backend
- Python 3.9+
- FastAPI
- Kafka-Python 2.0.2
- psycopg2-binary 2.9.9
- redis 4.5.5

### Machine Learning
- google-generativeai 0.3.0
- sentence-transformers 2.2.2
- scikit-learn 1.3.0

### DevOps
- Docker
- Docker Compose

---

## Instrucciones de Despliegue

### Prerrequisitos
- Docker y Docker Compose instalados
- 8GB RAM mínimo
- 20GB espacio en disco
- API Key de Google Gemini

### Configuración

1. **Clonar repositorio**
```bash
git clone [URL_REPOSITORIO]
cd Tarea2SistemasDistribuidos
```

2. **Configurar variables de entorno**
```bash
cp .env.example .env
nano .env
```

Configurar:
```properties
GEMINI_API_KEY=tu_api_key_aqui
TASA_CONSULTAS=0.0033  # 12 req/hora
UMBRAL_SCORE=0.5
```

3. ### Iniciar Sistema
```bash
docker-compose build

docker-compose up -d postgres redis kafka zookeeper

sleep 30

docker-compose up -d

docker-compose ps

docker-compose logs -f
```

### Detener Sistema
```bash
# Detener todos los servicios
docker-compose down

# Eliminar volúmenes 
docker-compose down -v
```

---

## Monitoreo

### Kafka UI
- URL: http://localhost:8090
- Ver topics, mensajes, consumidores

### Logs
```bash
# Logs de un servicio específico
docker-compose logs -f [servicio]

# Servicios disponibles:
# - generador-trafico
# - kafka-consumer
# - puntuacion-consumer
# - almacenamiento
# - reintentador
# - puntuacion
# - cache
```

### PostgreSQL
```bash
# Conectar a la base de datos
docker exec -it postgres-tarea2 psql -U admin -d yahoo_qa

# Ver registros
SELECT COUNT(*) FROM resultados_validados;

# Ver estadísticas
SELECT 
    AVG(score) as score_promedio,
    MIN(score) as score_minimo,
    MAX(score) as score_maximo
FROM resultados_validados;
```

### Script de Métricas
```bash
./check_metrics.sh
```

---

## Configuración de Producción

### Parámetros Recomendados

**Generador de Tráfico:**
- Tasa: 0.0033 req/seg (12/hora, 288/día)
- Distribución: Poisson
- Evita agotar cuota de 250 req/día

**Puntuación:**
- Umbral: 0.5 (50% similitud mínima)
- Permite ~30-40% de validación

**Reintentos:**
- Máximo: 2 reintentos por error
- Backoff: Exponencial con base 2

---

## Pruebas

### Verificar Funcionamiento

1. **Ver preguntas en BD**
```bash
docker exec -it postgres-tarea2 psql -U admin -d yahoo_qa -c "SELECT COUNT(*) FROM resultados_validados;"
```

2. **Ver topics de Kafka**
```bash
docker exec -it kafka-tarea2 kafka-topics --list --bootstrap-server localhost:9092
```

3. **Ver mensajes en un topic**
```bash
docker exec -it kafka-tarea2 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic respuestas-exitosas \
  --from-beginning \
  --max-messages 5
```
---