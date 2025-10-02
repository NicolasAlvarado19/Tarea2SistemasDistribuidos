#Plataforma de análisis de preguntas y respuestas (Yahoo! Answers + Gemini)

#####RequisitosDespliegue#####

-Docker + Docker Compose v2 
-Python 3.10+ (para scripts locales)
-API Key de Gemini (Google AI Studio)

#####Configuración (.env)#####
Crea/edita .env en la raíz:

```env
# LLM
GEMINI_API_KEY=AIzaSyCQKhjTFe0K33Ikd1IYSx4WrCgqNT1Re60
GEMINI_MODEL=gemini-flash-latest

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_MAX_MEMORIA=256mb
REDIS_POLITICA_EXPULSION=allkeys-lru
TAMANIO_CACHE=1000

# Postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=yahoo_qa
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123

# Puertos
PUERTO_ALMACENAMIENTO=8000
PUERTO_CACHE=8001
PUERTO_PUNTUACION=8002

# Interno
PUNTUACION_URL=http://puntuacion:8002
EMBEDDINGS_MODEL_NAME=all-MiniLM-L6-v2

#####Despliegue#####

docker compose build
docker compose up -d
docker compose ps

#####Salud de servicios#####

curl -s http://localhost:8000/salud | jq .
curl -s http://localhost:8001/salud | jq .
curl -s http://localhost:8002/salud | jq .


#Esperado
almacenamiento → estado=ok
cache → estado=ok, redis_conectado=true
puntuacion → estado=ok, gemini_configurado=true y modelo activo

#####Dataset (10k) y carga#####

#Generar dataset estratificado (10k)
cd datos && python3 dataset.py && cd ..

#Cargar contra /guardar-qa (ajusta pausa si se bloquea el LLM)
python3 scripts/cargar_dataset.py --limite 10000 --sleep 0.35

#Conteo total de resultados
docker compose exec -T postgres psql -U admin -d yahoo_qa -c \
"SELECT COUNT(*) FROM public.qa_resultados;"
