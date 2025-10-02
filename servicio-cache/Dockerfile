# Imagen base Python
FROM python:3.11-slim

# Carpeta de trabajo
WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Exponer puerto
EXPOSE 8001

# Comando de inicio
CMD ["python", "app.py"]