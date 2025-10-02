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

# Este servicio no expone puerto (es un cliente)

# Por defecto, ejecutar con distribución Poisson
CMD ["python", "app.py", "poisson", "20", "500"]