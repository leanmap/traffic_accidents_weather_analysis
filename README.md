🚦 Análisis de Accidentes de Tráfico y Clima en Barcelona

Proyecto de ingeniería y ciencia de datos que analiza la relación entre las condiciones climáticas y los accidentes de tráfico en la ciudad de Barcelona.

## Descripción

Este proyecto construye un pipeline de datos end-to-end que extrae, transforma y prepara datos de accidentes de tráfico y condiciones meteorológicas de Barcelona para su posterior análisis y modelado predictivo.

La parte de **data engineering** implementa un pipeline ETL completo que:
- Extrae datos de accidentes desde la API del Open Data del Ajuntament de Barcelona
- Extrae datos meteorológicos históricos por hora desde la API de Open-Meteo
- Transforma y limpia ambos datasets, normalizando columnas, traduciendo valores del catalán al inglés y estandarizando tipos de datos
- Almacena los datos procesados en formato Parquet, listos para análisis

La parte de **data science** (en desarrollo) busca predecir si las condiciones climáticas influyen en la frecuencia de accidentes de tráfico en Barcelona.

## Fuentes de datos

- **Accidentes de tráfico**: [Open Data Ajuntament de Barcelona](https://opendata-ajuntament.barcelona.cat/data/es/dataset/accidents-causes-gu-bcn) — datos oficiales de la Guardia Urbana de Barcelona con información sobre causa, ubicación, fecha y hora de cada accidente.
- **Datos meteorológicos**: [Open-Meteo Archive API](https://open-meteo.com/) — datos históricos por hora de temperatura, precipitación, código meteorológico y velocidad del viento para Barcelona.

## Estructura del proyecto

```
traffic_accidents_weather_analysis/
├── main.py                        # Punto de entrada — corre el pipeline completo
├── src/
│   ├── extract/
│   │   ├── extract_accidents.py   # Extracción de accidentes desde la API
│   │   └── extract_weather.py     # Extracción de datos meteorológicos
│   └── transform/
│       ├── transform_accidents.py # Limpieza y transformación de accidentes
│       └── transform_weather.py   # Limpieza y transformación del clima
├── data/
│   ├── raw/                       # Datos crudos en JSON
│   ├── processed/                 # Datos procesados en Parquet
│   └── metadata/                  # Contexto de ejecución del pipeline
├── notebooks/                     # Exploración y análisis (en desarrollo)
├── .gitignore
└── README.md
```

## Instalación y uso

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/traffic_accidents_weather_analysis.git
cd traffic_accidents_weather_analysis
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python -m venv venv
source venv/bin/activate        # En Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Correr el pipeline

```bash
python main.py
```

El pipeline detecta automáticamente los dos años más recientes disponibles en la API, descarga los datos de accidentes y clima, los transforma y guarda los resultados en `data/processed/`.

## Próximos pasos — Data Science

- Cruce de datasets de accidentes y clima por fecha y hora
- Análisis exploratorio de la relación entre condiciones meteorológicas y frecuencia de accidentes
- Entrenamiento de un modelo de clasificación para predecir si las condiciones climáticas de un día aumentan la probabilidad de accidentes
- Evaluación del modelo y visualización de resultados