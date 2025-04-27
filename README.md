# 🎬 TMDB Pipeline

## Descripción General

Este proyecto implementa un pipeline de extracción, almacenamiento y transformación de datos desde la API de [The Movie Database (TMDB)](https://www.themoviedb.org/), utilizando **Delta Lake** como formato de persistencia y organización de las capas de datos (Bronze y Silver).

El objetivo principal es construir datasets históricos versionados que permitan:

- Monitorear la permanencia de películas en cartelera.
- Seguir la evolución diaria de ratings y votaciones.
- Consolidar atributos estáticos como géneros, países e idiomas.
- Facilitar análisis exploratorios y entrenar modelos predictivos.

El diseño modular permite ejecutar cada fase por separado, incrementar los datos incorporando nuevos endpoints, así como orquestar el flujo completo mediante un script bash.

## Requisitos

Instalación de dependencias:

```bash
pip install -r requirements.txt
```

Requiere:
- `pandas`
- `requests`
- `pyarrow`
- `deltalake`

Además, es obligatorio obtener una **api_key** desde la [web de TMDB](https://www.themoviedb.org/) y configurarla en el archivo `tmdb.conf`:

```ini
[TMDB]
api_key = TU_API_KEY_AQUI
```

## Estructura del Proyecto

```
.
├── config/                                 # Configuración de credenciales (api_key TMDB)
│   └── tmdb.conf
├── data/                                   # Datos y almacenamiento Delta Lake
│   ├── delta/                              # Datos persistidos en Bronze y Silver (Delta Lake)
│   ├── metadata/                           # CSVs de apoyo (ISO 3166 y ISO 639)
│   │   ├── iso_3166.csv
│   │   └── iso_639.csv
│   ├── raw/                                # Datos crudos descargados desde la API (JSON)
│   └── tmp/                                # Datos temporales (p.ej., nuevos movie_id)
├── logs/                                   # Logs de ejecución
├── notebooks/                              # Análisis exploratorios (EDA)
│   ├── eda_bronze.ipynb
│   └── eda_silver.ipynb
├── README.md                               # Documentación principal del proyecto
├── requirements.txt                        # Dependencias de Python
├── run_pipeline.sh                         # Script bash para ejecutar el pipeline completo
├── scripts/                                # Scripts Python del pipeline
│   ├── building/                           # Construcción de capa Bronze
│   │   ├── build_movie_details_bronze.py
│   │   └── build_now_playing_bronze.py
│   ├── extraction/                         # Extracción de datos desde la API
│   │   ├── extract_movie_details.py
│   │   └── extract_now_playing.py
│   ├── transformation/                     # Transformaciones y construcción de Silver
│   │   ├── build_movie_details_silver.py
│   │   ├── build_now_playing_silver.py
│   │   └── update_dimensions.py
│   └── __init__.py
└── utils/                                  # Utilidades comunes (cliente TMDB, logger, funciones Delta)
    ├── delta_functions.py
    ├── logger.py
    ├── tmdb_client.py
    └── __init__.py

```

## Flujo del Pipeline

### 1. Fase de Extracción

| Script                        | Descripción |
|-------------------------------|-------------|
| `extract_now_playing.py`      | Obtiene películas en cartelera (snapshot diario). Guarda JSON crudo. |
| `extract_movie_details.py`    | Usa los movie_id del JSON de cartelera para extraer detalles estáticos. Guarda un JSON por película. |

### 2. Fase de Construcción Bronze

| Script                          | Descripción |
|---------------------------------|-------------|
| `build_now_playing_bronze.py`   | Lee el JSON diario de cartelera y crea la tabla Bronze con append diario. |
| `build_movie_details_bronze.py` | Lee los JSON por película y construye tabla Bronze con `MERGE` por `movie_id`. |

### 3. Fase de Transformación Silver

| Script                          | Descripción |
|---------------------------------|-------------|
| `build_now_playing_silver.py`   | Limpieza, tipado y clasificación por cuartiles (`vote_average`) en categorías de calidad. |
| `build_movie_details_silver.py` | Enriquecimiento, tipado, detección de plataformas y escritura con `MERGE`. |
| `update_dimensions.py`          | Crea dimensiones de países, idiomas y géneros cruzando con metadata ISO. |

## Almacenamiento y Delta Lake

Se utiliza Delta Lake para persistir la información con capacidades de:

- Lectura incremental
- Versionado de datos
- Operaciones de `append` y `merge`
- Particionado diario por fecha (`now_playing`) o por `movie_id` (`movie_details`)

**Bronze:** datos tal como vienen de la API, con mínima limpieza  
**Silver:** datos estructurados, enriquecidos y listos para análisis o consumo analítico

## Decisiones Técnicas y de Diseño

1. Consolidación de scripts de extracción para simplificar la arquitectura y reducir redundancia.
2. Aplicación de `append` diario en la capa Bronze para preservar históricos, particionado por fecha.
3. Uso de metadata ISO externa descargada desde Kaggle para países e idiomas.
4. Construcción de dimensiones Silver garantizando integridad referencial.
5. Preservación del `imdb_id` en Silver para futuros enriquecimientos de datos.

## Transformaciones Realizadas

- Limpieza y tipado correcto de todos los campos relevantes.
- Imputación de valores faltantes en campos no críticos.
- Clasificación de películas en categorías cualitativas (`Mala`, `Regular`, `Buena`, `Excelente`).
- Enriquecimiento con géneros extraídos y disponibilidad en plataformas detectadas.
- Generación y actualización de dimensiones de países, idiomas y géneros.

## Análisis Exploratorio

Se incluyen notebooks en la carpeta `notebooks/`:

- `eda_bronze.ipynb`: exploración de datos crudos y snapshots
- `eda_silver.ipynb`: validación de la Capa Silver y sus enriquecimientos

## Ejecución del Pipeline

El pipeline puede ejecutarse completo con:

```bash
./run_pipeline.sh
```

Este script orquesta:

1. Extracción cruda
2. Construcción de Capas Bronze
3. Construcción de Capas Silver
4. Actualización de dimensiones

## Futuras Extensiones

- Incorporación de la evolución de ratings y cantidad de votos de forma incremental.
- Enriquecimiento con keywords, productoras y otros metadatos.
- Tiempo promedio en cartelera por género o país.
- Modelos predictivos sobre éxito y permanencia.
- Detección de sesgos en la representación lingüística y cultural.