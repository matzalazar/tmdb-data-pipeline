import pandas as pd
import json
import os
from utils.delta_functions import merge_bronze_movie_details
from utils.logger import setup_logger
from deltalake import DeltaTable

logger = setup_logger('load_bronze_movie_details')

raw_dir = 'data/raw/movie_details'
bronze_path = 'data/delta/bronze/tmdb/movie_details'
movie_details_list = []

# Obtener movie_ids ya procesados en Bronze
try:
    bronze_df = DeltaTable(bronze_path).to_pandas()
    processed_ids = set(bronze_df['movie_id'].unique())
except:
    processed_ids = set()

# Procesar solo los JSONs nuevos
if not os.path.exists(raw_dir):
    logger.error(f"No existe la carpeta {raw_dir}")
else:
    for filename in os.listdir(raw_dir):
        if filename.endswith(".json"):
            try:
                movie_id = int(filename.split("_")[1].split(".")[0])
                if movie_id in processed_ids:
                    continue  # ya procesado

                raw_path = os.path.join(raw_dir, filename)
                with open(raw_path, 'r') as f:
                    data = json.load(f)
                    movie_details_list.append({
                        'movie_id': data.get('id'),
                        'title': data.get('title'),
                        'runtime': data.get('runtime'),
                        'budget': data.get('budget'),
                        'genres': data.get('genres', []) if isinstance(data.get('genres', []), list) else [],
                        'imdb_id': data.get('imdb_id'),
                        'homepage': data.get('homepage'),
                        'origin_countries': data.get('origin_country', []) if isinstance(data.get('origin_country', []), list) else [],
                        'original_language': data.get('original_language'),
                        'production_companies': data.get('production_companies', []) if isinstance(data.get('production_companies', []), list) else [],
                        'release_date': data.get('release_date')
                    })
            except Exception as e:
                logger.warning(f"Error procesando {filename}: {e}")

# Guardar en Delta Lake si hay datos nuevos
if movie_details_list:
    details_df = pd.DataFrame(movie_details_list)
    merge_bronze_movie_details(details_df, bronze_path)
    logger.info('Detalles nuevos almacenados en Delta Lake con MERGE')
else:
    logger.info('No hay detalles nuevos para almacenar')
