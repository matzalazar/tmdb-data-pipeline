from deltalake import DeltaTable
from utils.delta_functions import merge_silver_movie_details
from datetime import date
from collections.abc import Sequence
import numpy as np
import pandas as pd
import os

# 1. Cargar la Capa Bronze
df_md = DeltaTable('data/delta/bronze/tmdb/movie_details').to_pandas()
df_md = df_md.dropna(subset=['movie_id'])

# 2. Detectar nuevos movie_id
try:
    dt_silver = DeltaTable('data/delta/silver/tmdb/movie_details')
    existing_movie_ids = dt_silver.to_pandas()['movie_id'].unique()
except:
    existing_movie_ids = []

df_md = df_md[~df_md['movie_id'].isin(existing_movie_ids)]

if df_md.empty:
    print("No hay movie_id nuevos para procesar.")
    exit(0)

# 3. Imputación en caso de NaN
imputation_mapping = {
    'title': 'Sin título', 
    'runtime': 0, 
    'budget': 0, 
    'imdb_id': 'unknown',
    'homepage': 'unknown', 
    'original_language': 'unknown', 
    'release_date': '1900-01-01'
}

df_md = df_md.fillna(imputation_mapping)
df_md['homepage'] = df_md['homepage'].replace('', 'unknown').fillna('unknown')

# 4. Casteo de tipos
df_md['movie_id'] = df_md['movie_id'].astype('Int64')
df_md['title'] = df_md['title'].astype('string')
df_md['runtime'] = df_md['runtime'].astype('Int32')
df_md['budget'] = df_md['budget'].astype('Int64')
df_md['imdb_id'] = df_md['imdb_id'].astype('string')
df_md['homepage'] = df_md['homepage'].astype('string')
df_md['original_language'] = df_md['original_language'].astype('string')
df_md['release_date'] = pd.to_datetime(df_md['release_date'], errors='coerce')

# 5. Manejo de ids para genre_ids
df_md['genres'] = df_md['genres'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
df_md['genre_ids'] = df_md['genres'].apply(
    lambda genres_list: [genre['id'] for genre in genres_list if isinstance(genre, dict) and 'id' in genre]
    if isinstance(genres_list, list) else []
)

# 6. Columna enriquecida con flag de disponibilidad en plataformas
platform_keywords = ['netflix', 'tv.apple', 'primevideo', 'disneyplus', 'hbo', 'paramountplus', 'mubi', 'amazon']
df_md['available_on_platform'] = df_md['homepage'].str.contains('|'.join(platform_keywords), case=False, na=False)

# 7. Tabla Silver final
df_silver = df_md[[
    'movie_id', 'title', 'runtime', 'budget', 'imdb_id', 'homepage',
    'original_language', 'release_date', 'genre_ids', 'origin_countries', 'available_on_platform'
]].copy()
df_silver['origin_countries'] = df_silver['origin_countries'].apply(str)

# 8. Guardar solo nuevos en la Capa Silver usando MERGE
merge_silver_movie_details(df_silver, 'data/delta/silver/tmdb/movie_details')

# 9. Dejar lista de movie_id procesados para actualizar tabla de dimensiones
csv_path = 'data/tmp/new_movie_ids.csv'
os.makedirs(os.path.dirname(csv_path), exist_ok=True)

# Leer lo ya guardado, si existe
if os.path.exists(csv_path):
    prev_ids = pd.read_csv(csv_path)
    df_union = pd.concat([prev_ids, df_silver[['movie_id']]])
    df_union = df_union.drop_duplicates(subset=['movie_id'])
else:
    df_union = df_silver[['movie_id']].drop_duplicates()

# Guardar actualizado
df_union.to_csv(csv_path, index=False)
