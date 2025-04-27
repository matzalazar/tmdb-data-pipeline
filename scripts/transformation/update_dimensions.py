from deltalake import DeltaTable
from utils.delta_functions import merge_silver_dimension
import pandas as pd
import numpy as np

# 1. Leer los nuevos movie_id procesados
df_new_movies = pd.read_csv('data/tmp/new_movie_ids.csv')

# 2. Cargar Bronze y filtrar solo nuevas películas
df_md = DeltaTable('data/delta/bronze/tmdb/movie_details').to_pandas()
df_md = df_md[df_md['movie_id'].isin(df_new_movies['movie_id'])]

# 3. Armar registros de dimensiones SOLO con las nuevas películas

# Dim Countries
df_countries = df_md.explode('origin_countries')
iso_countries = pd.read_csv('data/metadata/iso_3166.csv', encoding='latin1')
df_countries = df_countries.merge(
    iso_countries, left_on='origin_countries', right_on='iso2', how='left'
)[['movie_id', 'origin_countries', 'country_common']].drop_duplicates()
merge_silver_dimension(df_countries, 'data/delta/silver/tmdb/dim_countries', merge_keys=['origin_countries'])

# Dim Languages
iso_languages = pd.read_csv('data/metadata/iso_639.csv', encoding='latin1')
df_languages = df_md.merge(
    iso_languages, left_on='original_language', right_on='ISO_code', how='left'
)[['movie_id', 'original_language', 'Language']].drop_duplicates()
merge_silver_dimension(df_languages, 'data/delta/silver/tmdb/dim_languages', merge_keys=['original_language'])

# Dim Genres
df_genres = df_md.explode('genres').dropna(subset=['genres'])
df_genres['genre_id'] = df_genres['genres'].apply(lambda x: x['id'] if isinstance(x, dict) else None)
df_genres['genre_name'] = df_genres['genres'].apply(lambda x: x['name'] if isinstance(x, dict) else None)
df_dim_genres = df_genres[['genre_id', 'genre_name']].drop_duplicates().dropna()
merge_silver_dimension(df_dim_genres, 'data/delta/silver/tmdb/dim_genres', merge_keys=['genre_id'])
