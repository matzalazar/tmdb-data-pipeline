# load_bronze_now_playing.py
import pandas as pd
import json
import os
from datetime import date, datetime
from utils.delta_functions import append_bronze_snapshot
from utils.logger import setup_logger

logger = setup_logger('load_bronze_now_playing')
today_str = date.today().isoformat()
raw_path = f"data/raw/now_playing/now_playing_{datetime.today().strftime('%Y%m%d')}.json"

# Leer el JSON si existe
if not os.path.exists(raw_path):
    logger.error(f"No se encontró el archivo crudo en {raw_path}")
    exit(1)

with open(raw_path, 'r') as f:
    raw_data = json.load(f)

# Construir el DataFrame
now_playing_df = pd.DataFrame([{
    'movie_id': m['id'],
    'title': m['title'],
    'vote_average': m['vote_average'],
    'vote_count': m['vote_count'],
    'popularity': m['popularity']
} for m in raw_data])

# Guardado en Delta Lake
if now_playing_df.empty:
    logger.warning('No se encontraron resultados en el JSON.')
else:
    append_bronze_snapshot(now_playing_df, 'data/delta/bronze/tmdb/now_playing', date_str=today_str)
    logger.info('Snapshot diario de now_playing almacenado en capa bronze')
