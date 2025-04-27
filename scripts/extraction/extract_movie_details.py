import requests
import json
import os
from utils.tmdb_client import get_config
from utils.logger import setup_logger
from datetime import date, datetime

logger = setup_logger('extract_movie_details_raw')
config = get_config()
api_key = config['tmdb']['api_key']

today_str = date.today().isoformat()

# Leer movie_ids desde el JSON crudo de now_playing
raw_path = f"data/raw/now_playing/now_playing_{datetime.today().strftime('%Y%m%d')}.json"

if os.path.exists(raw_path):
    with open(raw_path, 'r') as f:
        raw_data = json.load(f)
        movie_ids = [m['id'] for m in raw_data]
    logger.info(f'Se cargaron {len(movie_ids)} movie_ids desde el JSON crudo de now_playing')
else:
    logger.error(f"No se encontró el JSON crudo de now_playing: {raw_path}")
    movie_ids = []

# Leer movie_ids ya procesados
try:
    from deltalake import DeltaTable
    details = DeltaTable('data/delta/bronze/tmdb/movie_details').to_pandas()
    processed_ids = details['movie_id'].unique().tolist()
except:
    processed_ids = []

movies_to_fetch = [m for m in movie_ids if m not in processed_ids]
logger.info(f'{len(movies_to_fetch)} nuevas películas para extraer detalles')

# Guardar JSONs uno por uno si no existen
for movie_id in movies_to_fetch:
    raw_path = f"data/raw/movie_details/movie_{movie_id}.json"
    if not os.path.exists(raw_path):
        url = f"https://api.themoviedb.org/3/movie/{movie_id}"
        params = {"api_key": api_key, "language": "en-US"}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            os.makedirs(os.path.dirname(raw_path), exist_ok=True)
            with open(raw_path, 'w') as f:
                json.dump(data, f)
            logger.info(f'Detalles crudos de movie_id {movie_id} guardados')
        else:
            logger.warning(f"No se pudo obtener detalles de movie_id {movie_id}")
    else:
        logger.info(f'Detalle crudo de movie_id {movie_id} ya existe')
