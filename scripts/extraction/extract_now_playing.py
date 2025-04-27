import requests
import json
import os
from datetime import datetime
from utils.tmdb_client import get_config
from utils.logger import setup_logger

logger = setup_logger('extract_now_playing_raw')
config = get_config()
api_key = config['tmdb']['api_key']

# Definir path para guardar el JSON
raw_path = f"data/raw/now_playing/now_playing_{datetime.today().strftime('%Y%m%d')}.json"

# Verificar si ya existe el JSON, si no, llamar a la API y guardar el crudo
if not os.path.exists(raw_path):
    all_movies = []
    for page in range(1, 6):        # Extrae 100 películas - 20 por página -, parámetro modificable
        url = "https://api.themoviedb.org/3/movie/now_playing"
        params = {"api_key": api_key, "language": "en-US", "page": page}
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data_page = response.json()
            all_movies.extend(data_page.get('results', []))
            logger.info(f'Página {page} descargada con {len(data_page.get("results", []))} películas')
        else:
            logger.warning(f"Error en la página {page}: {response.status_code}")
            break

    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    with open(raw_path, 'w') as f:
        json.dump(all_movies, f)
    logger.info(f'Datos crudos guardados en {raw_path}')
else:
    logger.info(f"Archivo ya existe: {raw_path}")
