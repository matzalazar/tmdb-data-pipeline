from deltalake import DeltaTable
from utils.delta_functions import overwrite_silver_now_playing
from datetime import date
import pandas as pd

# 1. Cargar la Capa Bronze
df_np = DeltaTable('data/delta/bronze/tmdb/now_playing').to_pandas()

# 2. Limpieza básica: eliminar nulos en atributos clave
df_np = df_np.dropna(subset=['movie_id', 'date'])

# 3. Imputación de valores faltantes en atributos no críticos con NaN
imputation_mapping = {
    'title': 'Sin título',
    'vote_average': 0.0,
    'vote_count': 0,
    'popularity': 0.0
}
df_np = df_np.fillna(value=imputation_mapping)

# 4. Eliminar duplicados exactos por movie_id y date
df_np = df_np.drop_duplicates(subset=['movie_id', 'date'])

# 5. Casteo de tipos
df_np['movie_id'] = df_np['movie_id'].astype('Int64')
df_np['title'] = df_np['title'].astype('string')
df_np['vote_average'] = df_np['vote_average'].astype('float64')
df_np['vote_count'] = df_np['vote_count'].astype('Int32')
df_np['popularity'] = df_np['popularity'].astype('float64')

# 6. Enriquecimiento con categorías de voto usando pandas.cut
quartiles = df_np['vote_average'].quantile([0.25, 0.5, 0.75])
q1, q2, q3 = quartiles[0.25], quartiles[0.5], quartiles[0.75]

bins = [-float('inf'), q1, q2, q3, float('inf')]
labels = ['Mala', 'Regular', 'Buena', 'Excelente']

df_np['rating_category'] = pd.cut(
    df_np['vote_average'],
    bins=bins,
    labels=labels,
    include_lowest=True
).astype(pd.CategoricalDtype(categories=labels, ordered=False))

# 7. Guardar en la Capa Silver
overwrite_silver_now_playing(df_np, 'data/delta/silver/tmdb/now_playing')
