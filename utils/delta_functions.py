import os
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from deltalake.exceptions import TableNotFoundError

def ensure_path(path):
    """
    Crea el directorio especificado si no existe.
    """
    os.makedirs(path, exist_ok=True)

# ============================ #
# FUNCIONES CAPA BRONZE        #
# ============================ #

def merge_bronze_movie_details(df_new, data_path):
    """
    Realiza un MERGE de movie_details en Bronze.
    Inserta sólo los nuevos movie_id.
    """
    ensure_path(data_path)
    try:
        dt = DeltaTable(data_path)
        new_data_pa = pa.Table.from_pandas(df_new)
        dt.merge(
            source=new_data_pa,
            source_alias="new",
            target_alias="bronze",
            predicate="new.movie_id = bronze.movie_id"
        ).when_not_matched_insert_all().execute()
    except TableNotFoundError:
        write_deltalake(data_path, df_new, mode="overwrite")

def append_bronze_snapshot(df_new, data_path, date_str):
    """
    Guarda snapshots diarios en Bronze 
    (para datos cronológicamente variables como now_playing y ratings).
    """
    ensure_path(data_path)
    df_new['date'] = date_str  # Columna de partición
    write_deltalake(data_path, df_new, mode="append", partition_by=["date"])

# ============================ #
# FUNCIONES CAPA SILVER        #
# ============================ #

def overwrite_silver_now_playing(df, data_path):
    """
    Sobrescribe la tabla Silver de now_playing con esquema controlado.
    """
    schema = pa.schema([
        ('movie_id', pa.int64()),
        ('title', pa.string()),
        ('vote_average', pa.float64()),
        ('vote_count', pa.int32()),
        ('popularity', pa.float64()),
        ('rating_category', pa.dictionary(index_type=pa.int32(), value_type=pa.string())),
        ('date', pa.string())
    ])

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    write_deltalake(data_path, table, mode="overwrite", partition_by=["date"])

def merge_silver_dimension(df_new, data_path, merge_keys, schema=None):
    """
    Realiza un merge para actualizar la tabla de dimensión en Silver.
    Si un registro con la clave merge_keys ya existe, se omite.
    Si no existe, se inserta.
    """
    ensure_path(data_path)
    table_new = pa.Table.from_pandas(df_new, schema=schema, preserve_index=False) if schema else pa.Table.from_pandas(df_new, preserve_index=False)
    
    try:
        dt = DeltaTable(data_path)
        # Construir el predicado de merge basado en las claves de unión
        predicate = " and ".join([f"new.{key} = silver.{key}" for key in merge_keys])
        
        dt.merge(
            source=table_new,
            source_alias="new",
            target_alias="silver",
            predicate=predicate
        ).when_not_matched_insert_all().execute()
    except TableNotFoundError:
        # Si la tabla no existe, se crea con los registros nuevos.
        write_deltalake(data_path, table_new, mode="overwrite")

def merge_silver_movie_details(df, data_path):
    """
    Realiza un MERGE sobre la tabla Silver de movie_details.
    Solo inserta nuevos movie_id (idempotente).
    """
    ensure_path(data_path)

    schema = pa.schema([
        ('movie_id', pa.int64()),
        ('title', pa.string()),
        ('runtime', pa.int32()),
        ('budget', pa.int64()),
        ('imdb_id', pa.string()),
        ('homepage', pa.string()),
        ('original_language', pa.string()),
        ('release_date', pa.timestamp('ns')),
        ('genre_ids', pa.list_(pa.int64())),
        ('origin_countries', pa.string()),
        ('available_on_platform', pa.bool_())
    ])

    new_data_pa = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    try:
        dt = DeltaTable(data_path)
        dt.merge(
            source=new_data_pa,
            source_alias="new",
            target_alias="silver",
            predicate="new.movie_id = silver.movie_id"
        ).when_not_matched_insert_all().execute()

    except TableNotFoundError:
        write_deltalake(data_path, new_data_pa, mode="overwrite")
