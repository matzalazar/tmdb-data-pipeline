#!/bin/bash

LOG_FILE="logs/pipeline_$(date +%Y%m%d).log"
mkdir -p logs

log() {
  echo "$1" | tee -a "$LOG_FILE"
}

run_step() {
  log ">> Ejecutando: $1"
  if python -m "$1" 2>&1 | tee -a "$LOG_FILE"; then
    log "✅ $1 completado correctamente"
  else
    log "❌ ERROR en $1"
  fi
}

log "🚀 Iniciando pipeline TMDB - $(date)"

log "🔽 Fase 1: Extracción"
run_step scripts.extraction.extract_now_playing
run_step scripts.extraction.extract_movie_details

log "🛠️ Fase 2: Construcción Capa Bronze"
run_step scripts.building.build_now_playing_bronze
run_step scripts.building.build_movie_details_bronze

log "🔁 Fase 3: Transformación y Capa Silver"
run_step scripts.transformation.build_now_playing_silver
run_step scripts.transformation.build_movie_details_silver

log "📊 Fase 4: Actualización de dimensiones"
run_step scripts.transformation.update_dimensions

log "✅ Pipeline TMDB finalizado - $(date)"
