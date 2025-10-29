Este proyecto implementa un sistema distribuido para procesar preguntas y respuestas basado en un dataset real de Yahoo Answers.
El sistema basado en Docker incluye generación de tráfico, caché con políticas configurables (REDIS), integración con un modelo LLM de forma local (Ollama), cálculo de métricas de similitud y almacenamiento en Postgres.



# Arquitectura del sistema

El sistema está compuesto por 6 servicios en Docker:

- Generador de Tráfico: genera preguntas aleatorias desde el dataset y las envía al caché.

- Sistema de Caché: responde preguntas si ya existen  o consulta al LLM en caso de miss.

- Políticas de remoción soportadas: LRU, FIFO.

- Ollama LLM: conectado de forma local, devolviendo respuestas simuladas.

- Calculadora de Puntuación: compara la respuesta del dataset con la del LLM y asigna un score (puntaje) de similitud.

- PostGres: almacena los documentos con preguntas, respuestas, hits, miss, scores, etc.


# Requisitos previos

- Docker
- Powershell Extension para VSCode (en caso de no ejecutarse en windows)
- Ollama v0.12.3 (ollama.com)

# Instalación y uso
- Clonar el repositorio
```bash
git clone <link del github>
cd <carpeta raiz del repositorio>
```
- Instalación e iniciación local de Ollama via PowerShell ejecutando archivo: init-ollama.ps1

- Levantar el sistema
```bash
docker compose up --build  -d
```
- Verificar que los contenedores estén corriendo
```bash
docker ps
```


# Monitoreo del sistema

Existen 3 comandos clave para monitorear el sistema:

- Estado de los Servicios, Cache, Scoring (puntuación) y Base de Datos:
```bash
while ($true) {
Clear-Host
Write-Host "=== SISTEMA DE ANÁLISIS DE PREGUNTAS Y RESPUESTAS ===" -ForegroundColor Green
Write-Host "Estado: FUNCIONANDO" -ForegroundColor Green
Write-Host "Hora: $(Get-Date)" -ForegroundColor Gray
Write-Host ""

Write-Host "COLAS DE REDIS:" -ForegroundColor Yellow
$questions = docker exec nuevacarpeta2-redis-cache-1 redis-cli LLEN questions_queue
$uncached = docker exec nuevacarpeta2-redis-cache-1 redis-cli LLEN uncached_questions
$scoring = docker exec nuevacarpeta2-redis-cache-1 redis-cli LLEN scoring_queue
Write-Host " questions_queue: $questions"
Write-Host " uncached_questions: $uncached"
Write-Host " scoring_queue: $scoring"

Write-Host "`nBASE DE DATOS:" -ForegroundColor Yellow
$count = docker exec nuevacarpeta2-postgres-db-1 psql -U user -d yahoo_analysis -t -c "SELECT COUNT(*) FROM responses;"
$avg_score = docker exec nuevacarpeta2-postgres-db-1 psql -U user -d yahoo_analysis -t -c "SELECT ROUND(AVG(cosine_score)::numeric, 3) FROM responses WHERE cosine_score > 0;"
Write-Host " Total registros: $($count.Trim())"
Write-Host " Score promedio: $($avg_score.Trim())"

Write-Host "`nESTADO DE SERVICIOS:" -ForegroundColor Yellow
$services = @("traffic-generator", "cache-system", "llm-processor", "score-calculator")
foreach ($service in $services) {
$status = docker-compose ps -q $service | ForEach-Object { docker inspect $_ --format='{{.State.Status}}' }
if ($status -eq "running") {
Write-Host " ✅ $service" -ForegroundColor Green
} else {
Write-Host " ❌ $service - $status" -ForegroundColor Red
}
}

Write-Host "`nLOGS RECIENTES:" -ForegroundColor Yellow
docker-compose logs --tail=3 cache-system 2>$null | ForEach-Object { Write-Host " CACHE: $_" -ForegroundColor Cyan }
docker-compose logs --tail=3 llm-processor 2>$null | ForEach-Object { Write-Host " LLM: $_" -ForegroundColor Magenta }
docker-compose logs --tail=3 score-calculator 2>$null | ForEach-Object { Write-Host " SCORE: $_" -ForegroundColor Blue }

Write-Host "`nPresiona Ctrl+C para detener el monitoreo..." -ForegroundColor Gray
Start-Sleep -Seconds 5
}
```
- Tiempo Total transcurrido:
```bash
while ($true) {
$time = Get-Date -Format "HH:mm:ss"
$result = docker exec nuevacarpeta2-postgres-db-1 psql -U user -d yahoo_analysis -t -c "SELECT NOW() - MIN(created_at) AS tiempo_transcurrido FROM responses;" 2>$null
Write-Host "[$time] Tiempo transcurrido: $result"
Start-Sleep -Seconds 5
}

```
- Tabla de Hit/Miss
```bash
while (1) { docker exec nuevacarpeta2-postgres-db-1 psql -U user -d yahoo_analysis -c "SELECT COUNT(CASE WHEN access_count>1 THEN 1 END) as hits, COUNT(*)-COUNT(CASE WHEN access_count>1 THEN 1 END) as misses, ROUND(COUNT(CASE WHEN access_count>1 THEN 1 END)*100.0/COUNT(*),2) as hit_rate FROM responses;" 2>$null; Start-Sleep 5; Clear-Host }
```

# Cambios de politicas y distribuciones del sistema

El paso de LRU a FIFO se hace de forma manual en el dockerfile de REDIS:

```bash
RUN echo "maxmemory-policy allkeys-lru
RUN echo "maxmemory-policy allkeys-fifo
```

# Apagar el sistema
```bash
docker compose down
```
# Apagar el sistema y reinicio completo
```bash
docker compose down --volumes
```

