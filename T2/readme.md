# Sistema Distribuido de Procesamiento de Preguntas con LLM

##  Descripción

Sistema distribuido que implementa un flujo completo de procesamiento de preguntas con LLM, incluyendo:
-  **Generador de tráfico** que lee del dataset CSV (Yahoo Answers)
-  **Sistema de almacenamiento con caché** que verifica en PostgreSQL
-  **Apache Kafka** para mensajería asíncrona y desacoplamiento
-  **TinyLlama** (Ollama) para generación de respuestas
-  **Apache Flink** para validación de calidad con threshold y retry logic
-  **Score Calculator** para cálculo de similitud coseno
-  **PostgreSQL** para almacenamiento persistente

---

##  Cómo Ejecutar

### Paso 1: Inicializar Ollama (si no existe)

```bash
# En Mac/Linux:
./init-ollama.sh

# En Windows (PowerShell):
.\init-ollama.ps1
```

**Nota:** Ollama se descargará automáticamente al iniciar, pero puede tardar 2-3 minutos.

### Paso 2: Levantar todos los servicios

```bash
docker-compose up -d
```

Esto iniciará:
-  Zookeeper
-  Kafka (con 4 topics: consultas-entrantes, consultas-procesadas, respuestas-llm, respuestas-validadas)
-  PostgreSQL (con tabla responses usando question_hash para caché)
-  Ollama + TinyLlama (1.1B parámetros, 637MB)
-  Flink JobManager y TaskManager
-  Traffic Generator
-  Storage System (con verificación de caché)
-  LLM Processor
-  Flink Processor (validador de calidad con threshold 0.3)
-  Score Calculator (calcula similitud entre best_answer y llm_answer)

### Paso 3: Verificar que los servicios están corriendo

```bash
# Ver todos los servicios
docker-compose ps

# Ver logs de un servicio específico
docker logs -f traffic-generator
docker logs -f storage-system
docker logs -f llm-processor
docker logs -f score-calculator
```


### Paso 4: Monitorear el sistema

**Usar el script de monitoreo automático**

```bash
# Dar permisos de ejecución
chmod +x monitor.sh

# Ejecutar el script de monitoreo
./monitor.sh
```

El script `monitor.sh` te mostrará:
-  Estado de todos los servicios
-  Estadísticas de cache (hits/misses)
-  Últimos cache hits detectados
-  Total de preguntas en base de datos
-  Topics de Kafka disponibles
-  Última actividad del sistema


### Paso 5: Consultar la base de datos

```bash
# Conectar a PostgreSQL
docker exec -it postgres-db psql -U user -d yahoo_analysis

# Ver todas las respuestas
SELECT id, question_title, cosine_score, question_hash, created_at 
FROM responses 
ORDER BY created_at DESC 
LIMIT 10;

# Ver estadísticas de caché
SELECT 
    COUNT(*) as total_preguntas,
    COUNT(DISTINCT question_hash) as preguntas_unicas,
    AVG(cosine_score) as score_promedio
FROM responses;
```

---

## Verificar Cache Hit Rate

El **Storage System** muestra estadísticas en tiempo real:

```bash
docker logs -f storage-system
```

---


##  Resumen de Comandos Útiles de Monitoreo

```bash

# Monitoreo rápido del sistema
./monitor.sh

# Ver estado de servicios
docker-compose ps

# Ver logs de un servicio específico
docker logs -f <service-name>

# Verificar topics de Kafka
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consultar base de datos
docker exec -it postgres-db psql -U user -d yahoo_analysis
```

---




##  Arquitectura y Pipeline Actual

```
┌─────────────────────────────────────────────────────────────────┐
│  1. Traffic Generator                                           │
│     - Lee preguntas del dataset (datasets/train.csv)           │
│     - Incluye: question_title, question_content, best_answer   │
│     - Genera hash MD5 único por pregunta                       │
│     - Envía a Kafka cada 5 segundos (configurable)             │
└──────────────┬──────────────────────────────────────────────────┘
               │ produce → consultas-entrantes
               ↓
┌─────────────────────────────────────────────────────────────────┐
│  2. Kafka Topic: consultas-entrantes                            │
│     - 3 partitions, replication factor 1                        │
└──────────────┬──────────────────────────────────────────────────┘
               │ consume (group: storage-system-group)
               ↓
┌─────────────────────────────────────────────────────────────────┐
│  3. Storage System (Verificación de Caché)                      │
│     - Busca question_hash en PostgreSQL                         │
│     - CACHE HIT: Obtiene respuesta + score de BD               │
│       → Envía a respuestas-llm con flag cache_hit=true         │
│     - CACHE MISS: Envía a consultas-procesadas                 │
└──────────────┬──────────────────────────────────────────────────┘
               │ produce (MISS → consultas-procesadas)
               │         (HIT → respuestas-llm)
               ↓
┌─────────────────────────────────────────────────────────────────┐
│  4. Kafka Topic: consultas-procesadas                           │
│     - 3 partitions, replication factor 1                        │
│     - Solo contiene preguntas NUEVAS (sin caché)                │
└──────────────┬──────────────────────────────────────────────────┘
               │ consume (group: llm-processor-group)
               ↓
┌─────────────────────────────────────────────────────────────────┐
│  5. LLM Processor                                               │
│     - Consume mensajes de Kafka                                 │
│     - Genera respuesta usando TinyLlama (Ollama)                │
│     - Incluye best_answer del dataset + llm_answer generada     │
│     - Envía resultado a siguiente topic                         │
└──────────────┬──────────────────────────────────────────────────┘
               │ produce → respuestas-llm
               ↓
┌─────────────────────────────────────────────────────────────────┐
│  6. Kafka Topic: respuestas-llm                                 │
│     - 3 partitions, replication factor 1                        │
│     - Contiene respuestas del LLM + respuestas cacheadas        │
└──────────────┬──────────────────────────────────────────────────┘
               │ consume (group: flink-validator-group)
               ↓
┌─────────────────────────────────────────────────────────────────┐
│  7. Apache Flink - Validador de Calidad                         │
│     - Consume respuestas del LLM                                │
│     - Valida score contra THRESHOLD (default: 0.3 = 30%)        │
│     - DECISIÓN 1: Score >= 0.3 → Aprueba                        │
│       → Envía a respuestas-validadas                            │
│     - DECISIÓN 2: Score < 0.3 y retry_count < 3 → Rechaza       │
│       → Envía a consultas-procesadas (reprocesar)               │
│     - DECISIÓN 3: retry_count >= 3 → Aprueba                    │
│       → Evita loops infinitos                                   │
│     - Cache hits siempre aprobados (ya validados antes)         │
└──────────────┬──────────────────────────────────────────────────┘
               │ produce → respuestas-validadas (aprobadas)
               │           consultas-procesadas (rechazadas)
               ↓
┌─────────────────────────────────────────────────────────────────┐
│  8. Kafka Topic: respuestas-validadas                           │
│     - 3 partitions, replication factor 1                        │
│     - Solo respuestas que pasaron validación de Flink           │
└──────────────┬──────────────────────────────────────────────────┘
               │ consume (group: score-calculator-group)
               ↓
┌─────────────────────────────────────────────────────────────────┐
│  9. Score Calculator                                            │
│     - Calcula cosine_similarity entre:                          │
│       * best_answer (respuesta original del dataset)            │
│       * llm_answer (respuesta generada por TinyLlama)           │
│     - Score: 0.0 (0%) a 1.0 (100%) de similitud                 │
│     - Guarda en PostgreSQL: pregunta + respuestas + score       │
└──────────────┬──────────────────────────────────────────────────┘
               ↓
┌─────────────────────────────────────────────────────────────────┐
│  10. PostgreSQL                                                 │
│     - Tabla: responses                                          │
│     - question_hash (UNIQUE) ← Clave para detección de caché    │
│     - best_answer: Respuesta original del dataset               │
│     - llm_answer: Respuesta generada por TinyLlama              │
│     - cosine_score: Similitud entre ambas respuestas            │
│     - Índice en question_hash para búsquedas rápidas            │
└─────────────────────────────────────────────────────────────────┘
```

---
