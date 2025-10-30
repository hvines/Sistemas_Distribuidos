#!/bin/bash
# Script para monitorear el sistema completo

echo "╔═══════════════════════════════════════════════════════════════════╗"
echo "║                     MONITOR DEL SISTEMA                           ║"
echo "╚═══════════════════════════════════════════════════════════════════╝"
echo ""


echo " ESTADO DE SERVICIOS:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(NAME|traffic|storage|llm|score|flink|kafka|postgres|redis|ollama|zookeeper)"
echo ""


echo " ESTADÍSTICAS DE CACHE:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker logs storage-system 2>&1 | grep "Stats:" | tail -1
echo ""


echo " ÚLTIMOS CACHE HITS DETECTADOS:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
CACHE_HITS=$(docker logs storage-system 2>&1 | grep "CACHE HIT" -A 3 | tail -20)
if [ -z "$CACHE_HITS" ]; then
    echo " No se han detectado cache hits aún"
    echo "   (Esperando preguntas duplicadas...)"
else
    echo "$CACHE_HITS"
fi
echo ""


echo "  BASE DE DATOS:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec -it postgres-db psql -U user -d yahoo_analysis -c \
  "SELECT COUNT(*) as total_preguntas FROM responses;" 2>/dev/null
echo ""


echo " TOPICS DE KAFKA:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null
echo ""


echo " ÚLTIMA ACTIVIDAD:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Traffic Generator:"
docker logs traffic-generator 2>&1 | grep "Consulta #" | tail -2