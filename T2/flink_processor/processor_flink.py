"""
Flink Validator - Sistema de Validación de Respuestas del LLM
Valida respuestas basándose en score de similitud y reintentos.
Guarda respuestas aprobadas en PostgreSQL.
"""

import json
import os
import time
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_INPUT = os.getenv('KAFKA_TOPIC_INPUT', 'respuestas-con-score')
KAFKA_TOPIC_REJECTED = os.getenv('KAFKA_TOPIC_REJECTED', 'consultas-procesadas')
SCORE_THRESHOLD = float(os.getenv('SCORE_THRESHOLD', '0.3'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))

# PostgreSQL
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'yahoo_analysis')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

# Estadísticas
stats = {
    'total_processed': 0,
    'approved': 0,
    'rejected': 0,
    'cache_hits': 0,
    'max_retries_reached': 0
}


def should_retry(data):
    """
    Decide si la respuesta debe reintentarse.
    IMPORTANTE: TODAS las respuestas se guardan en BD, esto solo decide reintentos.
    
    Returns:
        tuple: (should_retry: bool, reason: str, enriched_data: dict)
    """
    cache_hit = data.get('cache_hit', False)
    cosine_score = data.get('cosine_score')
    retry_count = data.get('retry_count', 0)
    
    # Agregar metadata de Flink
    data['flink_processed_at'] = time.time()
    data['flink_validator_version'] = '1.0'
    data['score_threshold_used'] = SCORE_THRESHOLD
    
    # Caso 1: Cache hit - no reintentar (ya fue procesado antes)
    if cache_hit:
        data['flink_decision'] = 'STORED_NO_RETRY'
        data['flink_reason'] = 'Cache hit - previamente validado'
        stats['cache_hits'] += 1
        return (False, 'cache_hit', data)
    
    # Caso 2: Score no disponible - reintentar
    if cosine_score is None:
        data['flink_decision'] = 'STORED_RETRY'
        data['flink_reason'] = 'Score no disponible - reintentando'
        data['retry_count'] = retry_count + 1
        return (True, 'no_score', data)
    
    # Caso 3: Score bueno (>= threshold) - no reintentar
    if cosine_score >= SCORE_THRESHOLD:
        data['flink_decision'] = 'STORED_NO_RETRY'
        data['flink_reason'] = f'Score {cosine_score:.4f} >= threshold {SCORE_THRESHOLD}'
        return (False, 'good_score', data)
    
    # Caso 4: Score bajo pero alcanzó max reintentos - no reintentar más
    if retry_count >= MAX_RETRIES:
        data['flink_decision'] = 'STORED_NO_RETRY'
        data['flink_reason'] = f'Max reintentos alcanzado ({MAX_RETRIES}), score final: {cosine_score:.4f}'
        stats['max_retries_reached'] += 1
        return (False, 'max_retries', data)
    
    # Caso 5: Score bajo y puede reintentar - reintentar
    data['flink_decision'] = 'STORED_RETRY'
    data['flink_reason'] = f'Score {cosine_score:.4f} < threshold {SCORE_THRESHOLD} - reintentando'
    data['retry_count'] = retry_count + 1
    return (True, 'low_score', data)


def wait_for_kafka():
    """Espera a que Kafka esté disponible"""
    max_retries = 30
    for i in range(max_retries):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            consumer.close()
            print("✅ Kafka conectado")
            return True
        except NoBrokersAvailable:
            print(f"⏳ Intento {i+1}/{max_retries}")
            time.sleep(2)
    
    return False


def main():
    print("=" * 70)
    print("🚀 FLINK VALIDATOR - TODAS las respuestas a PostgreSQL")
    print("=" * 70)
    print(f"📥 Input:  {KAFKA_TOPIC_INPUT}")
    print(f"💾 DB:     PostgreSQL ({POSTGRES_DB}) - GUARDA TODAS")
    print(f"🔄 Retry:  {KAFKA_TOPIC_REJECTED} (solo si score < {SCORE_THRESHOLD})")
    print(f"📊 Threshold: {SCORE_THRESHOLD} (para decidir reintentos)")
    print("=" * 70)
    
    if not wait_for_kafka():
        return
    
    print("🔌 Conectando a PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        print("✅ PostgreSQL conectado")
    except Exception as e:
        print(f"❌ Error PostgreSQL: {e}")
        return
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC_INPUT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='flink-validator-v2-all',  # Nuevo grupo para leer desde el inicio
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    producer_rejected = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    print("✅ Validator activo\n")
    
    try:
        for message in consumer:
            try:
                data = message.value
                stats['total_processed'] += 1
                
                # Decidir si reintentar (PERO SIEMPRE GUARDAR EN BD)
                needs_retry, reason, enriched_data = should_retry(data)
                
                # PASO 1: SIEMPRE guardar en PostgreSQL (todas las respuestas)
                try:
                    cursor.execute("""
                        INSERT INTO responses 
                        (question_title, question_content, best_answer, llm_answer, 
                         cosine_score, rouge_score, length_score, question_hash)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (question_hash) DO UPDATE SET
                            llm_answer = EXCLUDED.llm_answer,
                            cosine_score = EXCLUDED.cosine_score,
                            rouge_score = EXCLUDED.rouge_score,
                            length_score = EXCLUDED.length_score,
                            access_count = responses.access_count + 1
                    """, (
                        enriched_data.get('question_title', ''),
                        enriched_data.get('question_content', ''),
                        enriched_data.get('best_answer', ''),
                        enriched_data.get('llm_answer', ''),
                        enriched_data.get('cosine_score', 0.0),
                        enriched_data.get('rouge_score', 0.0),
                        enriched_data.get('length_score', 0.0),
                        enriched_data.get('question_hash', '')
                    ))
                    conn.commit()
                    stats['approved'] += 1
                    
                    score_str = f"{enriched_data.get('cosine_score', 0.0):.3f}" if enriched_data.get('cosine_score') is not None else 'N/A'
                    print(f"💾 Guardado (score: {score_str}, retry: {'Sí' if needs_retry else 'No'})")
                    
                except Exception as e:
                    print(f"❌ Error BD: {e}")
                    conn.rollback()
                
                # PASO 2: Si necesita reintento, enviar a topic de reintentos
                if needs_retry:
                    producer_rejected.send(KAFKA_TOPIC_REJECTED, enriched_data)
                    stats['rejected'] += 1
                    print(f"🔄 Reintento programado - Razón: {reason}")
                
                # Mostrar estadísticas cada 10 mensajes
                if stats['total_processed'] % 10 == 0:
                    print(f"\n📊 STATS: Total={stats['total_processed']} 💾={stats['approved']} 🔄={stats['rejected']} ⚡={stats['cache_hits']}\n")
                
            except Exception as e:
                print(f"❌ Error: {e}")
                continue
    
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo...")
    finally:
        consumer.close()
        producer_rejected.close()
        cursor.close()
        conn.close()
        print(f"📊 FINAL: Total={stats['total_processed']} 💾 Guardados={stats['approved']} 🔄 Reintentos={stats['rejected']}")


if __name__ == '__main__':
    main()
