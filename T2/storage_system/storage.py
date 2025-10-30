import json
import time
import sys
import os
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración desde variables de entorno
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'yahoo_analysis')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_INPUT = os.getenv('KAFKA_TOPIC_INPUT', 'consultas-entrantes')
KAFKA_TOPIC_OUTPUT = os.getenv('KAFKA_TOPIC_OUTPUT', 'consultas-procesadas')
KAFKA_TOPIC_CACHED = os.getenv('KAFKA_TOPIC_CACHED', 'respuestas-llm')  # Para respuestas cacheadas
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'storage-system-group')

def wait_for_postgres():
    """Espera a que PostgreSQL esté disponible"""
    max_retries = 30
    retry_interval = 3
    
    for attempt in range(max_retries):
        try:
            print(f" Intento {attempt + 1}/{max_retries}: Conectando a PostgreSQL...")
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            conn.close()
            print(f" PostgreSQL conectado en {POSTGRES_HOST}")
            return True
        except psycopg2.OperationalError:
            print(f" PostgreSQL no disponible, reintentando en {retry_interval}s...")
            time.sleep(retry_interval)
    
    return False

def wait_for_kafka():
    """Espera a que Kafka esté disponible"""
    max_retries = 30
    retry_interval = 5
    
    for attempt in range(max_retries):
        try:
            print(f" Intento {attempt + 1}/{max_retries}: Conectando a Kafka...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.close()
            print(f" Kafka conectado en {KAFKA_BOOTSTRAP_SERVERS}")
            return True
        except NoBrokersAvailable:
            print(f" Kafka no disponible, reintentando en {retry_interval}s...")
            time.sleep(retry_interval)
    
    return False

def get_db_connection():
    """Obtiene una conexión a PostgreSQL"""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def check_question_in_cache(question_hash):
    """
    Verifica si una pregunta ya fue procesada y está en la base de datos.
    Retorna (found, response_data) donde:
    - found: True si se encontró, False si no
    - response_data: diccionario con los datos si se encontró, None si no
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Buscar por hash de pregunta
        cur.execute("""
            SELECT 
                id,
                question_title,
                question_content,
                llm_answer,
                cosine_score,
                created_at
            FROM responses
            WHERE question_hash = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (question_hash,))
        
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result:
            return True, {
                'id': result[0],
                'question_title': result[1],
                'question_content': result[2],
                'llm_answer': result[3],
                'cosine_score': float(result[4]) if result[4] is not None else None,
                'created_at': result[5].isoformat() if result[5] else None,
                'cache_hit': True
            }
        else:
            return False, None
            
    except Exception as e:
        print(f" Error consultando caché: {e}")
        return False, None

def save_cached_response(message_data, cached_data):
    """
    Guarda en logs que se usó una respuesta cacheada.
    (Opcional: podrías guardar stats de cache hits)
    """
    print(f"   CACHE HIT - Respuesta encontrada en DB:")
    print(f"   ID en DB: {cached_data['id']}")
    print(f"   Fecha: {cached_data['created_at']}")
    print(f"   Score: {cached_data['cosine_score']}")
    

def main():
    print(" Iniciando Storage System (Verificación de Caché)...")
    print(f" Consumiendo de: {KAFKA_TOPIC_INPUT}")
    print(f" Produciendo a (MISS): {KAFKA_TOPIC_OUTPUT}")
    print(f" Produciendo a (HIT): {KAFKA_TOPIC_CACHED}")
    print(f"  PostgreSQL: {POSTGRES_HOST}/{POSTGRES_DB}")
    
    # Esperar servicios
    if not wait_for_postgres():
        print(" No se pudo conectar a PostgreSQL")
        sys.exit(1)
    
    if not wait_for_kafka():
        print(" No se pudo conectar a Kafka")
        sys.exit(1)
    
    # Crear consumer y producer
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_INPUT,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        print(f" Consumer creado - Topic: {KAFKA_TOPIC_INPUT}")
        
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        print(f" Producer creado - Topic: {KAFKA_TOPIC_OUTPUT}")
        
    except Exception as e:
        print(f" Error creando Kafka consumer/producer: {e}")
        sys.exit(1)
    
    print(" Storage System listo - Verificando caché...")
    
    # Contadores
    total_processed = 0
    cache_hits = 0
    cache_misses = 0
    
    # Loop principal
    try:
        for message in consumer:
            try:
                data = message.value
                total_processed += 1
                
                print(f" Mensaje #{total_processed} recibido [partition={message.partition}, offset={message.offset}]")
                print(f" Verificando caché: {data.get('question_title', '')[:60]}...")
                print(f" Hash: {data.get('question_hash', 'N/A')}")
                
                # Verificar si existe en caché (PostgreSQL)
                question_hash = data.get('question_hash')
                if not question_hash:
                    print("  Pregunta sin hash, enviando a procesamiento...")
                    producer.send(KAFKA_TOPIC_OUTPUT, data)
                    cache_misses += 1
                    continue
                
                found, cached_data = check_question_in_cache(question_hash)
                
                if found:
                    # CACHE HIT: La pregunta ya fue procesada
                    cache_hits += 1
                    print(f" CACHE HIT ({cache_hits}/{total_processed})")
                    save_cached_response(data, cached_data)
                    
                    # Enviar respuesta cacheada directamente al topic de respuestas
                    # Combinar datos originales con datos cacheados
                    cached_response = {
                        **data,  # Datos originales de la consulta
                        'llm_answer': cached_data['llm_answer'],
                        'cosine_score': cached_data['cosine_score'],
                        'cache_hit': True,
                        'cached_at': cached_data['created_at'],
                        'db_id': cached_data['id']
                    }
                    
                    print(f"   → Enviando respuesta cacheada a {KAFKA_TOPIC_CACHED}")
                    producer.send(KAFKA_TOPIC_CACHED, cached_response)
                    producer.flush()
                    print(f" Respuesta cacheada enviada (Score: {cached_data['cosine_score']})")
                    
                else:
                    # CACHE MISS: La pregunta es nueva
                    cache_misses += 1
                    print(f" CACHE MISS ({cache_misses}/{total_processed})")
                    print(f"   → Enviando a {KAFKA_TOPIC_OUTPUT} para procesamiento con LLM")
                    
                    # Enviar a siguiente topic para procesamiento
                    producer.send(KAFKA_TOPIC_OUTPUT, data)
                    producer.flush()
                    print(f" Mensaje enviado a procesamiento")
                
                # Mostrar estadísticas
                hit_rate = (cache_hits / total_processed * 100) if total_processed > 0 else 0
                print(f" Stats: Total={total_processed} | Hits={cache_hits} ({hit_rate:.1f}%) | Misses={cache_misses}")
                
            except Exception as e:
                print(f" Error procesando mensaje: {e}")
                continue
                
    except KeyboardInterrupt:
        print(" Cerrando Storage System...")
    finally:
        consumer.close()
        producer.close()
        print(f" Storage System cerrado")
        print(f" Resumen final:")
        print(f"   Total procesado: {total_processed}")
        print(f"   Cache Hits: {cache_hits}")
        print(f"   Cache Misses: {cache_misses}")
        if total_processed > 0:
            print(f"   Hit Rate: {cache_hits / total_processed * 100:.1f}%")

if __name__ == '__main__':
    main()
