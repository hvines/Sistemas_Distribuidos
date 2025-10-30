import json
import redis
import psycopg2
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import time
import sys
import os
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración desde variables de entorno
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cache')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'yahoo_analysis')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_INPUT = os.getenv('KAFKA_TOPIC_INPUT', 'respuestas-llm')
KAFKA_TOPIC_OUTPUT = os.getenv('KAFKA_TOPIC_OUTPUT', 'respuestas-con-score')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'score-calculator-group')

def wait_for_services():
    """Espera a que Redis, PostgreSQL y Kafka estén disponibles"""
    max_retries = 30
    
    # Esperar Redis
    r = None
    for i in range(max_retries):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            print(" Redis conectado exitosamente")
            break
        except Exception as e:
            print(f" Intento {i+1}/{max_retries} - Redis no disponible: {e}")
            time.sleep(2)
    else:
        print(" No se pudo conectar a Redis")
        return None, None, None
    
    # Esperar PostgreSQL
    conn, cur = None, None
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cur = conn.cursor()
            print(" PostgreSQL conectado exitosamente")
            break
        except Exception as e:
            print(f" Intento {i+1}/{max_retries} - PostgreSQL no disponible: {e}")
            time.sleep(2)
    else:
        print(" No se pudo conectar a PostgreSQL")
        return r, None, None
    
    # Esperar Kafka
    for i in range(max_retries):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                api_version=(0, 10, 1),
                request_timeout_ms=10000
            )
            consumer.close()
            print(" Kafka conectado exitosamente")
            return r, conn, cur
        except NoBrokersAvailable:
            print(f" Intento {i+1}/{max_retries} - Kafka no disponible")
            time.sleep(2)
        except Exception as e:
            print(f" Intento {i+1}/{max_retries} - Error conectando Kafka: {e}")
            time.sleep(2)
    
    print(" No se pudo conectar a Kafka")
    return r, conn, cur

class QualityMetrics:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        
    def cosine_similarity_score(self, best_answer, llm_answer):
        """
        Calcula similitud coseno entre:
        - best_answer: Respuesta original del dataset (train.csv)
        - llm_answer: Respuesta generada por TinyLlama
        """
        try:
            if not best_answer or not llm_answer:
                print("  ⚠️  Una de las respuestas está vacía")
                return 0.0
            
            # Vectorizar ambas respuestas
            tfidf_matrix = self.vectorizer.fit_transform([best_answer, llm_answer])
            
            # Calcular similitud coseno
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])
            score = float(similarity[0][0])
            
            print(f"  📊 Comparación:")
            print(f"     Best Answer (dataset): {len(best_answer)} chars")
            print(f"     LLM Answer (generada): {len(llm_answer)} chars")
            print(f"     Similitud: {score:.4f}")
            
            return score
            
        except Exception as e:
            print(f"  ❌ Error en cosine similarity: {e}")
            return 0.0

def run_score_loop(conn, cur, metrics, consumer, producer):
    """Loop principal que consume de Kafka, calcula scores y reenvía"""
    print(f" Consumiendo mensajes de: {KAFKA_TOPIC_INPUT}")
    print(f" Produciendo mensajes a: {KAFKA_TOPIC_OUTPUT}")
    
    for message in consumer:
        try:
            data = message.value
            print(f" Mensaje recibido [offset={message.offset}, partition={message.partition}]")
            
            # Si es un cache hit, reenviar directamente con su score
            if data.get('cache_hit', False):
                print(f"  CACHE HIT detectado - reenviando con score: {data.get('cosine_score')}")
                print(f"   Pregunta: {data['question_title'][:60]}...")
                producer.send(KAFKA_TOPIC_OUTPUT, data)
                continue
            
            print(f" Calculando scores para: {data['question_title'][:60]}...")
            
            # Obtener respuestas del dataset y del LLM
            best_answer = data.get('best_answer', '') or data.get('original_answer', '')
            llm_answer = data.get('llm_answer', '')
            
            if not best_answer:
                print(f"  ⚠️  No hay best_answer en el mensaje - enviando con score 0.0")
                data['cosine_score'] = 0.0
                producer.send(KAFKA_TOPIC_OUTPUT, data)
                continue
            
            if not llm_answer:
                print(f"  ⚠️  No hay llm_answer en el mensaje - enviando con score 0.0")
                data['cosine_score'] = 0.0
                producer.send(KAFKA_TOPIC_OUTPUT, data)
                continue
            
            # Calcular score de similitud (best_answer del dataset vs llm_answer generada)
            cosine_score = metrics.cosine_similarity_score(best_answer, llm_answer)
            
            print(f" Score calculado - Cosine: {cosine_score:.3f}")
            
            # Agregar score al mensaje
            data['cosine_score'] = cosine_score
            data['rouge_score'] = 0.0
            data['length_score'] = 0.0
            
            # Enviar mensaje enriquecido a Flink para validación
            producer.send(KAFKA_TOPIC_OUTPUT, data)
            print(f" Mensaje con score enviado a {KAFKA_TOPIC_OUTPUT}")
                
        except Exception as e:
            print(f" Error procesando mensaje: {e}")
            continue

def main():
    print(" Iniciando Score Calculator - Calcula scores y envía a Flink...")
    print(f" Consumiendo de: {KAFKA_TOPIC_INPUT}")
    print(f" Produciendo a: {KAFKA_TOPIC_OUTPUT}")
    
    r, conn, cur = wait_for_services()
    if r is None or conn is None:
        print(" No se pudieron conectar los servicios esenciales")
        sys.exit(1)
    
    # Crear consumer de Kafka
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_INPUT,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=10
        )
        print(f" Consumer creado - Topic: {KAFKA_TOPIC_INPUT}")
        
        # Crear producer de Kafka
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
    
    metrics = QualityMetrics()
    print(" Score Calculator listo - Calculando scores y enviando a Flink...")
    
    # Loop principal
    try:
        run_score_loop(conn, cur, metrics, consumer, producer)
    except KeyboardInterrupt:
        print(" Cerrando Score Calculator...")
    finally:
        consumer.close()
        producer.close()
        cur.close()
        conn.close()
        print(" Score Calculator cerrado correctamente")

if __name__ == '__main__':
    main()