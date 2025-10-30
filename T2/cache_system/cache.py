import json
import redis
import psycopg2
import time
import os
import sys
from collections import OrderedDict
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError



# Redis (caché)
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cache')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# PostgreSQL
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'yahoo_analysis')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_INPUT = os.getenv('KAFKA_TOPIC_INPUT', 'consultas-procesadas')
KAFKA_TOPIC_OUTPUT_MISS = os.getenv('KAFKA_TOPIC_OUTPUT_MISS', 'consultas-sin-cache')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'cache-system-group')


class FIFOCache:
    def __init__(self, capacity: int = 1000):
        self.cache = OrderedDict()
        self.capacity = capacity
        
    def get(self, key):
        if key not in self.cache:
            return None
        return self.cache[key]
    
    def put(self, key, value):
        if key in self.cache:
            self.cache[key] = value
        else:
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)



def wait_for_redis():
    """Espera a que Redis esté disponible"""
    max_retries = 30
    retry_count = 0
    
    print(" Esperando Redis...")
    while retry_count < max_retries:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            print(" Redis conectado exitosamente")
            return r
        except Exception as e:
            retry_count += 1
            print(f" Intento {retry_count}/{max_retries} - Redis no disponible: {e}")
            time.sleep(2)
    
    print(" No se pudo conectar a Redis después de múltiples intentos")
    return None

def wait_for_postgres():
    """Espera a que PostgreSQL esté disponible"""
    max_retries = 30
    retry_count = 0
    
    print(" Esperando PostgreSQL...")
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cur = conn.cursor()
            print(" PostgreSQL conectado exitosamente")
            return conn, cur
        except Exception as e:
            retry_count += 1
            print(f" Intento {retry_count}/{max_retries} - PostgreSQL no disponible: {e}")
            time.sleep(2)
    
    print(" No se pudo conectar a PostgreSQL después de múltiples intentos")
    return None, None

def create_kafka_consumer():
    """Crea un consumidor de Kafka con reintentos"""
    max_retries = 30
    retry_count = 0
    
    print(" Conectando a Kafka Consumer...")
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_INPUT,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Leer desde el principio si no hay offset
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                session_timeout_ms=30000,
                max_poll_interval_ms=300000
            )
            print(f" Kafka Consumer conectado al topic: {KAFKA_TOPIC_INPUT}")
            return consumer
        except Exception as e:
            retry_count += 1
            print(f" Intento {retry_count}/{max_retries} - Kafka Consumer no disponible: {e}")
            time.sleep(2)
    
    print(" No se pudo conectar a Kafka Consumer después de múltiples intentos")
    return None

def create_kafka_producer():
    """Crea un productor de Kafka con reintentos"""
    max_retries = 30
    retry_count = 0
    
    print(" Conectando a Kafka Producer...")
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Esperar confirmación de todos los brokers
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            print(f" Kafka Producer conectado")
            return producer
        except Exception as e:
            retry_count += 1
            print(f" Intento {retry_count}/{max_retries} - Kafka Producer no disponible: {e}")
            time.sleep(2)
    
    print(" No se pudo conectar a Kafka Producer después de múltiples intentos")
    return None




def process_message(data, redis_client, cache, conn, cur, kafka_producer):
    """
    Procesa un mensaje de consulta:
    1. Busca en Redis cache
    2. Si HIT: actualiza contador en DB
    3. Si MISS: envía a Kafka para procesamiento LLM
    """
    question_hash = data.get('hash') or data.get('question_hash')
    question_title = data.get('pregunta') or data.get('question_title', '')
    
    if not question_hash:
        print("  Mensaje sin hash, generando uno...")
        import hashlib
        question_hash = hashlib.md5(question_title.encode()).hexdigest()
        data['hash'] = question_hash
    
    print(f"📨 Procesando pregunta: {question_title[:50]}...")
    
    # Buscar en caché Redis
    cached_response = cache.get(question_hash)
    
    if cached_response:
    
        print(f" HIT en caché para hash: {question_hash[:8]}")
        
        try:
            # Actualizar contador de accesos en PostgreSQL
            cur.execute("""
                UPDATE responses 
                SET access_count = access_count + 1, 
                    last_accessed_at = NOW() 
                WHERE question_hash = %s
            """, (question_hash,))
            conn.commit()
            print(f"    Contador de accesos actualizado")
            
        except Exception as e:
            print(f"     Error actualizando contador: {e}")
            conn.rollback()
    
    else:
        
        print(f" MISS en caché para hash: {question_hash[:8]}")
        print(f"     Enviando a topic: {KAFKA_TOPIC_OUTPUT_MISS}")
        
        try:
            # Enviar mensaje a Kafka
            future = kafka_producer.send(KAFKA_TOPIC_OUTPUT_MISS, data)
            
            # Esperar confirmación 
            record_metadata = future.get(timeout=10)
            
            print(f"      Mensaje enviado a Kafka")
            print(f"      Topic: {record_metadata.topic}")
            print(f"      Partition: {record_metadata.partition}")
            print(f"      Offset: {record_metadata.offset}")
            
        except KafkaError as e:
            print(f"    Error enviando a Kafka: {e}")
        except Exception as e:
            print(f"    Error inesperado: {e}")

def run_cache_loop(consumer, redis_client, cache, conn, cur, producer):
    """
    Loop principal que consume mensajes de Kafka y los procesa
    """
    print(f" Cache System listo!")
    print(f" Consumiendo de: {KAFKA_TOPIC_INPUT}")
    print(f" Produciendo a: {KAFKA_TOPIC_OUTPUT_MISS}")
    print(f" Capacidad de caché: {cache.capacity}")
    print(f"=" * 60)
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            try:
                data = message.value
                
                print(f"\n[Mensaje #{message_count}] Recibido de Kafka")
                print(f"  Topic: {message.topic}")
                print(f"  Partition: {message.partition}")
                print(f"  Offset: {message.offset}")
                
                # Procesar el mensaje
                process_message(data, redis_client, cache, conn, cur, producer)
                
            except Exception as e:
                print(f" Error procesando mensaje: {e}")
                print(f"   Mensaje: {message.value}")
                # Continuar con el siguiente mensaje
                continue
    
    except KeyboardInterrupt:
        print("  Deteniendo Cache System...")
    except Exception as e:
        print(f" Error crítico en el loop: {e}")
        raise




def main():
    print("\n" + "=" * 60)
    print(" INICIANDO CACHE SYSTEM CON KAFKA")
    print("=" * 60)
    
    # 1. Conectar a Redis (solo para caché)
    print("\n[1/5] Conectando a Redis...")
    redis_client = wait_for_redis()
    if redis_client is None:
        print(" No se pudo conectar a Redis. Abortando.")
        sys.exit(1)
    
    # 2. Conectar a PostgreSQL
    print("\n[2/5] Conectando a PostgreSQL...")
    conn, cur = wait_for_postgres()
    if conn is None or cur is None:
        print(" No se pudo conectar a PostgreSQL. Abortando.")
        sys.exit(1)
    
    # 3. Crear Kafka Consumer
    print("\n[3/5] Creando Kafka Consumer...")
    consumer = create_kafka_consumer()
    if consumer is None:
        print(" No se pudo crear Kafka Consumer. Abortando.")
        sys.exit(1)
    
    # 4. Crear Kafka Producer
    print("\n[4/5] Creando Kafka Producer...")
    producer = create_kafka_producer()
    if producer is None:
        print(" No se pudo crear Kafka Producer. Abortando.")
        consumer.close()
        sys.exit(1)
    
    # 5. Inicializar caché FIFO
    print("\n[5/5] Inicializando caché FIFO...")
    cache = FIFOCache(capacity=500)
    print(f" Caché FIFO creado con capacidad: {cache.capacity}")
    
    # Iniciar el loop principal
    try:
        run_cache_loop(consumer, redis_client, cache, conn, cur, producer)
    
    except KeyboardInterrupt:
        print("\n  Recibida señal de interrupción. Cerrando servicios...")
    
    except Exception as e:
        print(f"\n Error crítico: {e}")
        print("Reiniciando en 10 segundos...")
        time.sleep(10)
    
    finally:
        # Cerrar conexiones
        print(" Cerrando conexiones...")
        try:
            if consumer:
                consumer.close()
                print("    Kafka Consumer cerrado")
        except:
            pass
        
        try:
            if producer:
                producer.flush()
                producer.close()
                print("    Kafka Producer cerrado")
        except:
            pass
        
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
                print("    PostgreSQL cerrado")
        except:
            pass
        
        print("\n" + "=" * 60)
        print("Cache System detenido")
        print("=" * 60)

if __name__ == '__main__':
    # Reiniciar automáticamente en caso de error
    while True:
        try:
            main()
        except Exception as e:
            print(f" Error fatal: {e}")
            print(" Reiniciando en 10 segundos...")
            time.sleep(10)