import json
import redis
import psycopg2
import time
from collections import OrderedDict
import sys

REDIS_HOST = 'redis-cache'
REDIS_PORT = 6379
POSTGRES_HOST = 'postgres-db'
POSTGRES_DB = 'yahoo_analysis'
POSTGRES_USER = 'user'
POSTGRES_PASSWORD = 'password'

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

def wait_for_services():
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            print("Redis conectado exitosamente")
            break
        except Exception as e:
            retry_count += 1
            print(f"Intento {retry_count}/{max_retries} - Redis no disponible: {e}")
            time.sleep(2)
    else:
        print("No se pudo conectar a Redis después de múltiples intentos")
        return None, None, None
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cur = conn.cursor()
            print("PostgreSQL conectado exitosamente")
            return r, conn, cur
        except Exception as e:
            retry_count += 1
            print(f"Intento {retry_count}/{max_retries} - PostgreSQL no disponible: {e}")
            time.sleep(2)
    
    print("No se pudo conectar a PostgreSQL después de múltiples intentos")
    return r, None, None

def run_cache_loop(r, conn, cur, cache):
    while True:
        try:
            message = r.brpop(['questions_queue', 'llm_responses'], timeout=30)
            
            if message is None:
                print("No hay mensajes en cola, esperando...")
                continue
                
            queue_name, message_data = message
            data = json.loads(message_data)
            
            if queue_name == 'questions_queue':
                question_hash = data['question_hash']
                print(f"Procesando pregunta: {data['question_title'][:50]}...")
                
                cached_response = cache.get(question_hash)
                if cached_response:
                    print(f"HIT en caché")
                    try:
                        cur.execute(
                            "UPDATE responses SET access_count = access_count + 1 WHERE question_hash = %s",
                            (question_hash,)
                        )
                        conn.commit()
                        r.lpush('final_responses', json.dumps(cached_response))
                    except Exception as e:
                        print(f"Error actualizando contador: {e}")
                else:
                    print(f"MISS en caché - Enviando a LLM Processor")
                    r.lpush('uncached_questions', json.dumps(data))
            
            elif queue_name == 'llm_responses':
                question_hash = data['question_hash']
                print(f"Guardando respuesta en caché FIFO: {question_hash[:8]}...")
                
                cache.put(question_hash, data)
                
                try:
                    cur.execute(
                        "INSERT INTO responses (question_hash, llm_response, created_at, access_count) VALUES (%s, %s, NOW(), 1) ON CONFLICT (question_hash) DO UPDATE SET access_count = responses.access_count + 1",
                        (question_hash, data.get('llm_response', ''))
                    )
                    conn.commit()
                except Exception as e:
                    print(f"Error guardando en DB: {e}")
                
                r.lpush('final_responses', json.dumps(data))
                
        except Exception as e:
            print(f"Error en cache system: {e}")
            time.sleep(1)

def main():
    print("Iniciando Cache System...")
    
    r, conn, cur = wait_for_services()
    if r is None or conn is None:
        print("No se pudieron conectar los servicios esenciales")
        sys.exit(1)
    
    cache = FIFOCache(capacity=500)
    
    print("Cache System listo - Esperando preguntas...")
    
    while True:
        try:
            run_cache_loop(r, conn, cur, cache)
        except Exception as e:
            print(f"Error crítico en cache system: {e}. Reiniciando en 10 segundos...")
            time.sleep(10)

if __name__ == '__main__':
    main()