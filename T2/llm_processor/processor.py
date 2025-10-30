import json
import redis
import requests
import time
import sys
import os
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración desde variables de entorno
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cache')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
OLLAMA_URL = os.getenv('OLLAMA_URL', "http://ollama:11434")
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_INPUT = os.getenv('KAFKA_TOPIC_INPUT', 'consultas-procesadas')
KAFKA_TOPIC_OUTPUT = os.getenv('KAFKA_TOPIC_OUTPUT', 'respuestas-llm')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'llm-processor-group')

def wait_for_redis():
    """Espera a que Redis esté disponible"""
    max_retries = 30
    for i in range(max_retries):
        try:
            r = redis.Redis(
                host=REDIS_HOST, 
                port=REDIS_PORT, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            r.ping()
            print(" Redis conectado exitosamente")
            return r
        except Exception as e:
            print(f" Intento {i+1}/{max_retries} - Redis no disponible: {e}")
            time.sleep(2)
    return None

def wait_for_kafka():
    """Espera a que Kafka esté disponible"""
    max_retries = 30
    for i in range(max_retries):
        try:
            # Intentar crear un consumer temporal
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                api_version=(0, 10, 1),
                request_timeout_ms=10000
            )
            consumer.close()
            print(" Kafka conectado exitosamente")
            return True
        except NoBrokersAvailable:
            print(f" Intento {i+1}/{max_retries} - Kafka no disponible")
            time.sleep(2)
        except Exception as e:
            print(f" Intento {i+1}/{max_retries} - Error conectando Kafka: {e}")
            time.sleep(2)
    return False

def wait_for_ollama():
    """Espera a que Ollama esté disponible y con modelos cargados"""
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(f"{OLLAMA_URL}/api/tags", timeout=30)
            if response.status_code == 200:
                models = response.json().get('models', [])
                if models:
                    print(" Ollama conectado y con modelos cargados")
                    return True
                else:
                    print(f" Ollama conectado pero sin modelos, esperando... ({i+1}/{max_retries})")
            else:
                print(f" Ollama respondió con código {response.status_code}, esperando... ({i+1}/{max_retries})")
        except Exception as e:
            print(f" Intento {i+1}/{max_retries} - Ollama no disponible: {e}")
        time.sleep(5)
    return False

class OllamaClient:
    def __init__(self, base_url=OLLAMA_URL):
        self.base_url = base_url
        
    def generate_response(self, question_title, question_content):
        prompt = f"Responde la siguiente pregunta de manera concisa: {question_title}. {question_content}"
        
        payload = {
            "model": "tinyllama:latest",  
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.7,
                "num_predict": 50  # Reducido de 100 → 50 tokens (respuestas más rápidas)
            }
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"[{attempt+1}/{max_retries}] Enviando pregunta a Ollama: {question_title[:50]}...")
                response = requests.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=30  # Reducido de 120 → 30 segundos (4x más rápido)
                )
                response.raise_for_status()
                result = response.json()["response"]
                print(f"✅ Respuesta recibida de Ollama ({len(result)} caracteres)")
                return result
            except requests.exceptions.Timeout:
                print(f"⏱️ Timeout en intento {attempt+1}")
                if attempt < max_retries - 1:
                    time.sleep(5)  # Reducido de 10 → 5 segundos
            except Exception as e:
                print(f"❌ Error en intento {attempt+1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)  # Reducido de 10 → 5 segundos
        
        return f"Error: No se pudo obtener respuesta después de {max_retries} intentos"

def main():
    print(" Iniciando LLM Processor (SIN cache_system)...")
    print(f" Consumiendo de: {KAFKA_TOPIC_INPUT}")
    print(f" Produciendo a: {KAFKA_TOPIC_OUTPUT}")
    
    # Esperar servicios
    r = wait_for_redis()
    if r is None:
        print(" No se pudo conectar a Redis")
        sys.exit(1)
    
    if not wait_for_kafka():
        print(" No se pudo conectar a Kafka")
        sys.exit(1)
    
    if not wait_for_ollama():
        print(" No se pudo conectar a Ollama o no tiene modelos cargados")
        sys.exit(1)
    
    # Crear consumer y producer de Kafka
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
    
    ollama = OllamaClient()
    print(" LLM Processor listo - Esperando consultas desde Kafka...")
    
    # Loop principal: consumir de Kafka, procesar con LLM, enviar a Kafka
    try:
        for message in consumer:
            try:
                data = message.value
                print(f"\n📨 Mensaje recibido [offset={message.offset}, partition={message.partition}]")
                print(f"🔍 Procesando: {data['question_title'][:60]}...")
                
                # Generar respuesta con LLM
                llm_answer = ollama.generate_response(
                    data['question_title'], 
                    data['question_content']
                )
                
                # Agregar respuesta al data
                data['llm_answer'] = llm_answer
                
                # Enviar a siguiente topic
                producer.send(KAFKA_TOPIC_OUTPUT, data)
                producer.flush()
                
                print(f" Respuesta generada y enviada a {KAFKA_TOPIC_OUTPUT}")
                
            except Exception as e:
                print(f" Error procesando mensaje: {e}")
                continue
                
    except KeyboardInterrupt:
        print(" Cerrando LLM Processor...")
    finally:
        consumer.close()
        producer.close()
        print(" LLM Processor cerrado correctamente")

if __name__ == '__main__':
    main()