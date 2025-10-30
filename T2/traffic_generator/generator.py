import csv
import json
import time
import os
import sys
import random
import hashlib
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración desde variables de entorno
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'consultas-entrantes')
CSV_PATH = os.getenv('CSV_PATH', '/app/datasets/train.csv')
SEND_INTERVAL = float(os.getenv('SEND_INTERVAL', 1.0))  # 1 segundo entre envíos (5x más rápido)
MAX_MESSAGES = int(os.getenv('MAX_MESSAGES', 0))  # 0 = infinito

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

def load_questions(csv_path):
    """Carga las preguntas desde el CSV"""
    questions = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, fieldnames=['id', 'question_title', 'question_content', 'answer'])
            next(reader)  # Skip header
            for row in reader:
                questions.append({
                    'id': row['id'],
                    'question_title': row['question_title'],
                    'question_content': row['question_content'],
                    'answer': row['answer']
                })
        print(f" Cargadas {len(questions)} preguntas desde {csv_path}")
        return questions
    except Exception as e:
        print(f" Error leyendo CSV: {e}")
        sys.exit(1)

def generate_question_hash(question_title, question_content):
    """Genera un hash único para la pregunta"""
    combined = f"{question_title}||{question_content}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()

def main():
    print(" Iniciando Traffic Generator...")
    print(f" Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f" Topic: {KAFKA_TOPIC}")
    print(f" CSV: {CSV_PATH}")
    print(f" Intervalo: {SEND_INTERVAL}s")
    print(f" Máximo: {'Infinito' if MAX_MESSAGES == 0 else MAX_MESSAGES}")
    
    # Esperar a Kafka
    if not wait_for_kafka():
        print(" No se pudo conectar a Kafka después de varios intentos")
        sys.exit(1)
    
    # Cargar preguntas
    questions = load_questions(CSV_PATH)
    if not questions:
        print(" No hay preguntas para enviar")
        sys.exit(1)
    
    # Crear producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        print(f" Producer creado correctamente")
    except Exception as e:
        print(f" Error creando producer: {e}")
        sys.exit(1)
    
    # Loop principal
    sent_count = 0
    print(f" Iniciando envío de consultas...")
    
    try:
        while True:
            # Seleccionar pregunta aleatoria
            question = random.choice(questions)
            
            # Generar hash
            question_hash = generate_question_hash(
                question['question_title'],
                question['question_content']
            )
            
            # Preparar mensaje
            message = {
                'question_id': question['id'],
                'question_title': question['question_title'],
                'question_content': question['question_content'],
                'best_answer': question['answer'],  # Respuesta original del dataset
                'question_hash': question_hash,
                'timestamp': time.time()
            }
            
            # Enviar a Kafka
            future = producer.send(KAFKA_TOPIC, message)
            metadata = future.get(timeout=10)
            
            sent_count += 1
            print(f" Consulta #{sent_count} enviada:")
            print(f"   ID: {question['id']}")
            print(f"   Título: {question['question_title'][:60]}...")
            print(f"   Hash: {question_hash}")
            print(f"   Partition: {metadata.partition}, Offset: {metadata.offset}")
            
            # Verificar si llegamos al máximo
            if MAX_MESSAGES > 0 and sent_count >= MAX_MESSAGES:
                print(f" Alcanzado límite de {MAX_MESSAGES} mensajes")
                break
            
            # Esperar antes del siguiente envío
            time.sleep(SEND_INTERVAL)
            
    except KeyboardInterrupt:
        print(" Deteniendo Traffic Generator...")
    except Exception as e:
        print(f" Error en loop principal: {e}")
    finally:
        producer.flush()
        producer.close()
        print(f"Traffic Generator cerrado. Total enviado: {sent_count}")

if __name__ == '__main__':
    main()
