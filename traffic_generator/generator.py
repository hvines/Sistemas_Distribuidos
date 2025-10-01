import time
import random
import pandas as pd
import redis
from scipy.stats import poisson
import json
import hashlib

REDIS_HOST = 'redis-cache'
REDIS_PORT = 6379
DATASET_PATH = '/app/datasets/train.csv'

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def load_dataset():
    try:
        df = pd.read_csv(DATASET_PATH, header=None, names=[
            'category', 
            'question_title', 
            'question_content', 
            'best_answer'
        ])
        print(f"Dataset cargado: {len(df)} preguntas")
        return df
    except Exception as e:
        print(f"Error cargando dataset: {e}")
        return pd.DataFrame()

def generate_hash(question_title, question_content):
    combined = question_title + question_content
    return hashlib.md5(combined.encode()).hexdigest()

def generate_poisson_traffic(df, rate=0.5, duration=60):
    questions = []
    t = 0
    while t < duration:
        interval = poisson.rvs(1/rate)
        t += interval
        if t < duration:
            row = df.sample(1).iloc[0]
            questions.append((t, row))
    return questions

def generate_uniform_traffic(df, rate=0.5, duration=60):
    questions = []
    interval = 1/rate
    t = 0
    while t < duration:
        row = df.sample(1).iloc[0]
        questions.append((t, row))
        t += interval
    return questions

def wait_for_redis():
    max_retries = 30
    for i in range(max_retries):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            print("Redis conectado exitosamente")
            return r
        except Exception as e:
            print(f"Intento {i+1}/{max_retries} - Redis no disponible: {e}")
            time.sleep(2)
    return None

def main():
    print("Iniciando Traffic Generator (modo continuo - SOLO UNIFORME)...")
    
    r = wait_for_redis()
    if r is None:
        print("No se pudo conectar a Redis")
        return
    
    df = load_dataset()
    if df.empty:
        print("No se pudo cargar el dataset")
        return
    
    cycle_count = 0
    while True:
        cycle_count += 1
        print(f"\nCiclo {cycle_count} - Generando tráfico UNIFORME...")
        
        print("=== Generando tráfico uniforme ===")
        
        events = generate_uniform_traffic(df, rate=0.5, duration=60)
        
        start_time = time.time()
        for event_time, row in events:
            elapsed = time.time() - start_time
            wait_time = event_time - elapsed
            if wait_time > 0:
                time.sleep(wait_time)
            
            message = {
                'question_title': str(row['question_title']),
                'question_content': str(row['question_content']),
                'original_answer': str(row['best_answer']),
                'question_hash': generate_hash(str(row['question_title']), str(row['question_content']))
            }
            
            r.lpush('questions_queue', json.dumps(message))
            print(f"[uniforme] Pregunta publicada: {message['question_title'][:50]}...")
        
        print(f"Ciclo {cycle_count} completado (tráfico uniforme). Esperando 10 segundos...")
        time.sleep(10)

if __name__ == '__main__':
    main()