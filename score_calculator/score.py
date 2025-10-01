import json
import redis
import psycopg2
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import time
import sys

REDIS_HOST = 'redis-cache'
REDIS_PORT = 6379
POSTGRES_HOST = 'postgres-db'
POSTGRES_DB = 'yahoo_analysis'
POSTGRES_USER = 'user'
POSTGRES_PASSWORD = 'password'

def wait_for_services():
    max_retries = 30
    
    for i in range(max_retries):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            print("Redis conectado exitosamente")
            break
        except Exception as e:
            print(f"Intento {i+1}/{max_retries} - Redis no disponible: {e}")
            time.sleep(2)
    else:
        print("No se pudo conectar a Redis")
        return None, None, None
    
    for i in range(max_retries):
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
            print(f"Intento {i+1}/{max_retries} - PostgreSQL no disponible: {e}")
            time.sleep(2)
    
    print("No se pudo conectar a PostgreSQL")
    return r, None, None

class QualityMetrics:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        
    def cosine_similarity_score(self, text1, text2):
        try:
            if not text1 or not text2:
                return 0.0
            tfidf_matrix = self.vectorizer.fit_transform([text1, text2])
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])
            return float(similarity[0][0])
        except Exception as e:
            print(f"Error en cosine similarity: {e}")
            return 0.0

def run_score_loop(r, conn, cur, metrics):
    while True:
        try:
            message = r.brpop('scoring_queue', timeout=30)
            
            if message is None:
                print("No hay mensajes en cola de scoring, esperando...")
                continue
                
            _, message_data = message
            data = json.loads(message_data)
            
            print(f"Calculando scores para: {data['question_title'][:50]}...")
            
            original_answer = data['original_answer']
            llm_answer = data['llm_answer']
            
            cosine_score = metrics.cosine_similarity_score(original_answer, llm_answer)
            
            print(f"Score calculado - Cosine: {cosine_score:.3f}")
            
            try:
                cur.execute(
                    """
                    INSERT INTO responses 
                    (question_title, question_content, original_answer, llm_answer, 
                     cosine_score, rouge_score, length_score, question_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (data['question_title'], data['question_content'], 
                     data['original_answer'], data['llm_answer'],
                     cosine_score, 0.0, 0.0, data['question_hash'])
                )
                conn.commit()
                print("Resultados guardados en BD")
                
            except Exception as e:
                print(f"Error guardando en BD: {e}")
                
        except Exception as e:
            print(f"Error en score calculator: {e}")
            time.sleep(1)

def main():
    print("Iniciando Score Calculator...")
    
    r, conn, cur = wait_for_services()
    if r is None or conn is None:
        print("No se pudieron conectar los servicios esenciales")
        sys.exit(1)
    
    metrics = QualityMetrics()
    print("Score Calculator listo - Esperando respuestas...")
    
    while True:
        try:
            run_score_loop(r, conn, cur, metrics)
        except Exception as e:
            print(f"Error crítico en score calculator: {e}. Reiniciando en 10 segundos...")
            time.sleep(10)

if __name__ == '__main__':
    main()