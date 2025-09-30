import json
import redis
import requests
import time
import sys
import os

# Configuración - Ahora Ollama está en un contenedor en la misma red
REDIS_HOST = 'redis-cache'
REDIS_PORT = 6379
OLLAMA_URL = "http://ollama:11434"  # ← Cambiado a nombre del servicio Docker

def wait_for_redis():
    """Esperar a que Redis esté disponible"""
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
            print("✅ Redis conectado exitosamente")
            return r
        except Exception as e:
            print(f"🔄 Intento {i+1}/{max_retries} - Redis no disponible: {e}")
            time.sleep(2)
    return None

def wait_for_ollama():
    """Esperar a que Ollama esté disponible"""
    max_retries = 30  # Dar más tiempo para que Ollama descargue el modelo
    for i in range(max_retries):
        try:
            response = requests.get(f"{OLLAMA_URL}/api/tags", timeout=30)
            if response.status_code == 200:
                models = response.json().get('models', [])
                if models:
                    print("✅ Ollama conectado y con modelos cargados")
                    return True
                else:
                    print(f"🔄 Ollama conectado pero sin modelos, esperando... ({i+1}/{max_retries})")
            else:
                print(f"🔄 Ollama respondió con código {response.status_code}, esperando... ({i+1}/{max_retries})")
        except Exception as e:
            print(f"🔄 Intento {i+1}/{max_retries} - Ollama no disponible: {e}")
        time.sleep(5)
    return False

class OllamaClient:
    def __init__(self, base_url=OLLAMA_URL):
        self.base_url = base_url
        
    def generate_response(self, question_title, question_content):
        """Obtener respuesta del modelo Ollama con reintentos"""
        prompt = f"Responde la siguiente pregunta de manera concisa: {question_title}. {question_content}"
        
        payload = {
            "model": "llama2:latest",
            "prompt": prompt,
            "stream": False
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"🤖 [{attempt+1}/{max_retries}] Enviando pregunta a Ollama: {question_title[:50]}...")
                response = requests.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=120
                )
                response.raise_for_status()
                result = response.json()["response"]
                print(f"✅ Respuesta recibida de Ollama ({len(result)} caracteres)")
                return result
            except requests.exceptions.Timeout:
                print(f"⏰ Timeout en intento {attempt+1}")
                if attempt < max_retries - 1:
                    time.sleep(10)  # Esperar más entre intentos
            except Exception as e:
                print(f"❌ Error en intento {attempt+1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)
        
        return f"Error: No se pudo obtener respuesta después de {max_retries} intentos"

def main():
    print("🚀 Iniciando LLM Processor...")
    
    # Esperar a Redis
    r = wait_for_redis()
    if r is None:
        print("❌ No se pudo conectar a Redis")
        sys.exit(1)
    
    # Esperar a Ollama (dar más tiempo la primera vez)
    if not wait_for_ollama():
        print("❌ No se pudo conectar a Ollama o no tiene modelos cargados")
        sys.exit(1)
    
    ollama = OllamaClient()
    print("🎯 LLM Processor listo - Esperando preguntas...")
    
    while True:
        try:
            # Esperar por una pregunta no cacheada
            message = r.brpop('uncached_questions', timeout=30)
            
            if message is None:
                print("⏳ No hay preguntas no cacheadas, esperando...")
                continue
                
            _, message_data = message
            data = json.loads(message_data)
            
            print(f"📥 Procesando con LLM: {data['question_title'][:50]}...")
            
            # Obtener respuesta del LLM
            llm_answer = ollama.generate_response(
                data['question_title'], 
                data['question_content']
            )
            
            data['llm_answer'] = llm_answer
            r.lpush('scoring_queue', json.dumps(data))
            print(f"✅ Respuesta generada - Enviando a Score Calculator")
            
        except redis.exceptions.ConnectionError as e:
            print(f"❌ Error de conexión Redis: {e}. Reintentando...")
            time.sleep(5)
        except Exception as e:
            print(f"❌ Error en LLM processor: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()