import psycopg2

def get_db_connection():
    """Obtener conexión a la base de datos"""
    return psycopg2.connect(
        host='postgres-db',
        database='yahoo_analysis',
        user='user',
        password='password'
    )

def get_stats():
    """Obtener estadísticas de la base de datos"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM responses")
    total = cur.fetchone()[0]
    
    cur.execute("SELECT AVG(cosine_score), AVG(rouge_score), AVG(length_score) FROM responses")
    avg_scores = cur.fetchone()
    
    print(f"Total respuestas: {total}")
    print(f"Puntuaciones promedio - Cosine: {avg_scores[0]:.3f}, "
          f"ROUGE: {avg_scores[1]:.3f}, Length: {avg_scores[2]:.3f}")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    get_stats()