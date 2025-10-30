CREATE TABLE IF NOT EXISTS responses (
    id SERIAL PRIMARY KEY,
    question_title TEXT NOT NULL,
    question_content TEXT NOT NULL,
    best_answer TEXT NOT NULL,            -- Respuesta original del dataset (train.csv)
    llm_answer TEXT NOT NULL,             -- Respuesta generada por TinyLlama
    cosine_score FLOAT NOT NULL,          -- Similitud entre best_answer y llm_answer
    rouge_score FLOAT NOT NULL,
    length_score FLOAT NOT NULL,
    question_hash VARCHAR(32) UNIQUE NOT NULL,
    access_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_question_hash ON responses(question_hash);
CREATE INDEX IF NOT EXISTS idx_scores ON responses(cosine_score, rouge_score);