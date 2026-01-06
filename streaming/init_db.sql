CREATE TABLE IF NOT EXISTS cleaned_reviews (
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(50) UNIQUE NOT NULL,
    business_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    
    text TEXT NOT NULL,
    rating DECIMAL(2,1) NOT NULL,
    date TIMESTAMPTZ NOT NULL,
    
    useful_count INTEGER DEFAULT 0,
    funny_count INTEGER DEFAULT 0,
    cool_count INTEGER DEFAULT 0,
    
    word_count INTEGER,
    char_count INTEGER,
    sentence_count INTEGER,
    avg_word_length DECIMAL(5,2),
    
    language VARCHAR(10),
    language_confidence DECIMAL(5,3),
    data_quality_score DECIMAL(5,3),
    is_spam BOOLEAN DEFAULT FALSE,
    is_anomaly BOOLEAN DEFAULT FALSE,
    
    source VARCHAR(50),
    ingestion_timestamp TIMESTAMPTZ,
    processing_timestamp TIMESTAMPTZ DEFAULT NOW(),
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_business_id ON cleaned_reviews(business_id);
CREATE INDEX idx_date ON cleaned_reviews(date DESC);
CREATE INDEX idx_quality_score ON cleaned_reviews(data_quality_score DESC);
CREATE INDEX idx_rating ON cleaned_reviews(rating);