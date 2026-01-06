import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from typing import List, Dict
import time


class PostgresSink:
    
    def __init__(
        self,
        kafka_bootstrap_servers: str = 'localhost:9092',
        kafka_topic: str = 'cleaned_reviews',
        kafka_group_id: str = 'postgres-sink',
        postgres_config: dict = None,
        batch_size: int = 100,
        batch_timeout_seconds: int = 10,
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.kafka_group_id = kafka_group_id
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        
        self.pg_config = postgres_config or {
            'host': 'localhost',
            'port': 5432,
            'database': 'yelp_reviews_db',
            'user': 'yelp_user',
            'password': 'yelp_pass',
        }
        
        self.consumer = None
        self.pg_conn = None
        self.pg_cursor = None
        
    def connect(self):
        """Establish Kafka and PostgreSQL connections"""
        def safe_deserializer(m):
            if not m:
                return None
            try:
                return json.loads(m.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"Skipping malformed message: {e}")
                return None

        # Kafka consumer
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=self.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            value_deserializer=safe_deserializer,
        )
        
        # Wait for PostgreSQL to be ready
        max_retries = 30
        for i in range(max_retries):
            try:
                self.pg_conn = psycopg2.connect(**self.pg_config)
                self.pg_cursor = self.pg_conn.cursor()
                print(f"Connected to PostgreSQL")
                break
            except psycopg2.OperationalError as e:
                if i < max_retries - 1:
                    print(f"Waiting for PostgreSQL... ({i+1}/{max_retries})")
                    time.sleep(2)
                else:
                    raise
        
        print(f"Connected to Kafka topic: {self.kafka_topic}")
        
    def close(self):
        if self.consumer:
            self.consumer.close()
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()
            
    def insert_batch(self, records: List[Dict]):
        if not records:
            return
            
        query = """
            INSERT INTO cleaned_reviews (
                review_id, business_id, user_id, text, rating, date,
                useful_count, funny_count, cool_count,
                word_count, char_count, sentence_count, avg_word_length,
                language, language_confidence, data_quality_score,
                is_spam, is_anomaly, source,
                ingestion_timestamp, processing_timestamp
            ) VALUES (
                %(review_id)s, %(business_id)s, %(user_id)s, %(text)s, %(rating)s, %(date)s,
                %(useful_count)s, %(funny_count)s, %(cool_count)s,
                %(word_count)s, %(char_count)s, %(sentence_count)s, %(avg_word_length)s,
                %(language)s, %(language_confidence)s, %(data_quality_score)s,
                %(is_spam)s, %(is_anomaly)s, %(source)s,
                %(ingestion_timestamp)s, %(processing_timestamp)s
            )
            ON CONFLICT (review_id) DO UPDATE SET
                data_quality_score = EXCLUDED.data_quality_score,
                processing_timestamp = EXCLUDED.processing_timestamp
        """
        
        try:
            execute_batch(self.pg_cursor, query, records)
            self.pg_conn.commit()
            print(f"✓ Inserted batch of {len(records)} reviews")
        except Exception as e:
            self.pg_conn.rollback()
            print(f"✗ Failed to insert batch: {e}")
            raise
    
    def process_message(self, message) -> Dict:
        """Convert Kafka message to database record"""
        data = message.value

        for field in ['date', 'ingestion_timestamp', 'processing_timestamp']:
            if isinstance(data.get(field), str):
                data[field] = datetime.fromisoformat(data[field].replace('Z', '+00:00'))

        if 'useful' in data:
            data['useful_count'] = data.pop('useful')
        if 'funny' in data:
            data['funny_count'] = data.pop('funny')
        if 'cool' in data:
            data['cool_count'] = data.pop('cool')

        return data
    
    def run(self):
        """Main consumption loop"""
        self.connect()
        
        batch = []
        last_commit = time.time()
        messages_processed = 0
        
        print(f"\n{'='*70}")
        print("POSTGRESQL SINK - Starting consumption")
        print(f"{'='*70}\n")
        
        try:
            for message in self.consumer:
                if message.value is None:
                    continue

                record = self.process_message(message)
                batch.append(record)

                batch_full = len(batch) >= self.batch_size
                timeout_reached = (time.time() - last_commit) >= self.batch_timeout_seconds
                
                if batch_full or timeout_reached:
                    self.insert_batch(batch)
                    self.consumer.commit()
                    
                    messages_processed += len(batch)
                    print(f"[PROGRESS] Total reviews saved: {messages_processed}")
                    
                    batch = []
                    last_commit = time.time()
                    
        except KeyboardInterrupt:
            print("\n\nShutting down...")
        finally:
            if batch:
                self.insert_batch(batch)
                self.consumer.commit()
            self.close()
            print(f"\n✓ Total reviews saved: {messages_processed}\n")


if __name__ == "__main__":
    sink = PostgresSink()
    sink.run()