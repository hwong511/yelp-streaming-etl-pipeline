import json
import time
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from kafka import KafkaProducer
from models import RawYelpReview
import argparse
from typing import Optional

class YelpDataLoader:
    """Loads reviews from Yelp academic dataset JSON file"""
    
    def __init__(self, filepath: str):
        self.filepath = Path(filepath)
        if not self.filepath.exists():
            raise FileNotFoundError(
                f"Yelp dataset not found at {filepath}\n"
            )
    
    def load_reviews(self, limit: Optional[int] = None, shuffle: bool = False):
        """
        Load reviews from JSON file (one JSON object per line).
        """
        reviews = []
        count = 0
        
        print(f"Loading reviews from {self.filepath}...")
        
        with open(self.filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if limit and count >= limit:
                    break
                
                try:
                    # Parse Yelp JSON format
                    data = json.loads(line)
                    review = self._parse_yelp_json(data)
                    reviews.append(review)
                    count += 1
                    
                    if count % 10000 == 0:
                        print(f"Loaded {count:,} reviews...")
                        
                except json.JSONDecodeError:
                    print(f"Warning: Skipped malformed JSON line")
                    continue
                except Exception as e:
                    print(f"Warning: Error parsing review: {e}")
                    continue
        
        if shuffle:
            random.shuffle(reviews)
        
        for review in reviews:
            yield review
    
    def _parse_yelp_json(self, data: dict) -> RawYelpReview:
        """
        Convert Yelp JSON format to our RawYelpReview model.
        """
        date_str = data.get('date', '')
        try:
            review_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        except:
            review_date = datetime.now(timezone.utc)
        
        return RawYelpReview(
            review_id=data.get('review_id', ''),
            business_id=data.get('business_id', ''),
            user_id=data.get('user_id', ''),
            text=data.get('text'),
            rating=data.get('stars'),
            date=review_date,
            useful=data.get('useful', 0),
            funny=data.get('funny', 0),
            cool=data.get('cool', 0),
            source='yelp_dataset',
            ingestion_timestamp=datetime.now(timezone.utc),
        )

class YelpReviewProducer:
    """
    Produces Yelp reviews to Kafka topic.
    Supports configurable rate limiting to simulate real-time streaming.
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers: str = 'localhost:9092',
        topic: str = 'raw_reviews'
    ):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=5,
            compression_type='lz4',
            api_version=(2, 5, 0),  # Explicitly set API version for compatibility
        )
        self.messages_sent = 0
        self.start_time = None
    
    def send_review(self, review: RawYelpReview):
        """
        Send a single review to Kafka.
        Uses business_id as partition key for consistent routing.
        """
        review_dict = review.model_dump()
        
        key = review.business_id
        
        # Send to Kafka
        future = self.producer.send(
            self.topic,
            key=key,
            value=review_dict
        )
        
        self.messages_sent += 1
        
        return future
    
    def send_batch(
        self,
        reviews,
        reviews_per_second: int = 100,
        inject_delays: bool = True
    ):
        """
        Send reviews in batches with rate limiting.
        """
        self.start_time = time.time()

        print(f"Starting Kafka producer")
        print(f"Topic: {self.topic}")
        print(f"Target rate: {reviews_per_second} reviews/second")

        base_delay = 1.0 / reviews_per_second if reviews_per_second > 0 else 0
        
        last_report_time = time.time()
        report_interval = 10
        
        try:
            for review in reviews:
                self.send_review(review)
                
                if base_delay > 0:
                    if inject_delays:
                        delay = base_delay * random.uniform(0.5, 1.5)
                    else:
                        delay = base_delay
                    
                    time.sleep(delay)
                
                current_time = time.time()
                if current_time - last_report_time >= report_interval:
                    self._print_stats()
                    last_report_time = current_time
            
            print("\nFlushing remaining messages...")
            self.producer.flush()
            self._print_stats(final=True)
            
        except KeyboardInterrupt:
            print("\n\nStopping producer...")
            self.producer.flush()
            self._print_stats(final=True)
        
        finally:
            self.close()
    
    def _print_stats(self, final: bool = False):
        """Print throughput statistics"""
        elapsed = time.time() - self.start_time
        rate = self.messages_sent / elapsed if elapsed > 0 else 0
        
        prefix = "FINAL" if final else "STATUS"
        
        print(f"[{prefix}] Sent: {self.messages_sent:,} reviews | "
              f"Elapsed: {elapsed:.1f}s | "
              f"Rate: {rate:.1f} reviews/sec")
    
    def close(self):
        if self.producer:
            self.producer.close()
            print("Producer closed.")

def run_producer(
    data_file: str,
    kafka_servers: str = 'localhost:9092',
    topic: str = 'raw_reviews',
    limit: Optional[int] = None,
    rate: int = 100,
    shuffle: bool = False,
    inject_delays: bool = True
):
    """
    Main function to run the producer.
    """
    print(f"\n{'='*70}")
    print("YELP REVIEW PRODUCER - Batch Simulation Mode")
    print(f"{'='*70}\n")

    try:
        loader = YelpDataLoader(data_file)
        reviews = loader.load_reviews(limit=limit, shuffle=shuffle)
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: {e}")
        return

    producer = YelpReviewProducer(
        kafka_bootstrap_servers=kafka_servers,
        topic=topic
    )

    producer.send_batch(
        reviews=reviews,
        reviews_per_second=rate,
        inject_delays=inject_delays
    )

    print("\nProducer finished successfully.\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Stream Yelp reviews to Kafka',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
# Quick test: Send 1,000 reviews at 100/sec
python producer.py --limit 1000 --rate 100

# Slow realistic stream: 10 reviews/sec
python producer.py --limit 5000 --rate 10

# Fast stress test: 1,000 reviews/sec
python producer.py --limit 10000 --rate 1000

# Shuffle for out-of-order simulation
python producer.py --limit 5000 --shuffle

# Custom Kafka settings
python producer.py --kafka kafka-prod:9092 --topic prod_reviews
        """
    )

    parser.add_argument(
        '--data-file',
        default='../data/external/yelp_reviews.json',
        help='Path to Yelp reviews JSON file (default: ../data/external/yelp_reviews.json)'
    )

    parser.add_argument(
        '--kafka',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )

    parser.add_argument(
        '--topic',
        default='raw_reviews',
        help='Kafka topic name (default: raw_reviews)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Max number of reviews to send (default: all reviews in file)'
    )

    parser.add_argument(
        '--rate',
        type=int,
        default=100,
        help='Target reviews per second (default: 100)'
    )

    parser.add_argument(
        '--shuffle',
        action='store_true',
        help='Shuffle reviews to simulate out-of-order arrival'
    )

    parser.add_argument(
        '--no-delays',
        action='store_true',
        help='Disable variable delays (constant rate)'
    )

    args = parser.parse_args()

    run_producer(
        data_file=args.data_file,
        kafka_servers=args.kafka,
        topic=args.topic,
        limit=args.limit,
        rate=args.rate,
        shuffle=args.shuffle,
        inject_delays=not args.no_delays
    )