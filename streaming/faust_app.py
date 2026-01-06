import faust
from datetime import datetime, timedelta
from typing import Optional
import json
from models import (
    RawYelpReview,
    CleanedYelpReview,
    DataQualityIssue,
    ReviewStats,
)
from quality_engine import QualityEngine

def datetime_to_iso(dt: datetime) -> str:
    return dt.isoformat() if dt else None


def iso_to_datetime(s: str) -> datetime:
    return datetime.fromisoformat(s) if s else None

app = faust.App(
    'yelp-review-etl',
    broker='kafka://localhost:9092',
    value_serializer='json',
    processing_guarantee='exactly_once',
    store='rocksdb://',
    topic_partitions=1,
)


quality_engine = QualityEngine()


class RawReviewRecord(faust.Record, serializer='json'):
    review_id: str
    business_id: str
    user_id: str
    date: str
    ingestion_timestamp: str
    text: Optional[str] = None
    rating: Optional[float] = None
    useful: Optional[int] = 0
    funny: Optional[int] = 0
    cool: Optional[int] = 0
    source: str = "yelp_dataset"


class CleanedReviewRecord(faust.Record, serializer='json'):
    review_id: str
    business_id: str
    user_id: str
    text: str
    rating: float
    date: str
    useful: int
    funny: int
    cool: int
    word_count: int
    char_count: int
    sentence_count: int
    avg_word_length: float
    language: str
    language_confidence: float
    data_quality_score: float
    is_spam: bool
    is_anomaly: bool
    source: str
    ingestion_timestamp: str
    processing_timestamp: str


# Kafka topics!!
raw_reviews_topic = app.topic('raw_reviews', value_type=RawReviewRecord)
cleaned_reviews_topic = app.topic('cleaned_reviews', value_type=CleanedReviewRecord)
quality_issues_topic = app.topic('quality_issues', value_type=str)

# Deduplication table
dedup_table = app.Table(
    'review_dedup',
    default=None,
).tumbling(timedelta(hours=1), expires=timedelta(hours=2))

# Stats table
stats_table = app.Table(
    'review_stats',
    default=lambda: {
        'total': 0,
        'accepted': 0,
        'rejected': 0,
        'total_quality_score': 0.0,
    }
).tumbling(timedelta(hours=1), expires=timedelta(days=7))


# The main processing agent...........................
@app.agent(raw_reviews_topic)
async def process_reviews(reviews):
    async for review_record in reviews:
        review = RawYelpReview(
            review_id=review_record.review_id,
            business_id=review_record.business_id,
            user_id=review_record.user_id,
            text=review_record.text,
            rating=review_record.rating,
            date=iso_to_datetime(review_record.date),
            useful=review_record.useful,
            funny=review_record.funny,
            cool=review_record.cool,
            source=review_record.source,
            ingestion_timestamp=iso_to_datetime(review_record.ingestion_timestamp),
        )
        
        # Dupe?
        dedup_key = f"{review.review_id}:{review.date.isoformat()}"
        
        if dedup_key in dedup_table:
            issue = DataQualityIssue(
                review_id=review.review_id,
                issue_type='duplicate',
                field_name='review_id',
                severity=7
            )
            await quality_issues_topic.send(value=issue.json())
            print(f"[DUPLICATE] Skipped review {review.review_id}")
            continue
        dedup_table[dedup_key] = True
        
        # Quality?
        cleaned, issues = quality_engine.clean_review(review)
        
        # Route based on result?
        if cleaned:
            cleaned_record = CleanedReviewRecord(
                review_id=cleaned.review_id,
                business_id=cleaned.business_id,
                user_id=cleaned.user_id,
                text=cleaned.text,
                rating=cleaned.rating,
                date=datetime_to_iso(cleaned.date),
                useful=cleaned.useful,
                funny=cleaned.funny,
                cool=cleaned.cool,
                word_count=cleaned.word_count,
                char_count=cleaned.char_count,
                sentence_count=cleaned.sentence_count,
                avg_word_length=cleaned.avg_word_length,
                language=cleaned.language,
                language_confidence=cleaned.language_confidence,
                data_quality_score=cleaned.data_quality_score,
                is_spam=cleaned.is_spam,
                is_anomaly=cleaned.is_anomaly,
                source=cleaned.source,
                ingestion_timestamp=datetime_to_iso(cleaned.ingestion_timestamp),
                processing_timestamp=datetime_to_iso(cleaned.processing_timestamp),
            )
            
            await cleaned_reviews_topic.send(
                key=cleaned.business_id,
                value=cleaned_record
            )
            
            print(f"[ACCEPTED] Review {review.review_id} | "
                  f"Quality: {cleaned.data_quality_score:.3f} | "
                  f"Words: {cleaned.word_count}")
        else:
            print(f"[REJECTED] Review {review.review_id} | "
                  f"Issues: {len(issues)}")
        
        if issues:
            for issue in issues:
                await quality_issues_topic.send(value=issue.json())


@app.agent(cleaned_reviews_topic)
async def aggregate_stats(reviews):
    async for review in reviews:
        review_date = iso_to_datetime(review.date)
        hour_key = review_date.replace(minute=0, second=0, microsecond=0)
        stats = stats_table[hour_key]

        stats['total'] += 1
        stats['accepted'] += 1
        stats['total_quality_score'] += review.data_quality_score
        stats_table[hour_key] = stats

        if stats['total'] % 100 == 0:
            avg_quality = stats['total_quality_score'] / stats['accepted']
            print(f"[STATS] Hour {hour_key} | "
                  f"Total: {stats['total']} | "
                  f"Avg Quality: {avg_quality:.3f}")



# For eventually when I export metrics (e.g. for monitoring using Prometheus or something)
@app.timer(interval=60.0)
async def print_pipeline_stats():
    pass


if __name__ == '__main__':
    app.main()