"""
Data models for streaming Yelp review data and associated data quality issues.
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, validator
from enum import Enum

class DataQualityIssueType(str, Enum):
    MISSING_VALUE = "missing_value"
    OUT_OF_RANGE = "out_of_range"
    DUPLICATE = "duplicate"
    STALE_DATA = "stale_data"
    INVALID_FORMAT = "invalid_format"
    SPAM_DETECTED = "spam_detected"
    WRONG_LANGUAGE = "wrong_language"
    UNSUPPORTED_LANGUAGE = "unsupported_language"
    TOO_SHORT = "too_short"
    TOO_LONG = "too_long"

class RawYelpReview(BaseModel):
    """ 
    Just loading in the raw Yelp review.
    We will do data quality checks later.
    """
    review_id: str
    user_id: str
    business_id: str
    rating: Optional[float] = None
    text: Optional[str] = None
    date: datetime
    useful: Optional[int] = 0
    funny: Optional[int] = 0
    cool: Optional[int] = 0

    source: str = "yelp_api"
    ingestion_timestamp: datetime = Field(default_factory=datetime.utcnow)

class CleanedYelpReview(BaseModel):
    """
    Cleaning and validating.
    """

    review_id: str
    user_id: str
    business_id: str
    rating: int = Field(ge=1.0, le=5.0)
    text: str = Field(min_length=10, max_length=5000)
    date: datetime
    useful: int = Field(ge=0, default=0)
    funny: int = Field(ge=0, default=0)
    cool: int = Field(ge=0, default=0)

    # NLP features (That weren't necessary before)
    word_count: int
    char_count: int
    avg_word_length: float
    sentence_count: int

    language: str = "en"
    language_confidence: float = Field(ge=0.0, le=1.0)
    data_quality_score: float = Field(ge=0.0, le=1.0)
    is_spam: bool = False
    is_anomaly: bool = False

    source: str
    ingestion_timestamp: datetime
    processing_timestamp: datetime = Field(default_factory=datetime.utcnow)

    @validator('text')
    def validate_text(cls, v):
        """
        Some custom stuff for validating text.
        """
        if not v or not v.strip():
            raise ValueError("Text cannot be empty or whitespace")
        
        alpha_chars = sum(c.isalpha() for c in v)
        if alpha_chars / len(v) < 0.4:
            raise ValueError("Text must contain at least 40% alphabetic characters")
        
        return v.strip()

class DataQualityIssue(BaseModel):
    """
    Tracks what's wrong with each review.
    """
    review_id: str
    issue_type: DataQualityIssueType
    field_name: Optional[str] = None

    og_value: Optional[str] = None
    expected_range: Optional[str] = None

    severity: int = Field(ge=1, le=10, default=5)

    detected_at: datetime = Field(default_factory=datetime.utcnow)
    detection_range: str = "validation"

class ReviewStats(BaseModel):
    """
    Tracks aggregated stats.
    """
    window_start: datetime
    window_end: datetime
    business_id: Optional[str] = None

    total_reviews: int = 0
    valid_reviews: int = 0
    rejected_reviews: int = 0

    avg_quality_score: float = 0.0
    spam_count: int = 0
    wrong_language_count: int = 0

    rating_1_count: int = 0
    rating_2_count: int = 0
    rating_3_count: int = 0
    rating_4_count: int = 0
    rating_5_count: int = 0
    avg_rating: float = 0.0

    avg_word_count: float = 0.0
    avg_sentence_count: float = 0.0
    
    created_at: datetime = Field(default_factory=datetime.utcnow)


# Now we can define validation rules...

VALIDATION_RULES = {
    'text': {
        'min_length': 10,
        'max_length': 5000,
        'min_alpha_ratio': 0.4,
        'max_caps_ratio': 0.7,
        'weight': 0.25
    },
    'rating': {
        'min': 1,
        'max': 5,
        'weight': 0.15
    },
    'language_confidence': {
        'min': 0.0,
        'max': 1.0,
        'threshold': 0.8,
        'weight': 0.20
    },
    'word_count': {
        'min': 3,
        'max': 1000,
        'weight': 0.15
    },
    'useful_count': {
        'min': 0,
        'max': 10000,
        'weight': 0.05
    },
    'age_hours': {
        'min': 0,
        'max': 24 * 365 * 5,
        'stale_threshold': 24*30,
        'weight': 0.10
    },
    'spam_indicators': {
        'max_duplicate_chars': 5,
        'max_url_count': 2,
        'max_email_count': 1,
        'weight': 0.10
    }
}

SPAM_PATTERNS = [
    r'\b(buy|purchase|discount|promo|click here)\b',
    r'http[s]?://', # URLs
    r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b',
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
    r'(\w)\1{4,}',
]

SUPPORTED_LANGUAGES = ['en']

QUALITY_WEIGHTS = {
    'completeness': 0.25,
    'validity': 0.25,
    'consistency': 0.20,
    'freshness': 0.10,
    'spam_check': 0.20
}