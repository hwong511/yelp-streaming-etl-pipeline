"""
Handles validation, cleaning, and quality socring
"""
import re
from datetime import datetime, timedelta
from typing import Tuple, List, Optional
from models import (
    RawYelpReview,
    CleanedYelpReview,
    DataQualityIssue,
    DataQualityIssueType,
    VALIDATION_RULES,
    SPAM_PATTERNS,
    SUPPORTED_LANGUAGES,
    QUALITY_WEIGHTS
)

try:
    from langdetect import detect, detect_langs, LangDetectException
except ImportError:
    raise ImportError("LangDetect not installed.")
    detect = None

class QualityEngine:
    def __init__(self):
        self.spam_patterns = [re.compile(pattern) for pattern in SPAM_PATTERNS]
        self.validation_rules = VALIDATION_RULES

    def calculate_quality_score(self, review:RawYelpReview) -> float:
        scores = {}

        scores['completeness'] = self._score_completeness(review)
        scores['validity'] = self._score_validity(review)
        scores['freshness'] = self._score_freshness(review)
        scores['spam_check'] = self._score_spam(review)

        total_score = sum(
            scores[key] * QUALITY_WEIGHTS[key] for key in scores
        )

        return round(total_score, 2)
    
    def _score_completeness(self, review:RawYelpReview) -> float:
        req_fields = ['review_id', 'business_id', 'user_id', 'text', 'rating', 'date']
        optional_fields = ['useful', 'funny', 'cool']

        required_present = sum(
            1 for field in req_fields if getattr(review, field, None) is not None
        )

        optional_present = sum(
            1 for field in optional_fields if getattr(review, field, None) is not None
        )

        required_score = (required_present / len(req_fields)) * 0.8
        optional_score = (optional_present / len(optional_fields)) * 0.2

        return required_score + optional_score
    
    def _score_validity(self, review:RawYelpReview) -> float:
        scores = []

        if review.text:
            text_len = len(review.text)
            rules = self.validation_rules['text']

            if rules['min_length'] <= text_len <= rules['max_length']:
                scores.append(1.0)
            else:
                if text_len < rules['min_length']:
                    scores.append(text_len / rules['min_length'])
                else:
                    scores.append(rules['max_length'] / text_len)
        else:
            scores.append(0.0)
        
        if review.rating:
            rules = self.validation_rules['rating']
            if rules['min'] <= review.rating <= rules['max']:
                scores.append(1.0)
            else:
                scores.append(0.0)
        else:
            scores.append(0.0)

        return sum(scores) / len(scores) if scores else 0.0

    def _score_freshness(self, review:RawYelpReview) -> float:
        now = datetime.utcnow()
        review_date = review.date

        if review_date > now:
            return 0.0
    
        age = now - review_date
        age_hours = age.total_seconds() / 3600

        rules = self.validation_rules['age_hours']

        if age_hours <= rules['max']:
            return 0.0
        
        stale_threshold = rules['stale_threshold']
        if age_hours <= stale_threshold:
            score = 1.0 - (0.5 * age_hours / stale_threshold)
        else:
            remaining = age_hours - stale_threshold
            max_remaining = rules['max'] - stale_threshold
            score = 0.5 * (1.0 - remaining / max_remaining)
        
        return max(0.0, score)

    def _score_spam(self, review:RawYelpReview) -> float:
        if not review.text:
            return 0.0
        
        text = review.text.lower()
        spam_score = 1.0

        pattern_matches = 0
        for pattern in self.spam_patterns:
            if pattern.search(text):
                pattern_matches += 1
        
        if pattern_matches > 0:
            spam_score -= (0.3 * pattern_matches)
        
        punct_count = sum(1 for c in text if c in '!?.,;:')
        if len(text) > 0:
            punct_ratio = punct_count / len(text)
            if punct_ratio > 0.3:
                spam_score -= 0.3

        if text.isupper() and len(text) > 10:
            spam_score -= 0.4
        
        repeated_chars = re.findall(r'(.)\1{3,}', text)
        if len(repeated_chars) > 2:
            spam_score -= 0.2
        
        digit_count = sum(1 for c in text if c.isdigit())
        if len(text) > 0:
            digit_ratio = digit_count / len(text)
            if digit_ratio > 0.3:
                spam_score -= 0.3
        
        return max(0.0, min(1.0, spam_score))

    def detect_language(self, text:str) -> Tuple[str, float]:
        if not text or not detect:
            return('unknown', 0.0)
        
        try:
            langs = detect_langs(text)
            if langs:
                top_lang = langs[0]
                return (top_lang.lang, top_lang.prob)
            else:
                return ('unknown', 0.0)
        except LangDetectException:
            return ('unknown', 0.0)
    
    def detect_spam(self, text: str) -> Tuple[bool, List[str]]:
        if not text:
            return (True, ['empty_text'])
        
        matched = []
        text_lower = text.lower()

        for i, pattern in enumerate(self.spam_patterns):
            if pattern.search(text_lower):
                matched.append(f'pattern_{i}')
        is_spam = len(matched) >= 2

        return (is_spam, matched)
    
    def calculate_text_features(self, text: str) -> dict:
        if not text:
            return {
                'word_count': 0,
                'char_count': 0,
                'sentence_count': 0,
                'avg_word_length': 0.0,
                'caps_ratio': 0.0,
                'punct_ratio': 0.0,
                'digit_ratio': 0.0
            }
        
        char_count = len(text)
        words = text.split()
        word_count = len(words)

        sentence_count = max(1, text.count('.') + text.count('!') + text.count('?'))

        if word_count > 0:
            total_word_length = sum(len(word.strip('.,!?;:')) for word in words)
            avg_word_length = total_word_length / word_count
        else:
            avg_word_length = 0.0
        
        non_space_chars = text.replace(' ', '')
        if len(non_space_chars) > 0:
            caps_ratio = sum(1 for c in non_space_chars if c.isupper()) / len(non_space_chars)
        else:
            caps_ratio = 0.0
        
        punct_chars = sum(1 for c in text if c in '.,!?;:\'"()-')
        punct_ratio = punct_chars / char_count if char_count > 0 else 0.0

        digit_chars = sum(1 for c in text if c.isdigit())
        digit_ratio = digit_chars / char_count if char_count > 0 else 0.0

        return {
            'word_count': word_count,
            'char_count': char_count,
            'sentence_count': sentence_count,
            'avg_word_length':round(avg_word_length, 2),
            'caps_ratio': round(caps_ratio, 2),
            'punct_ratio': round(punct_ratio, 2),
            'digit_ratio': round(digit_ratio, 2)
        }

    def clean_review(self, raw_review:RawYelpReview) -> Tuple[Optional[CleanedYelpReview], List[DataQualityIssue]]:
        issues = []

        if not raw_review.text or not raw_review.rating:
            if not raw_review.text:
                issues.append(DataQualityIssue(
                    review_id=raw_review.review_id,
                    issue_type=DataQualityIssueType.MISSING_VALUE,
                    field_name='text',
                    severity=10
                ))
            if not raw_review.rating:
                issues.append(DataQualityIssue(
                    review_id=raw_review.review_id,
                    issue_type=DataQualityIssueType.MISSING_VALUE,
                    field_name='rating',
                    severity=10
                ))
            return (None, issues)
        
        lang, lang_conf = self.detect_language(raw_review.text)

        if lang not in SUPPORTED_LANGUAGES:
            issues.append(DataQualityIssue(
                review_id=raw_review.review_id,
                issue_type=DataQualityIssueType.UNSUPPORTED_LANGUAGE,
                field_name='text',
                og_value=lang,
                expected_range=str(SUPPORTED_LANGUAGES),
                severity=8
            ))

            return (None, issues)
    

        if lang_conf < self.validation_rules['language_confidence']['threshold']:
            issues.append(DataQualityIssue(
                review_id=raw_review.review_id,
                issue_type=DataQualityIssueType.WRONG_LANGUAGE,
                field_name='language_confidence',
                og_value=str(lang_conf),
                expected_range=f">={self.validation_rules['language_confidence']['threshold']}",
                severity=6
            ))
        
        is_spam, spam_patterns = self.detect_spam(raw_review.text)

        if is_spam:
            issues.append(DataQualityIssue(
                review_id=raw_review.review_id,
                issue_type=DataQualityIssueType.SPAM_DETECTED,
                field_name='text',
                og_value=f'Matched patterns: {spam_patterns}',
                severity=9
            ))
            return (None, issues)
        
        text_len = len(raw_review.text)
        text_rules = self.validation_rules['text']

        if text_len < text_rules['min_length']:
            issues.append(DataQualityIssue(
                review_id=raw_review.review_id,
                issue_type=DataQualityIssueType.TOO_SHORT,
                field_name='text',
                og_value=str(text_len),
                expected_range=f">={text_rules['min_length']}",
                severity=7
            ))

            return (None, issues)
    
        if text_len > text_rules['max_length']:
            issues.append(DataQualityIssue(
                review_id=raw_review.review_id,
                issue_type=DataQualityIssueType.TOO_LONG,
                field_name='text',
                og_value=str(text_len),
                expected_range=f"<={text_rules['max_length']}",
                severity=5
            ))

            cleaned_text = raw_review.text[:text_rules['max_length']]
        else:
            cleaned_text = raw_review.text
        
        if raw_review.rating < 1 or raw_review.rating > 5:
            issues.append(DataQualityIssue(
                review_id=raw_review.review_id,
                issue_type=DataQualityIssueType.OUT_OF_RANGE,
                field_name='rating',
                og_value=str(raw_review.rating),
                expected_range='1-5',
                severity=10
            ))

            return (None, issues)
        
        text_features = self.calculate_text_features(cleaned_text)
        quality_score = self.calculate_quality_score(raw_review)

        try:
            cleaned_review = CleanedYelpReview(
                review_id=raw_review.review_id,
                business_id=raw_review.business_id,
                user_id=raw_review.user_id,
                text=cleaned_text,
                rating=raw_review.rating,
                date=raw_review.date,
                useful=raw_review.useful or 0,
                funny=raw_review.funny or 0,
                cool=raw_review.cool or 0,
                word_count=text_features['word_count'],
                char_count=text_features['char_count'],
                sentence_count=text_features['sentence_count'],
                avg_word_length=text_features['avg_word_length'],
                language=lang,
                language_confidence=lang_conf,
                data_quality_score=quality_score,
                is_spam=False,
                is_anomaly=quality_score < 0.5,
                source=raw_review.source,
                ingestion_timestamp=raw_review.ingestion_timestamp,
            )

            return (cleaned_review, issues)
        except Exception as e:
            issues.append(DataQualityIssue(
                review_id=raw_review.review_id,
                issue_type=DataQualityIssueType.INVALID_FORMAT,
                field_name='multiple',
                og_value=str(e),
                severity=10
            ))

            return (None, issues)
