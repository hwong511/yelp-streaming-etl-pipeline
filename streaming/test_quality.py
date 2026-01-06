from models import RawYelpReview
from quality_engine import QualityEngine
from producer import YelpDataLoader


def test_quality_engine():

    print("\n" + "="*70)
    print("Testing quality engiine...")
    print("="*70 + "\n")
    
    loader = YelpDataLoader('../data/external/yelp_reviews.json')
    engine = QualityEngine()
    
    total = 0
    accepted = 0
    rejected = 0
    issue_counts = {}
    
    for review in loader.load_reviews(limit=100):
        total += 1
        
        cleaned, issues = engine.clean_review(review)
        
        if cleaned:
            accepted += 1
            print(f"\n ACCEPTED - Quality: {cleaned.data_quality_score:.3f}")
            print(f"    Text: {review.text[:80]}...")
            print(f"    Rating: {review.rating} stars")
            print(f"    Language: {cleaned.language} (confidence: {cleaned.language_confidence:.3f})")
            print(f"    Features: {cleaned.word_count} words, {cleaned.sentence_count} sentences")
            if issues:
                print(f"  Minor issues: {len(issues)}")
        else:
            rejected += 1
            print(f"\n REJECTED")
            print(f"    Text: {review.text[:80] if review.text else 'NO TEXT'}...")
            print(f"    Issues: {len(issues)}")
            for issue in issues:
                print(f"    - {issue.issue_type.value}: {issue.field_name}")
                if issue.og_value:
                    print(f"        Details: {issue.og_value[:200]}")
                issue_type = issue.issue_type.value
                issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1
    
    print("\n" + "="*70)
    print(f"Total processed:  {total}")
    print(f"Accepted:         {accepted} ({accepted/total*100:.1f}%)")
    print(f"Rejected:         {rejected} ({rejected/total*100:.1f}%)")
    
    if issue_counts:
        print("\nRejection reasons:")
        for issue_type, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
            print(f"  {issue_type}: {count}")

if __name__ == '__main__':
    test_quality_engine()