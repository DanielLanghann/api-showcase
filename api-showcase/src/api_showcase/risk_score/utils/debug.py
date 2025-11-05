from ..calculate_risk_scores import collect_questions, analyze_document

def count_questions_answered_no(questions: list) -> int:
    """
    Count the total number of questions answered with No/False.
    
    Args:
        questions: List of question dictionaries
        
    Returns:
        Count of questions answered No/False
    """
    return sum(1 for q in questions if not q['yes_no_value'])

def debug_questions_answered_no(questions: list):
    """
    Debug function to show all questions answered No/False.
    
    Args:
        questions: List of question dictionaries
    """
    print("\n=== Questions Answered No/False ===")
    count = 0
    for q in questions:
        if not q['yes_no_value']:
            count += 1
            # Extract question text (first part before |)
            question_text = q['identifier'].split('|')[0].strip() if '|' in q['identifier'] else q['identifier']
            print(f"{count}. {question_text}")
            print(f"   Full identifier: {q['identifier']}")
            print(f"   Yes/No value: {q['yes_no_value']}")
            print()
    print(f"Total count: {count}")
    return count

# Modified main function with debug output
if __name__ == "__main__":
    import json

    # Load your data
    with open('/Users/daniellanghann/src/api-showcase/api-showcase/results_9fce5c4febdd4cd187240f088d2833f3_20251029_141452.json', 'r') as f:
        data = json.load(f)
    
    # Get root children
    document = data.get('document', {})
    root_children = document.get('children', [])
    
    # Collect all questions
    all_questions = []
    for category in root_children:
        all_questions.extend(collect_questions(category.get('children', [])))
    
    # Debug the count
    actual_count = debug_questions_answered_no(all_questions)
    
    print(f"\n=== Summary ===")
    print(f"Script counted: {count_questions_answered_no(all_questions)}")
    print(f"Debug counted: {actual_count}")
    
    # Run the full analysis
    result = analyze_document(data=data)
    print(f"\nFull analysis result:")
    print(json.dumps(result, indent=2))