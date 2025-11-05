def parse_identifier_fields(identifier: str) -> dict:
    """
    Parse the document_class_identifier_by_organization string.
    
    Args:
        identifier: String in format "Question | Points | IsKO | IsPlausible"
        
    Returns:
        Dictionary with parsed fields: points, is_ko, is_plausible
    """
    if not identifier or identifier.count('|') < 3:
        return {'points': 0, 'is_ko': False, 'is_plausible': False}
    
    parts = [part.strip() for part in identifier.split('|')]
    
    return {
        'points': int(parts[1]) if parts[1].isdigit() else 0,
        'is_ko': parts[2].lower() == 'true',
        'is_plausible': parts[3].lower() == 'true'
    }

def get_yes_no_value(node: dict) -> bool:
    """
    Extract the Yes/No value from a node's children.
    
    Args:
        node: Document node dictionary
        
    Returns:
        Boolean value (True for "True", False otherwise)
    """
    for child in node.get('children', []):
        if child.get('document_class_display_name') == 'Yes/No':
            return child.get('value', '').lower() == 'true'
    return True  # Default to True if not found

def get_document_id(document: dict) -> str:
    """
    Extract the document_id from the document.
    
    Args:
        document: Document dictionary
        
    Returns:
        Document ID string
    """
    return document.get('document_id', '')

def collect_questions(children: list) -> list:
    """
    Recursively collect all questions with their metadata.
    
    Args:
        children: List of child nodes
        
    Returns:
        List of question dictionaries
    """
    questions = []
    
    for child in children:
        identifier = child.get('document_class_identifier_by_organization')
        
        if identifier and '|' in identifier:
            parsed = parse_identifier_fields(identifier)
            yes_no_value = get_yes_no_value(child)
            
            questions.append({
                'identifier': identifier,
                'points': parsed['points'],
                'is_ko': parsed['is_ko'],
                'is_plausible': parsed['is_plausible'],
                'yes_no_value': yes_no_value,
                'node': child
            })
        
        # Recursively process children
        if child.get('children'):
            questions.extend(collect_questions(child['children']))
    
    return questions

def calculate_total_risk_score(questions: list) -> int:
    """
    Calculate total risk score from KO questions answered False.
    
    Args:
        questions: List of question dictionaries
        
    Returns:
        Total risk score
    """
    return sum(
        q['points'] 
        for q in questions 
        if q['is_ko'] and not q['yes_no_value']
    )

def count_questions_answered_no(questions: list) -> int:
    """
    Count the total number of questions answered with No/False.
    
    Args:
        questions: List of question dictionaries
        
    Returns:
        Count of questions answered No/False
    """
    return sum(1 for q in questions if not q['yes_no_value'])

def count_ko_questions_answered_no(questions: list) -> int:
    """
    Count the number of KO questions answered with No/False.
    
    Args:
        questions: List of question dictionaries
        
    Returns:
        Count of KO questions answered No/False
    """
    return sum(1 for q in questions if q['is_ko'] and not q['yes_no_value'])

def count_plausible_checks_answered_no(questions: list) -> int:
    """
    Count the number of plausible check questions answered with No/False.
    
    Args:
        questions: List of question dictionaries
        
    Returns:
        Count of plausible check questions answered No/False
    """
    return sum(1 for q in questions if q['is_plausible'] and not q['yes_no_value'])

def calculate_category_metrics(category_node: dict) -> dict:
    """
    Calculate risk metrics for a specific category.
    
    Args:
        category_node: Category node from document
        
    Returns:
        Dictionary with category metrics
    """
    questions = collect_questions(category_node.get('children', []))
    
    max_points = sum(q['points'] for q in questions)
    risk_score = calculate_total_risk_score(questions)
    risk_ratio = risk_score / max_points if max_points > 0 else 0
    
    return {
        'category_name': category_node.get('document_class_identifier_by_organization', 
                                          category_node.get('document_class_display_name', 'Unknown')),
        'max_total_risk_points': max_points,
        'total_risk_score': risk_score,
        'risk_ratio': round(risk_ratio, 4)
    }

def analyze_document(data: dict) -> dict:
    """
    Main function to analyze document and generate comprehensive report.
    
    Args:
        data: Input document data
        
    Returns:
        Dictionary with all calculated metrics
    """
    upload_data = data.get('upload', {})
    document = data.get('document', {})
    
    # Extract basic information
    document_id = get_document_id(document)
    filename = upload_data.get('document_id_by_organization', '')
    assessment = document.get('document_class', '').lstrip('/')
    
    # Get root level children (categories)
    root_children = document.get('children', [])
    
    # Collect all questions from all levels
    all_questions = []
    for category in root_children:
        all_questions.extend(collect_questions(category.get('children', [])))
    
    # Calculate metrics
    number_of_questions = len(all_questions)
    number_of_ko_questions = sum(1 for q in all_questions if q['is_ko'])
    number_of_plausible_checks = sum(1 for q in all_questions if q['is_plausible'])
    number_of_questions_answered_no = count_questions_answered_no(all_questions)
    number_of_ko_questions_answered_no = count_ko_questions_answered_no(all_questions)
    number_of_plausible_checks_answered_no = count_plausible_checks_answered_no(all_questions)
    
    # Set is_plausible flag: True if no plausible checks were answered No, False otherwise
    is_plausible = (number_of_plausible_checks_answered_no == 0)
    
    max_total_risk_points = sum(q['points'] for q in all_questions)
    total_risk_score = calculate_total_risk_score(all_questions)
    risk_ratio = total_risk_score / max_total_risk_points if max_total_risk_points > 0 else 0
    
    # Calculate category-level metrics
    category_metrics = [
        calculate_category_metrics(category) 
        for category in root_children
    ]
    
    return {
        'document_id': document_id,
        'filename': filename,
        'assessment': assessment,
        'number_of_questions': number_of_questions,
        'number_of_ko_questions': number_of_ko_questions,
        'number_of_plausible_checks': number_of_plausible_checks,
        'number_of_questions_answered_no': number_of_questions_answered_no,
        'number_of_ko_questions_answered_no': number_of_ko_questions_answered_no,
        'number_of_plausible_checks_answered_no': number_of_plausible_checks_answered_no,
        'is_plausible': is_plausible,
        'max_total_risk_points': max_total_risk_points,
        'total_risk_score': total_risk_score,
        'risk_ratio': round(risk_ratio, 4),
        'categories': category_metrics
    }

if __name__ == "__main__":
    import json

    # Load your data
    with open('/Users/daniellanghann/src/api-showcase/api-showcase/results_9fce5c4febdd4cd187240f088d2833f3_20251029_141452.json', 'r') as f:
        data = json.load(f)

    result = analyze_document(data=data)
    print(json.dumps(result, indent=2))