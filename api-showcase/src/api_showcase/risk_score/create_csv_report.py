import csv

from .calculate_risk_scores import analyze_document, collect_questions

import csv


def create_csv_report(data: dict, output_filename: str = None):
    """
    Create a CSV file with detailed question information.
    
    Args:
        data: Input document data
        output_filename: Name of the output CSV file (optional, auto-generated if not provided)
    
    Returns:
        Path to the created CSV file
    """
    document = data.get('document', {})
    
    # Auto-generate filename if not provided
    if output_filename is None:
        document_id = document.get('document_id', 'unknown')
        output_filename = f'report_{document_id}.csv'
    
    # Collect all questions with their category information
    rows = []
    root_children = document.get('children', [])
    
    for category_node in root_children:
        category_name = category_node.get('document_class_identifier_by_organization', 
                                         category_node.get('document_class_display_name', 'Unknown'))
        
        # Get all questions in this category
        questions = collect_questions(category_node.get('children', []))
        
        for q in questions:
            # Extract question text (first part before |)
            question_text = q['identifier'].split('|')[0].strip() if '|' in q['identifier'] else q['identifier']
            
            # Determine answer text
            answer = 'Yes' if q['yes_no_value'] else 'No'
            
            # Potential risk points - the point value of the question regardless of KO status
            potential_risk_points = q['points']
            
            # Actual risk points for this specific question
            # Risk points are only counted if it's a KO question AND answered No
            actual_risk_points = q['points'] if (q['is_ko'] and not q['yes_no_value']) else 0
            
            rows.append({
                'category': category_name,
                'question': question_text,
                'answer': answer,
                'potential_risk_points': potential_risk_points,
                'actual_risk_points': actual_risk_points,
                'ko_question': 'Yes' if q['is_ko'] else 'No',
                'plausible_check': 'Yes' if q['is_plausible'] else 'No'
            })
    
    # Write to CSV
    fieldnames = ['category', 'question', 'answer', 'potential_risk_points', 'actual_risk_points', 'ko_question', 'plausible_check']
    
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"CSV report created: {output_filename}")
    return output_filename

# Usage example in main:
if __name__ == "__main__":
    import json

    # Load your data
    with open('/Users/daniellanghann/src/api-showcase/api-showcase/2025-10-29 16:44:19.008444_results_4ce07b0217e94a6a830461901a4f2a25_20251029_164419.json', 'r') as f:
        data = json.load(f)

    # Create the analysis report
    result = analyze_document(data=data)
    print(json.dumps(result, indent=2))
    
    # Create the CSV report
    csv_file = create_csv_report(data)
    
    # Or specify a custom filename:
    # csv_file = create_csv_report(data, output_filename='my_custom_report.csv')