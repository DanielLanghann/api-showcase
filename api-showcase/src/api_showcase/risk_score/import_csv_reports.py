#!/usr/bin/env python3
"""
CSV to PostgreSQL Importer (JSON Format)
This script imports CSV report files into a PostgreSQL database table,
storing all questions from a document as JSON in a single row.
"""

import os
import re
import csv
import json
import psycopg2
from collections import OrderedDict
from psycopg2.extras import Json
from pathlib import Path
from datetime import datetime


# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5434,
    'database': 'ovb',
    'user': 'ovb',
    'password': 'ovb'
}

# CSV folder path
CSV_FOLDER = '/Users/daniellanghann/src/api-showcase/api-showcase/csv_reports'

# Table creation SQL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS questions (
    id SERIAL PRIMARY KEY,
    document_id VARCHAR(255) NOT NULL UNIQUE,
    filename VARCHAR(500),
    questions_data JSONB NOT NULL,
    total_questions INTEGER,
    total_potential_risk_points INTEGER,
    total_actual_risk_points INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Create indexes for better performance
CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_questions_document_id ON questions(document_id);
CREATE INDEX IF NOT EXISTS idx_questions_data ON questions USING GIN (questions_data);
"""


def extract_document_id(filename):
    """
    Extract document_id from filename.
    Example: report_0a65f6b3176b4f43888238c21148c680_20251029_182403.csv
    Returns: 0a65f6b3176b4f43888238c21148c680
    """
    pattern = r'report_([a-f0-9]{32})_'
    match = re.search(pattern, filename)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Could not extract document_id from filename: {filename}")


def connect_to_database():
    """Establish connection to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✓ Connected to database '{DB_CONFIG['database']}' successfully")
        return conn
    except psycopg2.Error as e:
        print(f"✗ Error connecting to database: {e}")
        raise


def create_table(conn):
    """Create the questions table if it doesn't exist."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            cursor.execute(CREATE_INDEXES_SQL)
            conn.commit()
        print("✓ Table 'questions' created/verified successfully")
    except psycopg2.Error as e:
        print(f"✗ Error creating table: {e}")
        conn.rollback()
        raise


def import_csv_file(conn, filepath):
    """Import a single CSV file into the database."""
    filename = os.path.basename(filepath)
    
    try:
        # Extract document_id from filename
        document_id = extract_document_id(filename)
        print(f"\nProcessing: {filename}")
        print(f"  Document ID: {document_id}")
        
        # Read CSV file
        with open(filepath, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
        
        if not rows:
            print(f"  ⚠ Warning: No data found in {filename}")
            return 0
        
        # Convert CSV rows to JSON structure
        questions_data = []
        total_potential = 0
        total_actual = 0
        
        for row in rows:
            potential_points = int(row.get('potential_risk_points', 0)) if row.get('potential_risk_points') else 0
            actual_points = int(row.get('actual_risk_points', 0)) if row.get('actual_risk_points') else 0
            
            # Maintain field order: category, question, answer, then the rest
            from collections import OrderedDict
            question_obj = OrderedDict([
                ('category', row.get('category', '')),
                ('question', row.get('question', '')),
                ('answer', row.get('answer', '')),
                ('potential_risk_points', potential_points),
                ('actual_risk_points', actual_points),
                ('ko_question', row.get('ko_question', '')),
                ('plausible_check', row.get('plausible_check', ''))
            ])
            questions_data.append(question_obj)
            total_potential += potential_points
            total_actual += actual_points
        
        # Insert or update the record
        insert_sql = """
        INSERT INTO questions (
            document_id, filename, questions_data, 
            total_questions, total_potential_risk_points, 
            total_actual_risk_points
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (document_id) 
        DO UPDATE SET
            filename = EXCLUDED.filename,
            questions_data = EXCLUDED.questions_data,
            total_questions = EXCLUDED.total_questions,
            total_potential_risk_points = EXCLUDED.total_potential_risk_points,
            total_actual_risk_points = EXCLUDED.total_actual_risk_points,
            updated_at = CURRENT_TIMESTAMP
        """
        
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, (
                document_id,
                filename,
                Json(questions_data),
                len(questions_data),
                total_potential,
                total_actual
            ))
            conn.commit()
        
        print(f"  ✓ Imported {len(questions_data)} questions")
        print(f"  ✓ Total potential risk points: {total_potential}")
        print(f"  ✓ Total actual risk points: {total_actual}")
        return 1
        
    except ValueError as e:
        print(f"  ✗ Error: {e}")
        return 0
    except Exception as e:
        print(f"  ✗ Error importing {filename}: {e}")
        conn.rollback()
        return 0


def import_all_csv_files(conn, folder_path):
    """Import all CSV files from the specified folder."""
    folder = Path(folder_path)
    
    if not folder.exists():
        print(f"✗ Error: Folder not found: {folder_path}")
        return
    
    # Find all CSV files matching the pattern
    csv_files = list(folder.glob('report_*.csv'))
    
    if not csv_files:
        print(f"⚠ Warning: No CSV files found in {folder_path}")
        return
    
    print(f"\nFound {len(csv_files)} CSV file(s) to import")
    print("=" * 60)
    
    successful_imports = 0
    
    for csv_file in csv_files:
        result = import_csv_file(conn, csv_file)
        if result > 0:
            successful_imports += 1
    
    print("\n" + "=" * 60)
    print(f"✓ Import complete!")
    print(f"  Documents imported: {successful_imports}/{len(csv_files)}")


def print_sample_queries():
    """Print some useful SQL query examples."""
    print("\n" + "=" * 60)
    print("USEFUL QUERIES:")
    print("=" * 60)
    print("""
-- Get all data for a specific document:
SELECT * FROM questions WHERE document_id = '0a65f6b3176b4f43888238c21148c680';

-- Get all questions as pretty JSON:
SELECT document_id, 
       jsonb_pretty(questions_data) 
FROM questions;

-- Query specific questions within the JSON:
SELECT document_id, 
       q->>'category' as category,
       q->>'question' as question,
       q->>'answer' as answer
FROM questions, 
     jsonb_array_elements(questions_data) as q
WHERE document_id = '0a65f6b3176b4f43888238c21148c680';

-- Find documents with high actual risk points:
SELECT document_id, 
       filename, 
       total_actual_risk_points
FROM questions
WHERE total_actual_risk_points > 10
ORDER BY total_actual_risk_points DESC;

-- Find documents with specific answers:
SELECT document_id, filename
FROM questions
WHERE questions_data @> '[{"answer": "No"}]';

-- Count questions by category across all documents:
SELECT q->>'category' as category, COUNT(*)
FROM questions, 
     jsonb_array_elements(questions_data) as q
GROUP BY q->>'category'
ORDER BY COUNT(*) DESC;
""")


def main():
    """Main execution function."""
    print("CSV to PostgreSQL Importer (JSON Format)")
    print("=" * 60)
    
    conn = None
    try:
        # Connect to database
        conn = connect_to_database()
        
        # Create table if not exists
        create_table(conn)
        
        # Import all CSV files
        import_all_csv_files(conn, CSV_FOLDER)
        
        # Print sample queries
        print_sample_queries()
        
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
    finally:
        if conn:
            conn.close()
            print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()