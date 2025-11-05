import os
import re
import json
import psycopg2
from psycopg2.extras import Json
from pathlib import Path


# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5434,
    'database': 'ovb',
    'user': 'ovb',
    'password': 'ovb'
}

# JSON folder path
JSON_FOLDER = '/Users/daniellanghann/src/api-showcase/api-showcase/results'

# Table creation SQL
CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS assessment_results (
            id SERIAL PRIMARY KEY,
            document_id VARCHAR(255) NOT NULL UNIQUE,
            filename VARCHAR(500),
            assessment VARCHAR(100),
            number_of_questions INTEGER,
            number_of_ko_questions INTEGER,
            number_of_plausible_checks INTEGER,
            number_of_questions_answered_no INTEGER,
            number_of_ko_questions_answered_no INTEGER,
            number_of_plausible_checks_answered_no INTEGER,
            is_plausible BOOLEAN,
            max_total_risk_points INTEGER,
            total_risk_score INTEGER,
            risk_ratio DECIMAL(10, 4),
            categories JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """

# Create indexes for better performance
CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_assessment_document_id ON assessment_results(document_id);
CREATE INDEX IF NOT EXISTS idx_assessment_categories ON assessment_results USING GIN (categories);
CREATE INDEX IF NOT EXISTS idx_assessment_risk_ratio ON assessment_results(risk_ratio);
CREATE INDEX IF NOT EXISTS idx_assessment_is_plausible ON assessment_results(is_plausible);
"""


def extract_document_id(filename):
    """
    Extract document_id from filename.
    Example: analytics_0a65f6b3176b4f43888238c21148c680_20251029_182402.json
    Returns: 0a65f6b3176b4f43888238c21148c680
    """
    pattern = r'analytics_([a-f0-9]{32})_'
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
    """Create the assessment_results table if it doesn't exist."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            cursor.execute(CREATE_INDEXES_SQL)
            conn.commit()
        print("✓ Table 'assessment_results' created/verified successfully")
    except psycopg2.Error as e:
        print(f"✗ Error creating table: {e}")
        conn.rollback()
        raise


def import_json_file(conn, filepath):
    """Import a single JSON file into the database."""
    filename = os.path.basename(filepath)
    
    try:
        # Extract document_id from filename
        document_id = extract_document_id(filename)
        print(f"\nProcessing: {filename}")
        print(f"  Document ID: {document_id}")
        
        # Read JSON file
        with open(filepath, 'r', encoding='utf-8') as jsonfile:
            data = json.load(jsonfile)
        
        if not data:
            print(f"  ⚠ Warning: No data found in {filename}")
            return 0
        
        # Extract categories as JSON
        categories = data.get('categories', [])
        
        # Insert or update the record
        insert_sql = """
        INSERT INTO assessment_results (
            document_id, filename, assessment,
            number_of_questions, number_of_ko_questions, number_of_plausible_checks,
            number_of_questions_answered_no, number_of_ko_questions_answered_no,
            number_of_plausible_checks_answered_no, is_plausible,
            max_total_risk_points, total_risk_score, risk_ratio,
            categories
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (document_id) 
        DO UPDATE SET
            filename = EXCLUDED.filename,
            assessment = EXCLUDED.assessment,
            number_of_questions = EXCLUDED.number_of_questions,
            number_of_ko_questions = EXCLUDED.number_of_ko_questions,
            number_of_plausible_checks = EXCLUDED.number_of_plausible_checks,
            number_of_questions_answered_no = EXCLUDED.number_of_questions_answered_no,
            number_of_ko_questions_answered_no = EXCLUDED.number_of_ko_questions_answered_no,
            number_of_plausible_checks_answered_no = EXCLUDED.number_of_plausible_checks_answered_no,
            is_plausible = EXCLUDED.is_plausible,
            max_total_risk_points = EXCLUDED.max_total_risk_points,
            total_risk_score = EXCLUDED.total_risk_score,
            risk_ratio = EXCLUDED.risk_ratio,
            categories = EXCLUDED.categories,
            updated_at = CURRENT_TIMESTAMP
        """
        
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, (
                document_id,
                data.get('filename', ''),
                data.get('assessment', ''),
                data.get('number_of_questions', 0),
                data.get('number_of_ko_questions', 0),
                data.get('number_of_plausible_checks', 0),
                data.get('number_of_questions_answered_no', 0),
                data.get('number_of_ko_questions_answered_no', 0),
                data.get('number_of_plausible_checks_answered_no', 0),
                data.get('is_plausible', False),
                data.get('max_total_risk_points', 0),
                data.get('total_risk_score', 0),
                data.get('risk_ratio', 0.0),
                Json(categories)
            ))
            conn.commit()
        
        print(f"  ✓ Imported assessment: {data.get('assessment', 'N/A')}")
        print(f"  ✓ Risk ratio: {data.get('risk_ratio', 0.0)}")
        print(f"  ✓ Is plausible: {data.get('is_plausible', False)}")
        print(f"  ✓ Categories: {len(categories)}")
        return 1
        
    except ValueError as e:
        print(f"  ✗ Error: {e}")
        return 0
    except json.JSONDecodeError as e:
        print(f"  ✗ Error parsing JSON in {filename}: {e}")
        return 0
    except Exception as e:
        print(f"  ✗ Error importing {filename}: {e}")
        conn.rollback()
        return 0


def import_all_json_files(conn, folder_path):
    """Import all JSON files from the specified folder."""
    folder = Path(folder_path)
    
    if not folder.exists():
        print(f"✗ Error: Folder not found: {folder_path}")
        return
    
    # Find all JSON files matching the pattern
    json_files = list(folder.glob('analytics_*.json'))
    
    if not json_files:
        print(f"⚠ Warning: No JSON files found in {folder_path}")
        return
    
    print(f"\nFound {len(json_files)} JSON file(s) to import")
    print("=" * 60)
    
    successful_imports = 0
    
    for json_file in json_files:
        result = import_json_file(conn, json_file)
        if result > 0:
            successful_imports += 1
    
    print("\n" + "=" * 60)
    print(f"✓ Import complete!")
    print(f"  Documents imported: {successful_imports}/{len(json_files)}")


def print_sample_queries():
    """Print some useful SQL query examples."""
    print("\n" + "=" * 60)
    print("USEFUL QUERIES:")
    print("=" * 60)
    print("""
        -- Get all data for a specific document:
        SELECT * FROM assessment_results WHERE document_id = '0a65f6b3176b4f43888238c21148c680';

        -- Get assessment with pretty JSON categories:
        SELECT document_id, 
            filename,
            assessment,
            risk_ratio,
            jsonb_pretty(categories) 
        FROM assessment_results;

        -- Find high-risk assessments:
        SELECT document_id, 
            filename, 
            risk_ratio,
            total_risk_score,
            max_total_risk_points
        FROM assessment_results
        WHERE risk_ratio > 0.1
        ORDER BY risk_ratio DESC;

        -- Find non-plausible assessments:
        SELECT document_id, 
            filename,
            is_plausible,
            number_of_ko_questions_answered_no
        FROM assessment_results
        WHERE is_plausible = false;

        -- Query specific categories:
        SELECT document_id,
            c->>'category_name' as category,
            (c->>'risk_ratio')::decimal as category_risk
        FROM assessment_results, 
            jsonb_array_elements(categories) as c
        WHERE document_id = '0a65f6b3176b4f43888238c21148c680';

        -- Find documents with high risk in specific category:
        SELECT document_id, filename
        FROM assessment_results, 
            jsonb_array_elements(categories) as c
        WHERE c->>'category_name' = 'Completeness'
        AND (c->>'risk_ratio')::decimal > 0.2;

        -- Summary statistics:
        SELECT 
            COUNT(*) as total_assessments,
            AVG(risk_ratio) as avg_risk_ratio,
            COUNT(*) FILTER (WHERE is_plausible = false) as non_plausible_count,
            MAX(risk_ratio) as max_risk_ratio
        FROM assessment_results;

        -- Join with questions table:
        SELECT 
            a.document_id,
            a.filename,
            a.risk_ratio,
            q.total_actual_risk_points
        FROM assessment_results a
        JOIN questions q ON a.document_id = q.document_id
        WHERE a.risk_ratio > 0.05;
    """)


def main():
    """Main execution function."""
    print("Analytics JSON to PostgreSQL Importer")
    print("=" * 60)
    
    conn = None
    try:
        # Connect to database
        conn = connect_to_database()
        
        # Create table if not exists
        create_table(conn)
        
        # Import all JSON files
        import_all_json_files(conn, JSON_FOLDER)
        
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