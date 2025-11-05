
from decouple import config
import asyncio
import os
import json
from datetime import datetime
from ..rest_importer.auth import get_token

from ..pull_exporter.list_documents import list_documents
from ..pull_exporter.get_document_data import get_documents_by_ids
from .calculate_risk_scores import analyze_document
from .create_csv_report import create_csv_report


# config
org_id = config("ORGANIZATION_ID")
scope = config("DEFAULT_SCOPE")
email = config("EMAIL")
password = config("PROD_PASSWORD")
auth_url = config("PROD_AUTH_URL")

async def main():
    try:
        # Step 1: Get token
        print("Step 1: Authenticating...")
        token = await get_token(email=email, password=password, org_id=org_id, auth_url=auth_url)
        print("✓ Authentication successful")
        
        # Step 2: Get available documents
        print("\nStep 2: Fetching available documents...")
        documents = await list_documents(access_token=token, scope=scope)
        print(f"✓ Fetched {len(documents)} document(s)")
        
        if not documents:
            print("No documents found. Exiting.")
            return
        
        # Extract document IDs from the documents list
        # Assuming documents is a list of dicts with 'document_id' field
        # Adjust the key name if your API returns a different structure
        document_ids = []
        for doc in documents:
            if isinstance(doc, dict):
                doc_id = doc.get('document_id') or doc.get('id')
                if doc_id:
                    document_ids.append(doc_id)
            elif isinstance(doc, str):
                document_ids.append(doc)
        
        if not document_ids:
            print("No valid document IDs found. Exiting.")
            return
            
        print(f"Extracted {len(document_ids)} document ID(s)")
        
        # Step 3: Get document data for all documents
        print("\nStep 3: Fetching document data for all documents...")
        output_directory = "/Users/daniellanghann/src/api-showcase/api-showcase/document_data"
        
        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        print(f"✓ Output directory ready: {output_directory}")
        
        # Fetch all documents concurrently
        from ..pull_exporter.get_document_data import get_documents_by_ids
        
        results = await get_documents_by_ids(
            access_token=token,
            document_ids=document_ids,
            scope=scope,
            org_id=org_id,
            document_details_url=config("PROD_DOCUMENT_DETAILS_URL"),
            print_results=False,  # Set to False to reduce console output
            path_to_result_file=f"{output_directory}/results.json",
            max_concurrent=5  # Adjust based on API rate limits
        )
        
        # Print final summary
        print("\n" + "="*50)
        print("FINAL SUMMARY")
        print("="*50)
        successful = [doc_id for doc_id, data in results.items() if data.get("status") == "success"]
        failed = [doc_id for doc_id, data in results.items() if data.get("status") == "error"]
        
        print(f"Total documents processed: {len(document_ids)}")
        print(f"✓ Successful: {len(successful)}")
        if failed:
            print(f"✗ Failed: {len(failed)}")
            print(f"  Failed IDs: {', '.join(failed[:5])}{'...' if len(failed) > 5 else ''}")
        
        print(f"\nAll document data saved to: {output_directory}")
        
        # Step 4: Generate analytics for all documents
        print("\n" + "="*50)
        print("Step 4: Generating analytics for all documents...")
        print("="*50)
        
        results_directory = "/Users/daniellanghann/src/api-showcase/api-showcase/results"
        os.makedirs(results_directory, exist_ok=True)
        print(f"✓ Results directory ready: {results_directory}")
        
        # Get all JSON files from the document_data directory
        json_files = [f for f in os.listdir(output_directory) if f.endswith('.json')]
        print(f"Found {len(json_files)} document data file(s) to analyze")
        
        analytics_success = 0
        analytics_failed = 0
        
        for json_file in json_files:
            try:
                file_path = os.path.join(output_directory, json_file)
                
                # Load document data
                with open(file_path, 'r', encoding='utf-8') as f:
                    document_data = json.load(f)
                
                # Run analytics
                analytics_result = analyze_document(data=document_data)
                
                # Extract document ID for filename
                doc_id = analytics_result.get('document_id', 'unknown')
                
                # Create output filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"analytics_{doc_id}_{timestamp}.json"
                output_path = os.path.join(results_directory, output_filename)
                
                # Save analytics result
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(analytics_result, f, indent=2, ensure_ascii=False)
                
                print(f"✓ Analytics saved: {output_filename}")
                analytics_success += 1
                
            except Exception as e:
                print(f"✗ Failed to analyze {json_file}: {e}")
                analytics_failed += 1
        
        # Final analytics summary
        print("\n" + "="*50)
        print("ANALYTICS SUMMARY")
        print("="*50)
        print(f"Documents analyzed: {len(json_files)}")
        print(f"✓ Successful: {analytics_success}")
        if analytics_failed > 0:
            print(f"✗ Failed: {analytics_failed}")
        print(f"\nAll analytics saved to: {results_directory}")
        
        # Step 5: Generate CSV reports for all documents
        print("\n" + "="*50)
        print("Step 5: Generating CSV reports for all documents...")
        print("="*50)
        
        csv_reports_directory = "/Users/daniellanghann/src/api-showcase/api-showcase/csv_reports"
        os.makedirs(csv_reports_directory, exist_ok=True)
        print(f"✓ CSV reports directory ready: {csv_reports_directory}")
        
        csv_success = 0
        csv_failed = 0
        
        # Re-scan the document_data directory for CSV generation
        for json_file in json_files:
            try:
                file_path = os.path.join(output_directory, json_file)
                
                # Load document data
                with open(file_path, 'r', encoding='utf-8') as f:
                    document_data = json.load(f)
                
                # Extract document ID for filename
                doc_id = document_data.get('document', {}).get('document_id', 'unknown')
                
                # Create CSV filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_filename = f"report_{doc_id}_{timestamp}.csv"
                csv_output_path = os.path.join(csv_reports_directory, csv_filename)
                
                # Generate CSV report
                create_csv_report(data=document_data, output_filename=csv_output_path)
                
                print(f"✓ CSV report saved: {csv_filename}")
                csv_success += 1
                
            except Exception as e:
                print(f"✗ Failed to create CSV for {json_file}: {e}")
                csv_failed += 1
        
        # Final CSV summary
        print("\n" + "="*50)
        print("CSV REPORTS SUMMARY")
        print("="*50)
        print(f"CSV reports generated: {len(json_files)}")
        print(f"✓ Successful: {csv_success}")
        if csv_failed > 0:
            print(f"✗ Failed: {csv_failed}")
        print(f"\nAll CSV reports saved to: {csv_reports_directory}")
        
    except Exception as e:
        print(f"\n✗ Error in main execution: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())