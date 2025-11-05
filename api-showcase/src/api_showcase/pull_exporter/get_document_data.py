from decouple import config
import aiohttp
import asyncio
import os
import json
from datetime import datetime
from ..rest_importer.auth import get_token

email = config("EMAIL")
password = config("PROD_PASSWORD", default=config("PASSWORD_DEV", default=""))
auth_url = config("PROD_AUTH_URL")
document_details_url = config("PROD_DOCUMENT_DETAILS_URL")
org_id = "ovb"
scope = "production"
document_class = ""


async def get_document_by_id(
    access_token,
    scope: str = scope,
    document_id: str = None,
    document_class_regex=None,
    org_id: str = org_id,
    document_details_url: str = document_details_url,
    print_results: bool = True,
    path_to_result_file: str = None
):
    if document_id is None:
        raise ValueError("Document ID must be given!")
    
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    
    params = {"organization_id": org_id, "scope": scope}
    params.update({"document_class_regex": document_class_regex} if document_class_regex else {})
    
    url = document_details_url.replace(":document_id", document_id)
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url, headers=headers, params=params) as response:
                print(f"Document retrieval status: {response.status}")
                print(f"URL: {response.url}")
                response.raise_for_status()
                document_details = await response.json()
                
                # Print results if flag is set
                if print_results:
                    print(f"Document details for ID: {document_id}:\n")
                    print(document_details)
                
                # Save to file if path is provided
                if path_to_result_file:
                    try:
                        # Extract directory and create it if needed
                        directory = os.path.dirname(path_to_result_file)
                        if directory:
                            os.makedirs(directory, exist_ok=True)
                        
                        # Add timestamp to filename
                        current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        base_name = os.path.basename(path_to_result_file)
                        name, ext = os.path.splitext(base_name)
                        
                        # If no extension provided, default to .json
                        if not ext:
                            ext = '.json'
                        
                        # Create new filename with timestamp
                        timestamped_filename = f"{name}_{document_id}_{current_timestamp}{ext}"
                        final_path = os.path.join(directory, timestamped_filename) if directory else timestamped_filename
                        
                        # Save the file
                        with open(final_path, "w", encoding="utf-8") as f:
                            json.dump(document_details, f, indent=2, ensure_ascii=False)
                        print(f"Document data saved to: {final_path}")
                    except IOError as e:
                        print(f"Failed to save document data to file: {e}")
                
                return document_details
                
        except aiohttp.ClientError as e:
            print(f"Failed to get document {document_id}: {e}")
            if hasattr(e, "status"):
                print(f"Status: {e.status}")
            raise
        
async def get_documents_by_ids(
    access_token,
    document_ids: list[str],
    scope: str = scope,
    document_class_regex=None,
    org_id: str = org_id,
    document_details_url: str = document_details_url,
    print_results: bool = True,
    path_to_result_file: str = None,
    max_concurrent: int = 5
):
    """
    Fetch multiple documents by their IDs concurrently.
    
    Args:
        access_token: Authentication token
        document_ids: List of document IDs to fetch
        scope: Scope for the request
        document_class_regex: Optional document class filter
        org_id: Organization ID
        document_details_url: Base URL for document details
        print_results: Whether to print results to console
        path_to_result_file: Base path for saving results (will be modified per document)
        max_concurrent: Maximum number of concurrent requests (default: 5)
    
    Returns:
        Dictionary mapping document IDs to their details (or error info)
    """
    if not document_ids:
        raise ValueError("Document IDs list cannot be empty!")
    
    print(f"Processing {len(document_ids)} documents...")
    
    # Use a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def fetch_with_semaphore(doc_id):
        async with semaphore:
            try:
                result = await get_document_by_id(
                    access_token=access_token,
                    scope=scope,
                    document_id=doc_id,
                    document_class_regex=document_class_regex,
                    org_id=org_id,
                    document_details_url=document_details_url,
                    print_results=print_results,
                    path_to_result_file=path_to_result_file
                )
                return doc_id, {"status": "success", "data": result}
            except Exception as e:
                print(f"Error processing document {doc_id}: {e}")
                return doc_id, {"status": "error", "error": str(e)}
    
    # Create tasks for all documents
    tasks = [fetch_with_semaphore(doc_id) for doc_id in document_ids]
    
    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Convert results to dictionary
    results_dict = {}
    for result in results:
        if isinstance(result, Exception):
            print(f"Unexpected error: {result}")
        else:
            doc_id, doc_data = result
            results_dict[doc_id] = doc_data
    
    # Print summary
    success_count = sum(1 for v in results_dict.values() if v.get("status") == "success")
    error_count = len(results_dict) - success_count
    print(f"\n=== Summary ===")
    print(f"Total documents: {len(document_ids)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {error_count}")
    
    return results_dict



async def main():
    try:
        access_token = await get_token(email=email, password=password, org_id=org_id)
        
        # Example: Single document
        # document_id = "9fce5c4febdd4cd187240f088d2833f3"
        # path_to_result_file = "./results.json"
        # document_details = await get_document_by_id(
        #     access_token=access_token, 
        #     scope=scope, 
        #     document_id=document_id, 
        #     org_id=org_id, 
        #     document_details_url=document_details_url, 
        #     path_to_result_file=path_to_result_file
        # )
        
        # Example: Multiple documents
        document_ids = [
            "4ce07b0217e94a6a830461901a4f2a25",
        ]
        now = datetime.now()
        path_to_result_file = f"document_data/results.json"
        results = await get_documents_by_ids(
            access_token=access_token,
            document_ids=document_ids,
            scope=scope,
            org_id=org_id,
            document_details_url=document_details_url,
            path_to_result_file=path_to_result_file,
            print_results=False,  # Set to False to reduce console output for multiple docs
            max_concurrent=5
        )
        
        print(f"\nProcessed {len(results)} documents")

    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())