"""Upload multiple files sequentially using the single-file upload logic.

Run as module:
    python -m api_showcase.rest_importer.upload_files

Environment variables expected: STAGE_UPLOAD_URL / PROD_UPLOAD_URL plus ORGANIZATION_ID, auth vars.
"""
import asyncio
import os
import json
from typing import List
from decouple import config

from .auth import get_token
from .upload_file import upload_file, UploadResult


workflow = "/imd_check"  # Will be supplied via query params by upload_file
scope = "production"
ENVIRONMENT = config("ENVIRONMENT", default="prod").lower()
upload_url = config("PROD_UPLOAD_URL") if ENVIRONMENT == "prod" else config("PROD_UPLOAD_URL", default=config("PROD_UPLOAD_URL"))


async def upload_files_from_folder(
    folder_path: str,
    access_token: str,
    scope: str = scope.lower(),
    workflow: str = workflow,
    upload_url: str = upload_url,
    metadata: dict = None
) -> List[UploadResult]:
    """
    Upload all files from a specified folder one by one.
    
    Args:
        folder_path: Path to the folder containing files to upload
        access_token: Authentication token
        scope: Scope for the upload (default: production)
        workflow: Workflow path (default: /imd)
        upload_url: URL endpoint for uploads
        metadata: Optional metadata to attach to all uploads
    
    Returns:
        List of UploadResult objects for each file
    """
    if not os.path.exists(folder_path):
        print(f"❌ Folder not found: {folder_path}")
        return []
    
    if not os.path.isdir(folder_path):
        print(f"❌ Path is not a directory: {folder_path}")
        return []
    
    # Get all files in the folder (excluding subdirectories)
    files = [
        os.path.join(folder_path, f) 
        for f in os.listdir(folder_path) 
        if os.path.isfile(os.path.join(folder_path, f))
    ]
    
    if not files:
        print(f"⚠️ No files found in folder: {folder_path}")
        return []
    
    print(f"📁 Found {len(files)} file(s) to upload from: {folder_path}")
    print(f"➡️ Using environment: {ENVIRONMENT} | upload_url={upload_url}")
    print("-" * 60)
    
    results = []
    
    # Ensure workflow has leading slash (upload_file also normalizes, but we keep it explicit here)
    wf = workflow if workflow.startswith('/') else f"/{workflow}"

    for i, file_path in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] Uploading: {os.path.basename(file_path)}")
        
        result = await upload_file(
            access_token=access_token,
            file_path=file_path,
            scope=scope,
            workflow=wf,
            metadata=metadata,
            upload_url=upload_url,
            metadata_as_file=True
        )
        
        results.append(result)
        
        # Small delay between uploads to avoid overwhelming the server
        if i < len(files):
            await asyncio.sleep(0.5)
    
    return results


def print_summary(results: List[UploadResult]):
    """Print a summary of all upload results."""
    print("\n" + "=" * 60)
    print("📊 UPLOAD SUMMARY")
    print("=" * 60)
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    
    print(f"\n✅ Successful uploads: {len(successful)}/{len(results)}")
    print(f"❌ Failed uploads: {len(failed)}/{len(results)}")
    
    if successful:
        total_duration = sum(r.duration_ms for r in successful if r.duration_ms)
        avg_duration = total_duration / len(successful) if successful else 0
        print(f"⏱️  Average upload time: {avg_duration:.0f}ms")
    
    if failed:
        print("\n❌ Failed Files:")
        for result in failed:
            print(f"  - {os.path.basename(result.file_path)}: {result.error}")
    
    print("\n" + "=" * 60)


async def main():
    # Get access token
    access_token = await get_token()
    
    # Specify the folder path
    # Resolve folder path relative to this script's directory for reliability
    base_dir = os.path.dirname(__file__)
    folder_path = os.path.join(base_dir, "test_documents/1_1_IMD")
    
    # Optional: Add metadata for all uploads
    metadata = {
        "batch_upload": True,
        "uploaded_at": "2025-10-28"
    }
    print(f"Starting batch upload; environment={ENVIRONMENT}")
    
    # Upload all files from the folder
    results = await upload_files_from_folder(
        folder_path=folder_path,
        access_token=access_token,
        scope=scope,
        workflow=workflow,
        metadata=metadata
    )
    
    # Print summary
    print_summary(results)
    
    # Optionally, save results to a JSON file
    output_file = "upload_results.json"
    with open(output_file, "w") as f:
        json.dump([
            {
                "file_path": r.file_path,
                "success": r.success,
                "document_id": r.document_id,
                "status": r.status,
                "error": r.error,
                "duration_ms": r.duration_ms
            }
            for r in results
        ], f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())