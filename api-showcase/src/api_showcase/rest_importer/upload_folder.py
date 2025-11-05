import asyncio
import aiohttp
import os
import json
from decouple import config
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime, timezone

from .auth import get_token


workflow = "/"  # default root workflow; will be added as query param if provided
path_to_folder = "./test_documents/"
upload_url = config("STAGE_UPLOAD_FOLDER_URL")
organization_id = config("ORGANIZATION_ID", default="ovb")

email = config("EMAIL")
password = config("STAGE_PASSWORD")

scope = "production"

@dataclass
class FolderUploadResult:
    folder_path: str
    success: bool
    uploaded_files: List[str]
    failed_files: List[str]
    status: Optional[int] = None
    error: Optional[str] = None
    duration_ms: Optional[int] = None
    response_data: Optional[dict] = None

async def upload_folder(
    access_token: str,
    folder_path: str,
    scope: str = scope.lower(),
    workflow: Optional[str] = None,
    document_id: Optional[str] = None,
    metadata: Optional[dict] = None,
    non_interactive: Optional[bool] = None,
    never_retry: Optional[bool] = None,
    retention_after_creation: Optional[str] = None,
    retention_after_finished: Optional[str] = None,
    file_extensions: Optional[List[str]] = None,
    upload_url: str = upload_url
) -> FolderUploadResult:
    print(f"📁 Starting Folder Upload from: {folder_path}")
    
    if not os.path.exists(folder_path):
        return FolderUploadResult(
            folder_path=folder_path,
            success=False,
            uploaded_files=[],
            failed_files=[],
            error=f"Folder not found: {folder_path}"
        )
    
    if not os.path.isdir(folder_path):
        return FolderUploadResult(
            folder_path=folder_path,
            success=False,
            uploaded_files=[],
            failed_files=[],
            error=f"Path is not a directory: {folder_path}"
        )
    
    # Get all files from the folder
    folder = Path(folder_path)
    all_files = [f for f in folder.iterdir() if f.is_file()]

    # Filter by extensions if provided
    if file_extensions:
        file_extensions_lower = [ext.lower() if ext.startswith('.') else f'.{ext.lower()}' 
                                for ext in file_extensions]
        all_files = [f for f in all_files if f.suffix.lower() in file_extensions_lower]

    if not all_files:
        return FolderUploadResult(
            folder_path=folder_path,
            success=False,
            uploaded_files=[],
            failed_files=[],
            error="No files found in folder"
        )
    
    print(f"📄 Found {len(all_files)} file(s) to upload")

    # Build query params (API expects these rather than multipart form fields)
    normalized_workflow = None
    if workflow:
        normalized_workflow = workflow if workflow.startswith('/') else f"/{workflow}"

    params = {"organization_id": organization_id}
    if scope:
        params["scope"] = scope
    if normalized_workflow:
        params["workflow"] = normalized_workflow
    if document_id:
        params["document_id"] = document_id
    if non_interactive is not None:
        params["non_interactive"] = str(non_interactive).lower()
    if never_retry is not None:
        params["never_retry"] = str(never_retry).lower()
    if retention_after_creation:
        params["retention_after_creation"] = retention_after_creation
    if retention_after_finished:
        params["retention_after_finished"] = retention_after_finished
    
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    start = datetime.now(timezone.utc)

    try:
        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()  # body will only contain files + metadata + per-file mapping
            
            # Add all files to the form data
            per_file_ids = {}
            for file_path in all_files:
                try:
                    with open(file_path, "rb") as f:
                        file_content = f.read()
                        file_name = file_path.name
                        # Add each file with the field name 'files'
                        data.add_field(
                            "files",
                            file_content,
                            filename=file_name,
                            content_type="application/octet-stream"
                        )
                        # Derive document_id for each file (basename without extension)
                        derived_id = file_path.stem
                        per_file_ids[file_name] = derived_id
                        print(f"  📎 Added: {file_name}")
                except Exception as e:
                    print(f"  ⚠️ Failed to read file {file_path.name}: {e}")
                    continue
            
            # Attach per-file document IDs mapping as JSON file (if any)
            if per_file_ids:
                mapping_json = json.dumps({"document_ids": per_file_ids})
                data.add_field(
                    "document_ids",
                    mapping_json,
                    filename="document_ids.json",
                    content_type="application/json"
                )

            # Add metadata if provided; server complained expecting UploadFile so send as file part
            if metadata:
                metadata_json = json.dumps(metadata)
                data.add_field(
                    "metadata",
                    metadata_json,
                    filename="metadata.json",
                    content_type="application/json"
                )
            print("Form & query prepared:")
            print(f"  query params: {params}")
            print(f"  per_file_ids (mapping file): {per_file_ids}")
            if metadata:
                print(f"  metadata keys={list(metadata.keys())}")
            
            # Make the request
            async with session.post(upload_url, headers=headers, params=params, data=data) as response:
                try:
                    print(f"🌐 POST {response.url}")
                except Exception:
                    pass
                status = response.status
                raw_text = None
                try:
                    raw_text = await response.text()
                except Exception:
                    raw_text = "<unable to read body>"
                
                duration_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
                
                if status < 300:
                    try:
                        response_data = json.loads(raw_text) if raw_text not in (None, "") else await response.json()
                        uploaded_file_names = [f.name for f in all_files]
                        print(f"✅ Folder upload succeeded! Uploaded {len(uploaded_file_names)} file(s)")
                        return FolderUploadResult(
                            folder_path=folder_path,
                            success=True,
                            uploaded_files=uploaded_file_names,
                            failed_files=[],
                            status=status,
                            duration_ms=duration_ms,
                            response_data=response_data
                        )
                    except Exception as e:
                        print(f"⚠️ Upload succeeded but JSON parsing failed: {e}")
                        uploaded_file_names = [f.name for f in all_files]
                        return FolderUploadResult(
                            folder_path=folder_path,
                            success=True,
                            uploaded_files=uploaded_file_names,
                            failed_files=[],
                            status=status,
                            duration_ms=duration_ms
                        )
                else:
                    body = raw_text or "<no body>"
                    print(f"❌ Folder upload failed: HTTP {status} | Body: {body[:500]}")
                    failed_file_names = [f.name for f in all_files]
                    return FolderUploadResult(
                        folder_path=folder_path,
                        success=False,
                        uploaded_files=[],
                        failed_files=failed_file_names,
                        status=status,
                        error=f"HTTP {status}: {body}",
                        duration_ms=duration_ms
                    )
    except aiohttp.ClientError as e:
        duration_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        failed_file_names = [f.name for f in all_files]
        return FolderUploadResult(
            folder_path=folder_path,
            success=False,
            uploaded_files=[],
            failed_files=failed_file_names,
            error=f"Client error: {str(e)}",
            duration_ms=duration_ms
        )
    
    except Exception as e:
        duration_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        failed_file_names = [f.name for f in all_files]
        return FolderUploadResult(
            folder_path=folder_path,
            success=False,
            uploaded_files=[],
            failed_files=failed_file_names,
            error=f"Unexpected error: {str(e)}",
            duration_ms=duration_ms
        )
    
async def main():
    access_token = await get_token()
    
    # Example: Upload all files from a folder
    # Resolve folder path relative to this script if given as relative
    default_rel = Path(__file__).parent / "test_documents"
    folder_path = str(default_rel)
    result = await upload_folder(
        access_token=access_token,
        folder_path=folder_path,
        scope=scope,
        workflow=workflow,
        file_extensions=['.pdf'],  # Optional: filter by extension
        metadata={"batch": "test_batch_001"}  # Optional metadata
    )
    
    # Pretty print result
    print("\n=== Folder Upload Result ===")
    print(json.dumps({
        "folder_path": result.folder_path,
        "success": result.success,
        "uploaded_files": result.uploaded_files,
        "failed_files": result.failed_files,
        "status": result.status,
        "error": result.error,
        "duration_ms": result.duration_ms,
        "response_data": result.response_data
    }, indent=2))
                    
if __name__ == "__main__":
    asyncio.run(main())    




    

    

    
    

