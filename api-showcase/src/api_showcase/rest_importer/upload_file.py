
import asyncio
import aiohttp
import os
import json
from decouple import config

from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone

from .auth import get_token


workflow = "/imd_check"  # default workflow path; will be sent as query param if provided
path_to_folder = "./test_documents/"
upload_url = config("PROD_UPLOAD_URL")
organization_id = config("ORGANIZATION_ID", default="ovb")  

email = config("EMAIL")
password = config("PROD_PASSWORD")

scope = "production"

def get_document_id_from_path(file_path):
    return os.path.splitext(os.path.basename(file_path))[0]


@dataclass
class UploadResult:
    file_path: str
    success: bool
    document_id: Optional[str] = None
    status: Optional[int] = None
    error: Optional[str] = None
    duration_ms: Optional[int] = None

async def upload_file(
    access_token: str,
    file_path: str,
    scope: str = scope.lower(),
    workflow: Optional[str] = None,
    document_id: Optional[str] = None,
    metadata: Optional[dict] = None,
    upload_url: str = upload_url,
    metadata_as_file: bool = False,
) -> UploadResult:
    print("📃 Starting File Upload")
    if not os.path.exists(file_path):
        return UploadResult(
            file_path=file_path,
            success=False,
            error=f"File not found: {file_path}"
        )

    # Build query params (API expects scope/workflow/document_id here, not in multipart body)
    resolved_document_id = document_id or get_document_id_from_path(file_path=file_path)
    normalized_workflow = None
    if workflow:
        # Ensure workflow starts with a leading slash (API examples show '/root')
        normalized_workflow = workflow if workflow.startswith('/') else f"/{workflow}"

    params = {
        "organization_id": organization_id,
        "scope": scope,
        "document_id": resolved_document_id,
    }
    if normalized_workflow:
        params["workflow"] = normalized_workflow
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    start = datetime.now(timezone.utc)

    try:
        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            
            with open(file_path, "rb") as f:
                file_content = f.read()
                file_name = os.path.basename(file_path)
                data.add_field("file", file_content, filename=file_name, content_type="application/octet-stream")
            # Only file and (optionally) metadata stay in multipart body; others moved to query params

            # Add metadata if provided
            if metadata:
                metadata_json = json.dumps(metadata)
                if metadata_as_file:
                    data.add_field('metadata', metadata_json, filename='metadata.json', content_type='application/json')
                else:
                    data.add_field('metadata', metadata_json, content_type='application/json')

            async with session.post(upload_url, headers=headers, params=params, data=data) as response:
                # Debug: show the final resolved URL once (aiohttp Response has .url)
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
                        uploaded_doc_id = response_data.get("document_id", "unknown")
                        print(f"✅ Upload succeeded for {file_path} -> document_id={uploaded_doc_id}")
                        return UploadResult(
                            file_path=file_path,
                            success=True,
                            document_id=uploaded_doc_id,
                            status=status,
                            duration_ms=duration_ms
                        )
                    except Exception:
                        print("⚠️ Upload succeeded but JSON parsing failed; returning without document_id")
                        return UploadResult(
                            file_path=file_path,
                            success=True,
                            status=status,
                            duration_ms=duration_ms
                        )
                else:
                    body = raw_text or "<no body>"
                    print(f"❌ Upload failed: HTTP {status} | Body: {body[:500]}")
                    return UploadResult(
                        file_path=file_path,
                        success=False,
                        status=status,
                        error=f"HTTP {status}: {body}",
                        duration_ms=duration_ms
                    )
    
    except aiohttp.ClientError as e:
        duration_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        return UploadResult(
            file_path=file_path,
            success=False,
            error=str(e),
            duration_ms=duration_ms
        )
    except Exception as e:
        duration_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
        return UploadResult(
            file_path=file_path,
            success=False,
            error=f"Unexpected error: {str(e)}",
            duration_ms=duration_ms
        )
    
async def main():
    access_token = await get_token()
    file_path = "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/rest_importer/test_documents/82101.pdf"
    result = await upload_file(access_token=access_token, file_path=file_path, scope=scope, workflow=workflow)
    # Pretty print result
    print("\n=== Upload Result ===")
    print(json.dumps({
        "file_path": result.file_path,
        "success": result.success,
        "document_id": result.document_id,
        "status": result.status,
        "error": result.error,
        "duration_ms": result.duration_ms
    }, indent=2))

if __name__ == "__main__":
    asyncio.run(main())