import asyncio
import base64
import json
import ssl
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import aiohttp
from decouple import config

from .get_token import get_ovb_access_token

DEFAULT_UPLOAD_URL = "https://api-lc-test.ovb.eu/api/v1/dataimport/jsonimport"
DEFAULT_UPLOAD_DIRECTORY = "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/ovb_import/upload_files"
DEFAULT_SUMMARY_DIRECTORY = "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/ovb_import/upload_summary"
DEFAULT_UPLOAD_DELAY_SECONDS = 5.0
SUMMARY_FILE_TEMPLATE = "upload_summary_{timestamp}.json"


def decode_jwt(token: str) -> dict:
    """Decode JWT token to see its contents (for debugging)"""
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return {}
        
        payload = parts[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += '=' * padding
        
        decoded = base64.b64decode(payload)
        return json.loads(decoded)
    except Exception as e:
        print(f"Could not decode token: {e}")
        return {}


async def upload_document(
    session: aiohttp.ClientSession,
    path_to_file: Path,
    access_token: str,
    upload_url: str,
) -> Dict:
    """Upload a JSON document to the OVB API"""
    if not path_to_file.exists():
        raise FileNotFoundError(f"File not found: {path_to_file}")
    
    with open(path_to_file, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    json_content = json.dumps(json_data, ensure_ascii=False)
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    print(f"Uploading file: {path_to_file.name}")
    print(f"Upload URL: {upload_url}")
    print(f"Token: {access_token[:20]}...")
    print(f"JSON size: {len(json_content)} bytes")
    print(f"Document ID: {json_data.get('document_id', 'N/A')}")
    
    async with session.post(upload_url, data=json_content, headers=headers) as response:
        status = response.status
        response_text = await response.text()
        
        print(f"\nResponse Status: {status}")
        print(f"Response Headers: {dict(response.headers)}")
        print(f"Response Body: {response_text}")
        
        if status >= 400:
            raise RuntimeError(f"Upload failed: {status} {response_text}")
        
        try:
            return await response.json()
        except:
            return {"status": status, "response": response_text}


def _parse_candidate_urls() -> List[str]:
    """Collect candidate upload endpoints while preserving order."""
    unique: List[str] = []

    def _add(values: Iterable[str]) -> None:
        for value in values:
            candidate = value.strip()
            if candidate and candidate not in unique:
                unique.append(candidate)

    configured_list = config("OVB__UPLOAD_API_URLS", default="")
    if configured_list:
        separators = [",", ";", " "]
        temp = [configured_list]
        for separator in separators:
            temp = [fragment for item in temp for fragment in item.split(separator)]
        _add(temp)

    configured_single = config("OVB__UPLOAD_API_URL", default="")
    if configured_single:
        _add([configured_single])

    _add([DEFAULT_UPLOAD_URL])
    return unique


async def select_upload_endpoint(session: aiohttp.ClientSession) -> str:
    """Probe candidate endpoints and return the first one that responds."""
    candidates = _parse_candidate_urls()
    if not candidates:
        raise RuntimeError("No upload endpoint candidates found.")

    acceptable_status = {200, 201, 202, 204, 301, 302, 307, 308, 401, 403, 405, 415}

    for candidate in candidates:
        print(f"Probing upload endpoint: {candidate}")
        for method in ("head", "options", "get"):
            try:
                async with session.request(method, candidate, allow_redirects=True) as response:
                    status = response.status
                    print(f"  {method.upper()} -> {status}")
                    if status in acceptable_status:
                        print(f"Using upload endpoint: {candidate} (probe {method.upper()} -> {status})\n")
                        return candidate
            except aiohttp.ClientError as exc:
                print(f"  {method.upper()} failed: {exc}")

        print(f"Endpoint probe failed: {candidate}\n")

    raise RuntimeError("Could not validate any configured upload endpoint.")


def resolve_upload_directory() -> Path:
    """Return the directory containing files to upload."""
    directory = Path(
        config("OVB__UPLOAD_DIRECTORY", default=DEFAULT_UPLOAD_DIRECTORY)
    ).expanduser().resolve()

    if not directory.is_dir():
        raise NotADirectoryError(f"Upload directory not found: {directory}")

    return directory


def resolve_upload_delay() -> float:
    """Return the delay between uploads in seconds."""
    delay_raw = config("OVB__UPLOAD_DELAY_SECONDS", default=str(DEFAULT_UPLOAD_DELAY_SECONDS))
    try:
        delay_value = float(delay_raw)
    except ValueError as exc:
        raise ValueError("OVB__UPLOAD_DELAY_SECONDS must be a numeric value") from exc

    return max(delay_value, 0.0)


def write_summary(upload_dir: Path, upload_url: str, results: List[Dict]) -> Path:
    """Persist a summary of the upload run in the upload directory."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_directory = Path(
        config("OVB__SUMMARY_DIRECTORY", default=DEFAULT_SUMMARY_DIRECTORY)
    ).expanduser().resolve()
    summary_directory.mkdir(parents=True, exist_ok=True)
    summary_path = summary_directory / SUMMARY_FILE_TEMPLATE.format(timestamp=timestamp)

    total = len(results)
    successes = sum(1 for item in results if item["status"] == "success")
    failures = total - successes

    summary_payload = {
        "generated_at": datetime.now().isoformat(),
        "upload_url": upload_url,
        "total_files": total,
        "successful_uploads": successes,
        "failed_uploads": failures,
        "results": results,
    }

    with open(summary_path, "w", encoding="utf-8") as summary_file:
        json.dump(summary_payload, summary_file, indent=2, ensure_ascii=False)

    print(f"Summary written to: {summary_path}")
    return summary_path


async def upload_directory(
    session: aiohttp.ClientSession,
    upload_dir: Path,
    upload_url: str,
    access_token: str,
    delay_seconds: float,
) -> List[Dict]:
    """Sequentially upload all JSON files in the directory."""
    files = sorted(upload_dir.glob("*.json"))

    if not files:
        print(f"No JSON files found in directory: {upload_dir}")
        return []

    print(f"Found {len(files)} file(s) to upload in {upload_dir}\n")

    results: List[Dict] = []
    for index, file_path in enumerate(files):
        try:
            response = await upload_document(session, file_path, access_token, upload_url)
            results.append(
                {
                    "status": "success",
                    "file": file_path.name,
                    "response": response,
                }
            )
            print(f"✅ Upload succeeded: {file_path.name}\n")
        except Exception as exc:
            results.append(
                {
                    "status": "error",
                    "file": file_path.name,
                    "error": str(exc),
                }
            )
            print(f"❌ Upload failed: {file_path.name} -> {exc}\n")

        if index < len(files) - 1 and delay_seconds > 0:
            print(f"Waiting {delay_seconds} second(s) before next upload...\n")
            await asyncio.sleep(delay_seconds)

    return results


async def main():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            upload_dir = resolve_upload_directory()
            delay_seconds = resolve_upload_delay()

            print("Step 1: Getting access token...")
            access_token = await get_ovb_access_token(session)
            print(f"✅ Token received: {access_token[:30]}...\n")

            token_payload = decode_jwt(access_token)
            print("Token payload:")
            print(f"  Issuer: {token_payload.get('iss')}")
            print(f"  Subject: {token_payload.get('sub')}")
            print(f"  Scopes: {token_payload.get('scope')}")
            print(f"  Client ID: {token_payload.get('azp') or token_payload.get('client_id')}")
            print(f"  Expires: {token_payload.get('exp')}")
            print(f"  Full payload: {json.dumps(token_payload, indent=2)}\n")

            print("Step 2: Resolving upload endpoint...")
            upload_url = await select_upload_endpoint(session)

            print("Step 3: Uploading documents...")
            results = await upload_directory(
                session=session,
                upload_dir=upload_dir,
                upload_url=upload_url,
                access_token=access_token,
                delay_seconds=delay_seconds,
            )

            summary_path = write_summary(upload_dir, upload_url, results)

            print("\nUpload run completed.")
            print(f"Total files: {len(results)}")
            print(f"Summary file: {summary_path}")

        except Exception as e:
            print(f"\n❌ Error: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(main())