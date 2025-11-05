import asyncio
import aiohttp
import ssl
import json
import base64
from pathlib import Path
from decouple import config
from .get_token import get_ovb_access_token

PATH = "/Users/daniellanghann/src/api-showcase/api-showcase/src/api_showcase/ovb_import/fde2472ff7134da7b5aa41a1279f457c.json"
UPLOAD_URL = config("OVB__UPLOAD_API_URL", "https://api-lc-test.ovb.eu/api/v1/dataimport/jsonimport")


def decode_jwt(token: str) -> dict:
    """Decode JWT token to see its contents (for debugging)"""
    try:
        # JWT format: header.payload.signature
        parts = token.split('.')
        if len(parts) != 3:
            return {}
        
        # Decode payload (add padding if needed)
        payload = parts[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += '=' * padding
        
        decoded = base64.b64decode(payload)
        return json.loads(decoded)
    except Exception as e:
        print(f"Could not decode token: {e}")
        return {}


async def test_api_health(session: aiohttp.ClientSession):
    """Test if the API is accessible"""
    base_url = "https://api-lc-test.ovb.eu"
    
    test_endpoints = [
        "/",
        "/api",
        "/api/v1",
        "/api/v1/dataimport",
        "/health",
        "/api/health"
    ]
    
    print("Testing API endpoints...")
    for endpoint in test_endpoints:
        try:
            url = f"{base_url}{endpoint}"
            async with session.get(url) as response:
                print(f"  {url}: {response.status}")
        except Exception as e:
            print(f"  {url}: Error - {e}")
    print()


async def upload_document(session: aiohttp.ClientSession, path_to_file: str, access_token: str) -> dict:
    """Upload a JSON document to the OVB API"""
    file_path = Path(path_to_file)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {path_to_file}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        json_content = f.read()
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    print(f"Uploading file: {file_path.name}")
    print(f"Upload URL: {UPLOAD_URL}")
    print(f"Token: {access_token[:20]}...")
    
    async with session.post(UPLOAD_URL, data=json_content, headers=headers) as response:
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


async def main():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    session = aiohttp.ClientSession(connector=connector)
    
    try:
        # Step 1: Get access token
        print("Step 1: Getting access token...")
        access_token = await get_ovb_access_token(session)
        print(f"✅ Token received: {access_token[:30]}...\n")
        
        # Decode and inspect token
        token_payload = decode_jwt(access_token)
        print("Token payload:")
        print(f"  Issuer: {token_payload.get('iss')}")
        print(f"  Subject: {token_payload.get('sub')}")
        print(f"  Scopes: {token_payload.get('scope')}")
        print(f"  Client ID: {token_payload.get('azp') or token_payload.get('client_id')}")
        print(f"  Expires: {token_payload.get('exp')}")
        print(f"  Full payload: {json.dumps(token_payload, indent=2)}\n")

        # Step 1.5: Test API health
        await test_api_health(session)  # Fixed: Added await
        
        # Step 2: Upload document
        print("Step 2: Uploading document...")
        response = await upload_document(session, PATH, access_token)
        
        print(f"\n✅ Upload successful!")
        print(f"Response: {response}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())