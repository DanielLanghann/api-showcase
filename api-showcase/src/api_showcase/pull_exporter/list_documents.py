from decouple import config
import aiohttp
import asyncio

from ..rest_importer.auth import get_token

# API docs list scope values with leading capital letters:
# Production, Development, Testing, Healthcheck, Training
# We normalize any user/env-provided scope to the expected casing to avoid 422.
ALLOWED_SCOPES = {
    "production": "Production",
    "development": "Development",
    "testing": "Testing",
    "healthcheck": "Healthcheck",
    "training": "Training",
}

default_scope = config("DEFAULT_SCOPE", default="Production")
org_id = config("ORGANIZATION_ID")
document_list_url = config("STAGE_DOCUMENTS_LIST_URL")



async def list_documents(access_token, scope: str = default_scope, document_class_regex=None):
    # Normalize scope casing if provided in lowercase
    normalized_scope = ALLOWED_SCOPES.get(str(scope).lower(), scope)
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    params = {"organization_id": org_id, "scope": normalized_scope}
    params.update({"document_class_regex": document_class_regex} if document_class_regex else {})
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(document_list_url, headers=headers, params=params) as response:
                print(f"Status: {response.status}")
                print(f"URL: {response.url}")
                
                if response.status != 200:
                    # Print response body (attempt JSON first) for debugging
                    raw_text = await response.text()
                    try:
                        as_json = await response.json()
                        print(f"Error JSON: {as_json}")
                    except Exception:
                        print(f"Response body (text): {raw_text}")
                
                response.raise_for_status()
                
                documents = await response.json()
                print(f"Response: {documents}")
                return documents
                
        except aiohttp.ClientError as e:
            print(f"Failed to list documents: {e}")
            if hasattr(e, 'status'):
                print(f"Status: {e.status}")
            raise

async def main():
    token = await get_token()
    # Call with default scope (will be normalized) unless overridden.
    documents = await list_documents(access_token=token)
    print(f"Fetched {len(documents)} document id(s)")

if __name__ == "__main__":
    asyncio.run(main())
