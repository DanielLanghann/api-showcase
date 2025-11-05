from decouple import config
import asyncio
import aiohttp
import ssl

TOKEN_URL = "https://sso-test.ovb.eu/auth/realms/ovb/protocol/openid-connect/token"
CLIENT_ID = config("OVB__CLIENT_ID") 
CLIENT_SECRET = config("OVB__CLIENT_SECRET")
SCOPE = config("OVB__SCOPE")
GRANT_TYPE = config("OVB__GRANT_TYPE")

async def get_ovb_access_token(session: aiohttp.ClientSession) -> str:
    auth = aiohttp.BasicAuth(CLIENT_ID, CLIENT_SECRET)
    data = {
        "grant_type": GRANT_TYPE,
        "scope": SCOPE
    }
    
    async with session.post(TOKEN_URL, data=data, auth=auth) as response:
        text = await response.text()
        print(f"\nResponse Status: {response.status}")
        print(f"Response: {text}")
        
        if response.status >= 400:
            raise RuntimeError(f"Token request failed: {response.status} {text}")
        
        payload = await response.json()
        return payload["access_token"]

async def main():
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    session = aiohttp.ClientSession(connector=connector)
    
    try:
        token = await get_ovb_access_token(session)
        print(f"\n✅ Success!")
        print(f"Access Token: {token}")
        print(f"Token length: {len(token)}")
    finally:
        await session.close()

if __name__ == "__main__":
    asyncio.run(main())