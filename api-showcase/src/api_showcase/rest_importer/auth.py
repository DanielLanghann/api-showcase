from decouple import config
import aiohttp
import asyncio


EMAIL = config("EMAIL")
PASSWORD = config("PROD_PASSWORD", default=config("DEV_PASSWORD", default=""))
AUTH_URL = config("PROD_AUTH_URL")
ORGANIZATION_ID="ovb"


async def get_token(email: str = EMAIL, password: str = PASSWORD, org_id: str = ORGANIZATION_ID, auth_url: str = AUTH_URL):
    """Get authentication token"""
    print("🔐 Getting authentication token...")
    params = {"organization_id": org_id} if org_id else {}
    payload = {
        "email": email,
        "password": password
    }
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(auth_url, params=params, json=payload, headers=headers) as response:
                response.raise_for_status()
                print(f"✅ Authentication successful")
                token_data = await response.json()
                return token_data.get('access_token')
        except aiohttp.ClientError as e:
            print(f"❌ Authentication failed: {e}")
            raise

async def main():
    try:
       
        access_token = await get_token()

    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
