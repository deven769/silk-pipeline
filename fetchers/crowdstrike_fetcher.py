import aiohttp

class CrowdstrikeFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.recruiting.app.silk.security/api/crowdstrike/hosts/get"

    async def fetch_hosts(self, session: aiohttp.ClientSession, skip: int = 1, limit: int = 1):
        url = f"{self.base_url}?skip={skip}&limit={limit}"
        headers = {
            "token": self.api_key,
            "accept": "application/json"
        }
        async with session.post(url, headers=headers) as response:
            try:
                response.raise_for_status()
            except aiohttp.ClientResponseError as e:
                print(f"Error fetching hosts: {e}")
                print(f"Response content: {await response.text()}")
                raise
            return await response.json()
