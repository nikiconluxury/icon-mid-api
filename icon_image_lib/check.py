import aiohttp
import asyncio

async def create_image():
    async with aiohttp.ClientSession(headers={"Accept": "application/json", "Content-Type": "application/json"}) as session:
        url = 'https://supreme-space-waddle-q777pqp5gpwwc6645-8080.app.github.dev/api/v1/image/create'
        payload = {"dataset_split": ["Sergio Rossi", "A86730-MFN882-9003"]}
        
        async with session.post(url, json=payload) as response:
            print(response)
            if response.status == 200:
                response_json = await response.json()
                print(f"Task ID: {response_json.get('task_id')}, Status: {response_json.get('status')}")
            else:
                print(f"Failed to create image, status code: {response.status}, reason: {response.reason}")

asyncio.run(create_image())
