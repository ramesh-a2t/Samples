import aiohttp
import asyncio

async def main():
    async with aiohttp.ClientSession() as session:
        # Make requests using the session
        async with session.get("https://www.fast.com") as response:
            print(await response.text())

    # Session is automatically closed here

if __name__ == "__main__":
    asyncio.run(main())