import os
import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from tqdm import tqdm
import json

IN_FILE = "data/rekhta_all_poets_list.json"
OUT_FILE = "data/rekhta_all_poets_poems_list.json"
BATCH_SIZE = 150

links_sections = ["ghazals", "nazms"]


async def get_links(session, poet_url, dump_file, dump):
    details = {"ghazals": [], "nazms": []}

    for section in links_sections:
        url = f"{poet_url}/{section}"
        async with session.get(url) as response:
            text = await response.text()
            soup = BeautifulSoup(text, "html.parser")

            content_section = soup.find(
                "div",
                {
                    "class": "contentListBody contentLoadMoreSection rt_miriyaatSec rt_manageColumn"
                },
            )
            links = content_section.find_all("a") if content_section is not None else []

            for link in links:
                if link.get("href") and "//" not in link["href"].replace(
                    "https://", ""
                ):
                    details[section].append(link["href"].strip())
    dump[poet_url] = details
    with open(dump_file, "w") as f:
        json.dump(dump, f, ensure_ascii=False, indent=2)
    return details


async def process_batch(batch, session, dump_file, dump):
    tasks = [
        asyncio.create_task(get_links(session, poet["href"], dump_file, dump))
        for poet in batch
    ]
    await asyncio.gather(*tasks)


async def scrape_poems_list(poets_file, dump_file):
    with open(poets_file) as f:
        poets = json.load(f)

    if os.path.exists(dump_file):
        with open(dump_file) as f:
            dump = json.load(f)
    else:
        dump = {}

    async with aiohttp.ClientSession() as session:
        for i in tqdm(range(0, len(poets), BATCH_SIZE), desc="Processing batches"):
            batch = poets[i : i + BATCH_SIZE]
            await process_batch(batch, session, dump_file, dump)


if __name__ == "__main__":
    asyncio.run(scrape_poems_list(IN_FILE, OUT_FILE))
