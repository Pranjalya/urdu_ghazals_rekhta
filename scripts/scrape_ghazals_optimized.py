import os
import json
import asyncio
import aiofiles
from aiolimiter import AsyncLimiter
import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm
from aiohttp import ClientError

IN_FILE = "data/rekhta_all_poets_poems_list.json"
OUT_FILE = "data/rekhta_all_poets_ghazals.json"


def scrape_lines(poem_div):
    lines = []
    sections = poem_div.find_all("div", class_="w")
    for section in sections:
        poem_lines = section.find_all("p")
        for poem_line in poem_lines:
            lines.append(poem_line.text)
        lines.append("")
    poem = "\n".join(lines)
    poem = poem.strip().strip("\n")
    return poem


async def get_ghazal(session, ghazal_url, lang="hi", romanized=False, max_retries=3):
    for attempt in range(max_retries):
        try:
            async with session.get(ghazal_url, params={"lang": lang}) as response:
                if response.status == 200:
                    text = await response.text()
                    soup = BeautifulSoup(text, "html.parser")

                    ghazal_section = soup.find("div", {"class": "mainPageWrap NewPoem"})

                    if lang != "en":
                        poem_divs = ghazal_section.find(
                            "div", {"class": "pMC", "data-roman": "on"}
                        )
                        if poem_divs is None:
                            poem_divs = ghazal_section.find(
                                "div", {"class": "pMC", "data-roman": "off"}
                            )
                    else:
                        if romanized:
                            poem_divs = ghazal_section.find(
                                "div", {"class": "pMC", "data-roman": "on"}
                            )
                        else:
                            poem_divs = ghazal_section.find(
                                "div", {"class": "pMC", "data-roman": "off"}
                            )

                    for div in poem_divs.find_all("div", {"class": "t"}):
                        div.decompose()

                    poem = scrape_lines(poem_divs)
                    return poem
                else:
                    print(
                        f"Attempt {attempt + 1}: Got status {response.status} for {ghazal_url}"
                    )
                    if attempt == max_retries - 1:
                        print(
                            f"Failed to fetch {ghazal_url} after {max_retries} attempts"
                        )
                        return None
                    await asyncio.sleep(5)
        except ClientError as e:
            print(f"Attempt {attempt + 1}: ClientError for {ghazal_url}: {str(e)}")
            if attempt == max_retries - 1:
                print(f"Failed to fetch {ghazal_url} after {max_retries} attempts")
                return None
            await asyncio.sleep(5)


async def fetch_ghazals_for_poet(
    session, poet, ghazals, ghazals_dump, ghazals_dump_file, overall_progress, limiter
):
    if poet not in ghazals_dump:
        ghazals_dump[poet] = {}

    async with limiter:
        for ghazal_url in ghazals:
            if ghazal_url in ghazals_dump[poet]:
                check = True
                for lang in ["en-rm", "en", "hi", "ur"]:
                    if ghazals_dump[poet][ghazal_url].get(lang) is None:
                        check = False
                if check:
                    overall_progress.update(1)
                    continue

            ghazal_langs = {}
            try:
                for lang in ["en-rm", "en", "hi", "ur"]:
                    if ghazals_dump[poet].get(ghazal_url, {}).get(lang) is not None:
                        continue
                    romanized = "rm" in lang
                    ghazal = await get_ghazal(
                        session, ghazal_url, lang=lang, romanized=romanized
                    )
                    if ghazal is not None:
                        ghazal_langs[lang] = ghazal
            except Exception as e:
                print(f"Error fetching {ghazal_url}: {str(e)}")
                overall_progress.update(1)
                continue

            if ghazal_langs:
                ghazals_dump[poet][ghazal_url] = ghazal_langs

                async with aiofiles.open(ghazals_dump_file, "w", encoding="utf-8") as f:
                    await f.write(
                        json.dumps(ghazals_dump, ensure_ascii=False, indent=2)
                    )

            overall_progress.update(1)


async def scrape_ghazals_async(poets_list_file, ghazals_dump_file):
    async with aiofiles.open(poets_list_file, "r", encoding="utf-8") as f:
        poets = json.loads(await f.read())

    ghazals_dump = {}
    if os.path.exists(ghazals_dump_file):
        async with aiofiles.open(ghazals_dump_file, "r", encoding="utf-8") as f:
            ghazals_dump = json.loads(await f.read())

    total_ghazals = sum(len(poets[poet]["ghazals"]) for poet in poets)

    # 300 requests per second
    limiter = AsyncLimiter(300, 1)

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30)
    ) as session:
        tasks = []
        with tqdm(total=total_ghazals) as overall_progress:
            for poet in poets:
                task = fetch_ghazals_for_poet(
                    session,
                    poet,
                    poets[poet]["ghazals"],
                    ghazals_dump,
                    ghazals_dump_file,
                    overall_progress,
                    limiter,
                )
                tasks.append(task)

            await asyncio.gather(*tasks)


async def main():
    await scrape_ghazals_async(IN_FILE, OUT_FILE)


if __name__ == "__main__":
    asyncio.run(main())
