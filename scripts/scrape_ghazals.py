import os
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm

IN_FILE = "data/rekhta_top_poets_poems_list.json"
OUT_FILE = "data/rekhta_top_poets_ghazals.json"


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


async def get_ghazal(session, ghazal_url, lang="hi", romanized=False):
    async with session.get(ghazal_url, params={"lang": lang}) as response:
        text = await response.text()
        soup = BeautifulSoup(text, "html.parser")

        ghazal_section = soup.find("div", {"class": "mainPageWrap NewPoem"})

        if lang != "en":
            poem_divs = ghazal_section.find("div", {"class": "pMC", "data-roman": "on"})
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


async def fetch_ghazals_for_poet(
    session, poet, ghazals, ghazals_dump, ghazals_dump_file, overall_progress
):
    if poet not in ghazals_dump:
        ghazals_dump[poet] = {}

    for ghazal_url in ghazals:
        if ghazal_url in ghazals_dump[poet]:
            overall_progress.update(1)
            continue

        ghazal_langs = {}
        for lang in ["en-rm", "en", "hi", "ur"]:
            romanized = "rm" in lang
            ghazal = await get_ghazal(
                session, ghazal_url, lang=lang, romanized=romanized
            )
            ghazal_langs[lang] = ghazal

        ghazals_dump[poet][ghazal_url] = ghazal_langs

        with open(ghazals_dump_file, "w") as f:
            json.dump(ghazals_dump, f, ensure_ascii=False, indent=2)

        overall_progress.update(1)


async def scrape_ghazals_async(poets_list_file, ghazals_dump_file):
    with open(poets_list_file) as f:
        poets = json.load(f)

    ghazals_dump = {}
    if os.path.exists(ghazals_dump_file):
        with open(ghazals_dump_file) as f:
            ghazals_dump = json.load(f)

    total_ghazals = sum(len(poets[poet]["ghazals"]) for poet in poets)

    async with aiohttp.ClientSession() as session:
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
                )
                tasks.append(task)

            await asyncio.gather(*tasks)


async def main():
    await scrape_ghazals_async(IN_FILE, OUT_FILE)


if __name__ == "__main__":
    asyncio.run(main())
