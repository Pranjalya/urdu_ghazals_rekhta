import os
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm

IN_FILE = "data/rekhta_top_poets_poems_list.json"
OUT_FILE = "data/rekhta_poets_nazms.json"


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


async def get_nazm(session, nazm_url, lang="hi", romanized=False):
    async with session.get(nazm_url, params={"lang": lang}) as response:
        text = await response.text()
        soup = BeautifulSoup(text, "html.parser")

        nazm_section = soup.find("div", {"class": "mainPageWrap NewPoem"})

        if lang != "en":
            poem_divs = nazm_section.find("div", {"class": "pMC", "data-roman": "on"})
            if poem_divs is None:
                poem_divs = nazm_section.find(
                    "div", {"class": "pMC", "data-roman": "off"}
                )
        else:
            if romanized:
                poem_divs = nazm_section.find(
                    "div", {"class": "pMC", "data-roman": "on"}
                )
            else:
                poem_divs = nazm_section.find(
                    "div", {"class": "pMC", "data-roman": "off"}
                )

        for div in poem_divs.find_all("div", {"class": "t"}):
            div.decompose()

        poem = scrape_lines(poem_divs)
        return poem


async def fetch_nazms_for_poet(
    session, poet, nazms, nazms_dump, nazms_dump_file, overall_progress
):
    if poet not in nazms_dump:
        nazms_dump[poet] = {}

    for nazm_url in nazms:
        if nazm_url in nazms_dump[poet]:
            overall_progress.update(1)  # Update progress if the nazm is already fetched
            continue

        nazm_langs = {}
        for lang in ["en-rm", "en", "hi", "ur"]:
            romanized = "rm" in lang
            nazm = await get_nazm(session, nazm_url, lang=lang, romanized=romanized)
            nazm_langs[lang] = nazm

        nazms_dump[poet][nazm_url] = nazm_langs

        with open(nazms_dump_file, "w") as f:
            json.dump(nazms_dump, f, ensure_ascii=False, indent=2)

        overall_progress.update(1)


async def scrape_nazms_async(poets_list_file, nazms_dump_file):
    with open(poets_list_file) as f:
        poets = json.load(f)

    nazms_dump = {}
    if os.path.exists(nazms_dump_file):
        with open(nazms_dump_file) as f:
            nazms_dump = json.load(f)

    total_nazms = sum(len(poets[poet]["nazms"]) for poet in poets)

    async with aiohttp.ClientSession() as session:
        tasks = []
        with tqdm(total=total_nazms) as overall_progress:
            for poet in poets:
                task = fetch_nazms_for_poet(
                    session,
                    poet,
                    poets[poet]["nazms"],
                    nazms_dump,
                    nazms_dump_file,
                    overall_progress,
                )
                tasks.append(task)

            await asyncio.gather(*tasks)


async def main():
    await scrape_nazms_async(IN_FILE, OUT_FILE)


if __name__ == "__main__":
    asyncio.run(main())
