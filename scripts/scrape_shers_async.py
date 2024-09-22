import os
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import json
import aiofiles
from tqdm import tqdm


IN_FILE = "data/rekhta_all_poets_list.json"
OUT_FILE = "data/rekhta_all_poets_shers.json"


def scrape_lines(poem_div):
    lines = []
    sections = poem_div.find_all("div", class_="w")
    for section in sections:
        poem_lines = section.find_all("p")
        for poem_line in poem_lines:
            lines.append(poem_line.text)
        lines.append("")
    poem = "\n".join(lines).strip("\n")
    return poem


async def get_shers(session, poet_url, lang="hi", romanized=False):
    section = "couplets"
    url = f"{poet_url}/{section}"
    params = {"lang": lang}

    async with session.get(url, params=params) as response:
        soup = BeautifulSoup(await response.text(), "html.parser")

        shers = []
        main_shers_section = soup.find(
            "div",
            {"class": "sherContainer contentLoadMoreSection nwPoetSher fixed_Quote"},
        )
        if main_shers_section is None:
            return []

        for sher_section in main_shers_section.find_all("div", class_="sherSection"):
            if lang != "en":
                poem_divs = sher_section.find(
                    "div", {"class": "pMC", "data-roman": "on"}
                ) or sher_section.find("div", {"class": "pMC", "data-roman": "off"})
            else:
                if romanized:
                    poem_divs = sher_section.find(
                        "div", {"class": "pMC", "data-roman": "on"}
                    )
                else:
                    poem_divs = sher_section.find(
                        "div", {"class": "pMC", "data-roman": "off"}
                    )

            if poem_divs is not None:
                for div in poem_divs.find_all("div", {"class": "t"}):
                    div.decompose()
                sher = scrape_lines(poem_divs)
                shers.append(sher)
        return shers


async def fetch_poet_shers(session, poet, sher_dump):
    poet_shers = {}

    for lang in ["en-rm", "en", "hi", "ur"]:
        romanized = "rm" in lang
        shers = await get_shers(session, poet["href"], lang=lang, romanized=romanized)
        poet_shers[lang] = shers

    assert (
        len(poet_shers["en"])
        == len(poet_shers["en-rm"])
        == len(poet_shers["hi"])
        == len(poet_shers["ur"])
    )

    poet_data = []
    for i in range(len(poet_shers["en"])):
        poet_data.append(
            {
                "hi": poet_shers["hi"][i],
                "en": poet_shers["en"][i],
                "en-rm": poet_shers["en-rm"][i],
                "ur": poet_shers["ur"][i],
            }
        )

    sher_dump[poet["href"]] = poet_data


async def scrape_shers(poets_list_file, shers_dump_file):
    async with aiofiles.open(poets_list_file, mode="r") as f:
        poets = json.loads(await f.read())

    if os.path.exists(shers_dump_file):
        async with aiofiles.open(shers_dump_file, mode="r") as f:
            sher_dump = json.loads(await f.read())
    else:
        sher_dump = {}

    async with aiohttp.ClientSession() as session:
        tasks = []
        for poet in poets:
            if poet["href"] not in sher_dump:
                task = fetch_poet_shers(session, poet, sher_dump)
                tasks.append(task)

        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await task

        async with aiofiles.open(shers_dump_file, mode="w") as f:
            await f.write(json.dumps(sher_dump, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(scrape_shers(IN_FILE, OUT_FILE))
