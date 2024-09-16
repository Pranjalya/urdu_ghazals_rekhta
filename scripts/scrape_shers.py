import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import json

IN_FILE = "data/rekhta_top_poets_list.json"
OUT_FILE = "data/rekhta_poets_shers.json"


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


def get_shers(poet_url, lang="hi", romanized=False):
    section = "couplets"
    url = f"{poet_url}/{section}"
    response = requests.get(url, params={"lang": lang})
    soup = BeautifulSoup(response.text, 'html.parser')

    shers = []

    main_shers_section = soup.find("div", {"class": "sherContainer contentLoadMoreSection nwPoetSher fixed_Quote"})
    if main_shers_section is None:
        return []
    for sher_section in main_shers_section.find_all("div", class_="sherSection"):
        if lang != "en":
            poem_divs = sher_section.find("div", {"class": "pMC", "data-roman": "on"})
            if poem_divs is None:
                poem_divs = sher_section.find("div", {"class": "pMC", "data-roman": "off"})
        else:
            if romanized:
                poem_divs = sher_section.find("div", {"class": "pMC", "data-roman": "on"})
            else:
                poem_divs = sher_section.find("div", {"class": "pMC", "data-roman": "off"})
        for div in poem_divs.find_all("div", {'class':'t'}): 
            div.decompose()
        sher = scrape_lines(poem_divs)
        shers.append(sher)
    return shers
    

def scrape_shers(poets_list_file, shers_dump_file):
    with open(poets_list_file) as f:
        poets = json.load(f)

    sher_dump = {}
    
    for poet in tqdm(poets):
        poet_shers = {}
        for lang in tqdm(["en-rm", "en", "hi", "ur"]):
            romanized = "rm" in lang
            shers = get_shers(poet["href"], lang=lang, romanized=romanized)
            poet_shers[lang] = shers
        
        assert len(poet_shers["en"]) == len(poet_shers["en-rm"]) == len(poet_shers["hi"]) == len(poet_shers["ur"])
        
        sher_dump[poet["href"]] = []
        for i in range(len(poet_shers["en"])):
            sher_dump[poet["href"]].append({
                "hi": poet_shers["hi"][i],
                "en": poet_shers["en"][i],
                "en-rm": poet_shers["en-rm"][i],
                "ur": poet_shers["ur"][i],
            })

        with open(shers_dump_file, "w") as f:
            json.dump(sher_dump, f, ensure_ascii=False, indent=2)


if __name__=="__main__":
    scrape_shers(IN_FILE, OUT_FILE)
