import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import json

IN_FILE = "data/rekhta_top_poets_list.json"
OUT_FILE = "data/rekhta_top_poets_poems_list.json"

links_sections = ["ghazals", "nazms"]


def get_links(poet_url):
    details = {"ghazals": [], "nazms": []}

    for section in links_sections:
        url = f"{poet_url}/{section}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        content_section = soup.find(
            "div",
            {
                "class": "contentListBody contentLoadMoreSection rt_miriyaatSec rt_manageColumn"
            },
        )
        links = content_section.find_all("a") if content_section is not None else []

        for link in links:
            if link.get("href") and not "//" in link["href"].replace("https://", ""):
                details[section].append(link["href"].strip())
    return details


def scrape_poems_list(poets_file, poems_list_file):
    with open(poets_file) as f:
        poets = json.load(f)

    poems_list = {}
    for poet in tqdm(poets):
        details = get_links(poet["href"])
        poems_list[poet["href"]] = details

    with open(poems_list_file, "w") as f:
        json.dump(poems_list, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    scrape_poems_list(IN_FILE, OUT_FILE)
