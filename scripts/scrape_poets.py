import os
import json
import urllib
import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup


DATA_DIR = "data"
BASE_URL = "https://www.rekhta.org"


os.makedirs(DATA_DIR, exist_ok=True)


def scrape_poets_list(api_url, params, save_path=None):
    response = requests.get(api_url, params=params)
    soup = BeautifulSoup(response.text, "html.parser")

    poets = []
    for poet_div in soup.find_all("div", class_="poetColumn"):
        name_div = poet_div.find("div", class_="poetNameDatePlace")
        name_a = name_div.find("a")
        name = name_a.text.strip()
        href = name_a["href"]

        location_div = poet_div.find("div", class_="poetPlaceDate")
        location_a = location_div.find("a")
        location_a = location_a.text if location_a is not None else None

        active_years = poet_div.find("span", class_="poetListDate")
        if active_years:
            active_years = active_years.text.strip()
        else:
            active_years = None
        description = poet_div.find("div", class_="poetDescColumn")
        if description and description.find("p"):
            description = description.find("p").text.strip()
        else:
            description = None

        poet = {
            "name": name,
            "href": href,
            "location": location_a,
            "active_years": active_years,
            "description": description,
        }
        poets.append(poet)

    if save_path:
        with open(save_path, "w") as f:
            json.dump(poets, f, ensure_ascii=False, indent=2)
    else:
        return poets


def json_to_csv_dump(json_path, csv_path):
    df = pd.read_json(json_path)
    df.to_csv(csv_path, index=False)


def scrape_poets(top_poets=False):
    if top_poets:
        route = "poets/top-read-poets"
        params = {}
        save_path = os.path.join(DATA_DIR, "rekhta_top_poets_list.json")
        url = os.path.join(BASE_URL, route)
        scrape_poets_list(url, params, save_path)
    else:
        route = "poets"
        url = os.path.join(BASE_URL, route)
        save_path = os.path.join(DATA_DIR, "rekhta_all_poets_list.json")
        all_poets = []
        for letter in tqdm(
            list(range(ord("A"), ord("Z") + 1)) + list(range(ord("a"), ord("z") + 1))
        ):
            params = {"startswith": chr(letter)}
            poets_list = scrape_poets_list(url, params)
            all_poets.extend(poets_list)
        with open(save_path, "w") as f:
            json.dump(all_poets, f, ensure_ascii=False, indent=2)

    csv_path = save_path.replace(".json", ".csv")
    json_to_csv_dump(save_path, csv_path)


if __name__ == "__main__":
    scrape_poets(top_poets=True)
    scrape_poets(top_poets=False)
