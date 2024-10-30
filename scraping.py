import os
import time
import json
import re
from datetime import date
from dotenv import load_dotenv
from pprint import pprint
import asyncio
import requests
from requests.exceptions import ConnectionError, Timeout
from http.client import RemoteDisconnected
import numpy as np
import pandas as pd
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from kafka import KafkaProducer

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("GEMINI_API-KEY-3")


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    return driver


def get_subdristric_link(url, driver):
    driver.get(url)

    # get sublinks for each sub-district in Depok
    driver.find_element(
        By.XPATH, '//*[@id="js-crosslinkTopFilter"][@data-has-sublink=""]/a'
    ).click()
    sublinks_by_loc = driver.find_elements(
        By.XPATH, '//*[@id="js-crosslinkTopFilter"]/div/div/a'
    )
    return [tag.get_attribute("href") for tag in sublinks_by_loc]


def parse_page(driver, link, producer):
    """Parse individual house data from the current page."""
    data = []
    listings = driver.find_elements(By.CSS_SELECTOR, ".ListingCell-AllInfo.ListingUnit")
    info_a = driver.find_elements(By.CSS_SELECTOR, ".js-listing-link")

    for i, listing in tqdm(
        enumerate(listings), desc="Extracting house characteristics..."
    ):
        # time.sleep(3)
        description_text = listing.find_element(
            By.CLASS_NAME, "ListingCell-shortDescription"
        ).text
        # parsed_description = extract_description(description_text)

        details = {
            "price": listing.get_attribute("data-price"),
            "category": listing.get_attribute("data-category"),
            "subcategory": listing.get_attribute("data-subcategories"),
            "bedrooms": listing.get_attribute("data-bedrooms"),
            "bathrooms": listing.get_attribute("data-bathrooms"),
            "land_size": listing.get_attribute("data-land_size"),
            "building_size": listing.get_attribute("data-building_size"),
            "furnished": listing.get_attribute("data-furnished"),
            "geo_point": listing.get_attribute("data-geo-point"),
            "floors": listing.get_attribute("data-floors_total") or "",
            "description": description_text,
            # "parsed_description": parsed_description,
            "parent_url": link,
            "page_url": info_a[i * 2].get_attribute("href"),
        }
        try:
            # print("Send data to kafka...")
            producer.send("properties", value=json.dumps(details).encode("utf-8"))
            print("Sending to kafka...")
        except Exception as e:
            print(f"Can not send data into kakfa! {e}")

        data.append(details)

    return data


def extract_description(description):
    # prompt = f"""
    #     You are a web scraping model tasked with extracting house information from a website's description. Here is the description:

    #     {description}

    #     Please extract the following fields: bedrooms (jumlah kamar tidur), bathrooms (jumlah kamar mandi), land size (lt/luas tanah), building size (lb/luas bangunan), floors (jumlah lantai), and electricity capacity (listrik/tegangan).
    #     if there is no information in description, let fill with empty string "".
    #     The output should be in JSON format as follows:
    # """
    prompt = f"""
        Anda adalah sebuah model web scraper yang ditugaskan untuk mengekstrak informasi dari deskripsi informasi rumah pada sebuah website.
        ini adalah deskripsinya:
        
        {description}

        Selanjutnya, informasi kamar tidur dimasukkan ke bedrooms, kamar mandi ke bathrooms, luas tanah (lt) ke land_size, luas bangunan (lb) ke building_size, jumlah lantai ke floors, dan listrik ke electricity. Apabila tidak ada informasi di deskripsi, isi dengan string kosong ""
        Berikut adalah struktur hasil akhir dalam bentuk JSON. 
        {{
            "bedrooms": "",
            "bathrooms": "",
            "land_size": "",
            "building_size": "", 
            "floors":  "",
            "electricity": "",
        }}
    """

    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={API_KEY}"
    headers = {"Content-Type": "application/json"}
    data = {"contents": [{"parts": [{"text": prompt}]}]}

    retries = 3
    backoff_factor = 3

    tries = 0
    while tries < retries:
        try:
            response = requests.post(
                endpoint, headers=headers, data=json.dumps(data), timeout=10
            )
            if response.status_code == 200:
                result = response.json()
                json_str = result["candidates"][0]["content"]["parts"][0]["text"]
                # print(f"raw json:\n {json_str}")

                # json_str = json_str.replace("```json\n", "").replace("\n```", "")
                json_match = re.search(r"{[\s\S]*?}", json_str)
                if json_match:
                    json_str = json_match.group(0)
                    extracted_data = json.loads(json_str)
                    # print(f"Extracted data: {extracted_data}\n")
                    return extracted_data
                else:
                    print("No JSON format found in response.")
                    return None

            elif response.status_code == 503 and tries < retries - 1:
                sleep_time = backoff_factor**tries
                print(
                    f"Error: {response.status_code}. Retrying in {sleep_time} seconds..."
                )
                time.sleep(sleep_time)

            else:
                print(f"Error: {response.status_code}")
                print(response.text)
                return None

        except (ConnectionError, RemoteDisconnected, Timeout) as e:
            if tries < retries - 1:
                sleep_time = backoff_factor**tries
                print(f"Connection error: {e}. Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print(f"Failed after {retries} attempts. Error: {e}")
                return None

        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}")
            print(f"Response received: {json_str}")
            return None
        tries += 1


def extract_house_data(link, driver, producer, page_end, set_limit=False):
    house_data = []
    driver.get(link)
    if set_limit:
        page_end = page_end
    else:
        try:
            page_end = int(
                driver.find_element(
                    By.CSS_SELECTOR, "*[data-pagination-end]:last-child"
                ).get_attribute("data-pagination-end")
            )
        except:
            pass

    for page_iter in range(1, page_end + 1):
        parsed_page = parse_page(driver, link, producer)
        house_data.extend(parsed_page)

        # iterate through next page
        next_page = driver.find_element(
            By.CSS_SELECTOR, 'link[rel="next"]'
        ).get_attribute("href")

        driver.get(next_page)
        time.sleep(2 + np.random.normal(loc=0.0, scale=0.627))

    return house_data


def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)


def scrape_data(producer):
    url = "https://www.lamudi.co.id/depok/house/buy/"
    driver = init_driver()
    links = get_subdristric_link(url, driver)

    try:
        print(f"connected! Navigating to {url}")
        all_house_data = []
        for link in links[:1]:
            data = extract_house_data(
                link, driver, producer, set_limit=True, page_end=1
            )
            all_house_data.extend(data)

        # pprint(all_house_data)
        # save_to_csv(all_house_data, f"depok_house_data_{date.today().isoformat()}.csv")
        with open(
            f"./data/2depokhouse_{date.today().isoformat()}.json", "w", encoding="utf-8"
        ) as f:
            json.dump(all_house_data, f, ensure_ascii=False, indent=4)

    finally:
        driver.quit()


def run_kafka_stream():
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=5000)

    try:
        scrape_data(producer)
    finally:
        # producer.close()
        print("Kafka producer connection closed...")


if __name__ == "__main__":
    run_kafka_stream()
