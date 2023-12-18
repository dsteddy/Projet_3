from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import requests
import pandas as pd
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

def create_cols_to_keep(site):
    if site == "welcome to the jungle":
        cols_to_keep = [
        "name",
        "salary_period",
        "experience_level",
        "apply_url",
        "contract_duration_min",
        "office.city",
        "office.address",
        "office.district",
        "office.latitude",
        "office.longitude",
        "office.zip_code",
        "profession.category.fr",
        "profession.name.fr"
        "name",
        "education_level",
        "application_fields.mode",
        "application_fields.name",
        "description",
        "organization.average_age",
        "organization.creation_year",
        "organization.default_language",
        "organization.description",
        "organization.industry",
        "organization.nb_employee",
        "contract_type",
        "salary_min",
        "salary_max",
        "education_level",
        "remote"
    ]
        return cols_to_keep


async def fetch(session, url):
    logging.info("Gathering information from API...")
    while True:
        async with session.get(url) as response:
            if response.status == 429:
                logging.error("API Limit reached!")
                await asyncio.sleep(10)
                continue
            return await response.json()


async def fetch_all(api_links, cols_to_keep):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, link) for link in api_links]
        responses = await asyncio.gather(*tasks)
    logging.info("API requests done!")
    logging.info("Concatening dataframes...")
    full_df = pd.concat([pd.json_normalize(resp["job"]) for resp in responses], ignore_index=True)
    cols_to_drop = [col for col in full_df.columns if col not in cols_to_keep]
    df = full_df.drop(columns=cols_to_drop)
    logging.info("DataFrame done!")
    return df


def job_offers_wtj(
        job_title: str = "data analyst",
        pages: int = 1
):
    # Instanciation de la liste contenant les liens pour les requêtes APIs.
    api_links = []
    # Lien de l'API de Welcome To The Jungle pour récupérer les données.
    api_link = f"https://api.welcometothejungle.com/api/v1/organizations"
    job = job_title.lower().replace(" ", "+")
    # Instanciation du driver Firefox.
    driver = webdriver.Firefox()
    # Nom des colonnes à garder dans le dataframe final.
    logging.info(f"Starting job offer scrapping for {i} pages...")
    try:
        for i in range(1, pages+1):
            url = f"https://www.welcometothejungle.com/fr/jobs?refinementList%5Boffices.country_code%5D%5B%5D=FR&query={job}&page={i}"
            # Ouvre chaque page sur le navigateur.
            driver.get(url)
            try:
                # Récupère le lien de chaque offre d'emploi sur la page.
                contents = WebDriverWait(driver, 50).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".sc-6i2fyx-0.gIvJqh"))
                )
                for content in contents:
                    link = content.get_attribute("href")
                    end_link = re.findall(r"/companies(.+)", link)[0]
                    full_link = api_link + end_link
                    # Rajoute le lien de chaque offre à la liste.
                    api_links.append(full_link)
            except Exception as e:
                logging.error(f"Error scraping page {i} : {e}")
    finally:
        driver.quit()
        # Pour chaque lien de la liste, fait une requête API et stocke les informations dans un dataframe.
    logging.info("Scrapping done!")
    return api_links


def clean_html(text):
    logging.info("Cleaning columns...")
    soup = BeautifulSoup(text, 'html.parser')
    cleaned_text = soup.get_text(separator=" ")
    cleaned_text = cleaned_text.replace("\xa0", " ")
    logging.info("Cleaning done!")
    return cleaned_text


cols_to_keep = create_cols_to_keep("welcome to the jungle")
api_links = job_offers_wtj("data analyst", 5)
df = asyncio.run(fetch_all(api_links, cols_to_keep))
df["description"] = df["description"].apply(clean_html)
df["organization.description"] = df["organization.description"].apply(clean_html)
logging.info("Saving CSV...")
df.to_csv("WTT_offers.csv")
logging.info("Done!")