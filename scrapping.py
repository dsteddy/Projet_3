from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options

from offres_emploi import Api

import re

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

def create_cols_to_keep(site: str):
    if site == "wttj":
        cols_to_keep = [
        "published_at",
        "updated_at",
        "name",
        "salary_period",
        "experience_level",
        # "apply_url",
        "contract_duration_min",
        "office.city",
        "office.zip_code",
        # "profession.category.fr",
        "education_level",
        "description",
        "organization.description",
        "organization.industry",
        "contract_type",
        "salary_min",
        "salary_max",
        "education_level"
    ]
        return cols_to_keep

    if site == "pole emploi":
        cols_to_drop = [
                "id",
                "lieuTravail",
                "romeCode",
                "romeLibelle",
                "origineOffre",
                "appellationlibelle",
                "natureContrat",
                "entreprise",
                "typeContratLibelle",
                "experienceExige",
                "experienceLibelle",
                # "dureeTravailLibelle",
                # "dureeTravailLibelleConverti",
                "formations",
                "langues",
                "salaire",
                "alternance",
                "contact",
                "nombrePostes",
                "accessibleTH",
                "deplacementCode",
                "deplacementLibelle",
                "qualificationCode",
                "qualificationLibelle",
                "codeNAF",
                "secteurActivite",
                "qualitesProfessionnelles",
                "offresManqueCandidats",
                "experienceCommentaire",
                "permis",
                "complementExercice"
            ]
        return cols_to_drop


def clean_html(text):
    soup = BeautifulSoup(text, 'html.parser')
    cleaned_text = soup.get_text(separator=" ")
    cleaned_text = cleaned_text.replace("\xa0", " ")
    return cleaned_text


async def fetch(session, url):
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
    df["description"] = df["description"].apply(clean_html)
    df["organization.description"] = df["organization.description"].apply(clean_html)
    df.rename(
    {
        'education_level' : 'niveau_etudes',
        'contract_type' : 'type_contrat',
        'name' : 'intitule',
        'published_at' : 'date_publication',
        'updated_at' : 'date_modif',
        'experience_level' : 'experience',
        'office.city' : 'ville',
        'office.zip_code' : 'code_postal',
        'organization.description' : 'description_entreprise',
        'organization.industry' : 'secteur_activite'
        }, axis = 1, inplace = True
    )
    return df


def job_offers_wttj(
        job_title: str = "data analyst",
        pages: int = 1
):
    # Instanciation de la liste contenant les liens pour les requêtes APIs.
    api_links = []
    # Lien de l'API de Welcome To The Jungle pour récupérer les données.
    api_link = f"https://api.welcometothejungle.com/api/v1/organizations"
    job = job_title.lower().replace(" ", "+")
    # Instanciation du driver Firefox.
    firefox_options = Options()
    firefox_options.headless = True
    driver = webdriver.Firefox(options=firefox_options)
    # Nom des colonnes à garder dans le dataframe final.
    logging.info(f"Starting job offer scrapping for {pages} pages...")
    try:
        for i in range(1, pages+1):
            url = f"https://www.welcometothejungle.com/fr/jobs?refinementList%5Boffices.country_code%5D%5B%5D=FR&query={job}&page={i}"
            # Ouvre chaque page sur le navigateur.
            driver.get(url)
            try:
                # Récupère le lien de chaque offre d'emploi sur la page.
                contents = WebDriverWait(driver, 100).until(
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


def clean_dict_columns(df: pd.DataFrame):
    df['dateActualisation'] = pd.to_datetime(df['dateActualisation'])
    df['dateCreation'] = pd.to_datetime(df['dateCreation'])
    df_lieu_travail = pd.json_normalize(df["lieuTravail"])
    df_lieu_travail.drop(["latitude", "longitude", "commune"], axis=1, inplace=True)
    df_lieu_travail.rename({"libelle" : "ville"}, axis = 1, inplace = True)
    df_entreprise = pd.json_normalize(df["entreprise"])
    df_entreprise.drop(["entrepriseAdaptee", 'url', 'logo'], axis=1, inplace=True)
    df_entreprise.rename({"description" : "description_entreprise"}, axis=1, inplace=True)
    df_salaire = pd.json_normalize(df["salaire"])
    df_salaire.drop(["complement1", "complement2", "commentaire"], axis=1, inplace=True)
    df_formations = df["formations"].explode()
    df_formations = pd.json_normalize(df_formations)
    df_formations.drop(["codeFormation", "domaineLibelle", "exigence", "commentaire"], axis=1, inplace=True)
    df_final = pd.concat([df, df_formations, df_salaire, df_entreprise, df_lieu_travail], axis=1)
    return df_final


def job_offers_pole_emploi(params, cols_to_drop):
    # Initialisation des variables locales
    results = []
    start_range = 0
    max_results = float('inf')
    api_client = Api(client_id="PAR_datajobs_addbc0bc41d7d51f05b218a78c5a95e14be4d73d536fde31c5962de09420f7ba",
             client_secret="ec561083589b4912fb4feebf33ef1078098c0b8b3f8ff04a665d3e46173f11e1")

    try:
        # Pagination pour récupérer tous les résultats
        while start_range < max_results:
            params['range'] = f"{start_range}-{start_range + 149}"  # Utilisation de la pagination

            # Effectuer la recherche
            search_result = api_client.search(params=params)
            content_range = search_result.get('Content-Range', {})
            max_results_str = content_range.get('max_results', '0')
            max_results = int(max_results_str)

            # Ajouter les résultats actuels à la liste globale
            results.extend(search_result.get('resultats', []))

            # Mettre à jour la plage de départ pour la prochaine itération
            start_range += 150

        if max_results > 0:
            df_emploi = pd.DataFrame(results)
            df_final = clean_dict_columns(df_emploi)
            df_final.drop(cols_to_drop, axis=1, inplace=True)
            df_final.rename(
                {
                    'dateCreation' : 'date_publication',
                    'dateActualisation' : 'date_modif',
                    'typeContrat' : 'contrat',
                    "dureeTravailLibelle" : "duree_travail",
                    "dureeTravailLibelleConverti" : "type_contrat",
                    'secteurActiviteLibelle' : 'secteur_activite',
                    'niveauLibelle' : 'niveau_etudes',
                    'libelle' : 'salaire',
                    'nom' : 'entreprise',
                    'codePostal' : 'code_postal'
                }, axis = 1, inplace = True
            )
            return df_final

        else:
            print("Aucune offre d'emploi trouvée.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Une erreur s'est produite lors de la recherche : {e}")


