from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options as FirefoxOptions

import nltk
from nltk.corpus import stopwords

from offres_emploi.utils import dt_to_str_iso

from dotenv import load_dotenv
import os

import json

import sqlalchemy

from offres_emploi import Api
import datetime

import re

import pandas as pd

import asyncio
import aiohttp

from bs4 import BeautifulSoup

from tqdm import tqdm

import logging


logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


# Main Function
def scrapping(job_title: str, page : int = None):
    '''
    Fonction principale pour récupérer les infos et créer les bases de
    données.

    Paramètres:
    ---
    job_title: str: Intitulé du poste pour lequel rechercher les offres
    d'emplois sur les différents sites.
    '''
    if job_title == 'all':
        # Data Analyst
        logging.info("------------------------------")
        logging.info("Getting Data Analyst offers...")
        wttj_analyst = job_offers_wttj("data+analyst")
        wttj_analyst = clean_date(wttj_analyst)

        params = {
            "motsCles": "data analyst",
            'minCreationDate': dt_to_str_iso(datetime.datetime(
                2023, 12, 1, 12, 30
            )),
            'maxCreationDate': dt_to_str_iso(datetime.datetime.today()),
        }
        pe_analyst = job_offers_pole_emploi(params)
        pe_analyst = clean_date(pe_analyst)

        # Data Engineer
        logging.info("------------------------------")
        logging.info("Getting Data Engineer offers...")
        wttj_engineer = job_offers_wttj("data+engineer")
        wttj_engineer = clean_date(wttj_engineer)

        params = {
            "motsCles": "data engineer",
            'minCreationDate': dt_to_str_iso(datetime.datetime(
                2023, 9, 1, 12, 30
            )),
            'maxCreationDate': dt_to_str_iso(datetime.datetime.today()),
        }
        pe_engineer = job_offers_pole_emploi(params)
        pe_engineer = clean_date(pe_engineer)

        # Data Scientist
        logging.info("------------------------------")
        logging.info("Getting Data Scientist offers...")
        wttj_scientist = job_offers_wttj("data+scientist")
        wttj_scientist = clean_date(wttj_scientist)

        params = {
            "motsCles": "data scientist",
            'minCreationDate': dt_to_str_iso(datetime.datetime(
                2023, 9, 1, 12, 30
            )),
            'maxCreationDate': dt_to_str_iso(datetime.datetime.today()),
        }
        pe_scientist = job_offers_pole_emploi(params)
        pe_scientist = clean_date(pe_scientist)


        # Concat all
        logging.info("Regrouping dataframes...")
        df = pd.concat(
            [
                wttj_analyst,
                wttj_engineer,
                wttj_scientist,
                pe_analyst,
                pe_engineer,
                pe_scientist
                ],
            ignore_index=True
        )
        logging.info("Dropping duplicates...")
        df = df.drop_duplicates(subset="description", keep="first")
        df = extract_skills(df)
        # df["secteur_activite"] = df["secteur_activite"].apply(clean_secteur_activite)
        logging.info("Saving .parquet file...")
        df.to_parquet(f'datasets/all_jobs.parquet', index=False)
        logging.info("Updating .sqlite DB...")
        create_sql_table(df)
        logging.info("Finished!")
    else:
        job_title_nom_fichier = job_title.replace(" ", "_")

        # WTTJ
        df_wttj = job_offers_wttj(job_title, page)
        df_wttj = clean_date(df_wttj)
        df_wttj = extract_skills(df_wttj)
        # df_wttj["secteur_activite"] = df_wttj["secteur_activite"].apply(clean_secteur_activite)
        logging.info(f"Saving WTTJ_{job_title_nom_fichier}.parquet...")
        df_wttj.to_parquet(
            f'datasets/WTTJ_{job_title_nom_fichier}.parquet', index=False
        )

        # Pole Emploi
        params = {
            "motsCles": "data analyst",
            'minCreationDate': dt_to_str_iso(datetime.datetime(
                2023, 9, 1, 12, 30
            )),
            'maxCreationDate': dt_to_str_iso(datetime.datetime.today()),
        }
        df_pole_emploi = job_offers_pole_emploi(params)
        df_pole_emploi = clean_date(df_pole_emploi)
        df_pole_emploi = extract_skills(df_pole_emploi)
        # df_pole_emploi["secteur_activite"] = df_pole_emploi["secteur_activite"].apply(clean_secteur_activite)
        logging.info(f"Saving pole_emploi_{job_title_nom_fichier}.parquet...")
        df_pole_emploi.to_parquet(
            f'datasets/pole_emploi_{job_title_nom_fichier}.parquet', index=False
        )
        # Concat both
        logging.info("Regrouping both dataframes...")
        df = pd.concat([df_wttj, df_pole_emploi], ignore_index=True)
        df.to_parquet(f'datasets/{job_title_nom_fichier}.parquet', index=False)
        logging.info("Finished!")


# Welcome To The Jungle
def job_offers_wttj(
        job_title: str = "data analyst",
        page : int = None,
    ) -> pd.DataFrame:
    '''
    Scrapping de toutes les offres d'emploi du site Welcome To The Jungle
    pour le job indiqué.

    Paramètres:
    ---
    job_title: str: Nom de l'intitulé du job pour lequel rechercher les
    offres.

    Retourne:
    ---
    df: pd.DataFrame: dataframe contenant les informations de chaque offres
    d'emploi trouvée.
    '''
    # Instanciation de la liste contenant les liens pour les requêtes APIs.
    api_links = []
    # Lien de l'API de Welcome To The Jungle pour récupérer les données.
    api_link = f"https://api.welcometothejungle.com/api/v1/organizations"
    job = job_title.lower().replace(" ", "+")
    # Instanciation du driver Firefox.
    options = FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(
        options=options,
        )
    # Ouverture de la première page.
    if page == None:
        url = f"https://www.welcometothejungle.com/fr/jobs?refinementList%5Boffices.country_code%5D%5B%5D=FR&query={job}&page=1"
        driver.get(url)
        try:
            # Récupère le numéro de la dernière page.
            logging.info("Checking page numbers...")
            page_numbers = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".sc-ezreuY.gGgoDq"))
            )
            page_max = int(page_numbers[-1].text)
            logging.info(f"Starting job offer scrapping for {page_max} pages on Welcome To The Jungle...")
        except:
            logging.info("Page number not found, scrapping for 1 page on Welcome To The Jungle...")
            page_max = 1
    else:
        page_max = page
    try:
        for i in tqdm(range(1, page_max+1), desc="Pages Scrapped", unit="page"):
            url = f"https://www.welcometothejungle.com/fr/jobs?refinementList%5Boffices.country_code%5D%5B%5D=FR&query={job}&page={i}"
            # Ouvre chaque page sur le navigateur.
            driver.get(url)
            try:
                # Récupère le lien de chaque offre d'emploi sur la page.
                contents = WebDriverWait(driver, 30).until(
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
    df = asyncio.run(fetch_all(api_links))
    return df

async def fetch_all(
        api_links:list,
        # cols_to_keep:list
    ) -> pd.DataFrame:
    '''
    Lance toutes les requêtes API pour Welcome To The jungle et créée
    un dataframe contenant toutes
    les informations pour chaque offre et nettoie les données.
    ---
    Paramètres:
    ---
    api_links: list: liste de tout les liens API des offres d'emploi
    récoltées.
    cols_to_keep: list: liste des noms de colonnes à garder dans le
    dataframe final.
    ---
    Retourne:
    ---
    df: pd.DataFrame : dataframe avec les colonnes nettoyées.
    '''
    logging.info("API requests...")
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, link) for link in api_links]
        responses = await asyncio.gather(*tasks)
    logging.info("API requests done!")

    logging.info("Creating dataframe...")
    full_df = pd.concat([pd.json_normalize(resp["job"]) for resp in responses if "job" in resp], ignore_index=True)

    df = global_clean_wttj(full_df)
    logging.info("Welcome To The Jungle DataFrame done!")

    # df["description"] = df["description"].apply(clean_html)
    # df["organization.description"] = df["organization.description"].apply(clean_html)
    # df[df["salary_period"].isna()] = None
    # df[df["salary_max"].isna()] = None
    # df[df["salary_min"].isna()] = None
    # df["salaire"] = df.apply(lambda row: clean_salaire_wttj(
    #     row["salary_period"], row["salary_max"], row["salary_min"]
    # ), axis = 1)
    # df = rename_and_reorder_cols("wttj", df)
    # df["niveau_etudes"] = df["niveau_etudes"].astype(str)
    # df["niveau_etudes"] = df["niveau_etudes"].apply(clean_experience)
    # df["contrat"] = df["contrat"].str.replace(
    #     "full_time", "CDI"
    #     ).str.replace("internship", "Stage"
    #     ).str.replace("apprenticeship", "Alternance"
    #     ).str.replace("temporary", "CDD"
    #     ).str.replace("other", "Autre"
    #     ).str.replace("vie", "CDI"
    # )
    return df

async def fetch(
        session,
        url
    ):
    '''
    Requête API pour récupérer les infos d'une offre d'emploi
    Welcome To The Jungle.
    ---
    Paramètres:
    ---
    session: aiohttp session.
    url: url de l'api contenant les infos d'une offre d'emploi.
    ---
    Retourne:
    ---
    fichier json contenant les informations d'une offre d'emploi.
    '''
    while True:
        try:
            async with session.get(url) as response:
                if response.status == 429:
                    logging.error("API Limit reached!")
                    await asyncio.sleep(30)
                    continue
                return await response.json()
        except:
            await asyncio.sleep(30)

def clean_link(text):
    cleaned_link = text["href"]
    return cleaned_link

def global_clean_wttj(df_to_clean):
    # Creating list of cols_to_drop
    cols_to_keep = create_cols_to_keep('wttj', df_to_clean)
    cols_to_drop = [col for col in df_to_clean.columns if col not in cols_to_keep]
    # Cleaning link column
    df_urls = pd.json_normalize(df_to_clean["urls"])
    df_urls["link"] = df_urls[0]
    df_urls = df_urls.drop(columns=[0,1])
    df_urls["link"] = df_urls["link"].apply(clean_link)
    url_merged = pd.concat([df_to_clean, df_urls], axis=1)
    # Dropping unused columns
    df = url_merged.drop(columns=cols_to_drop)
    # Cleaning all remaining columns
    df["description"] = df["description"].apply(clean_html)
    df["organization.description"] = df["organization.description"].apply(clean_html)
    df[df["salary_period"].isna()] = None
    df[df["salary_max"].isna()] = None
    df[df["salary_min"].isna()] = None
    df["salaire"] = df.apply(lambda row: clean_salaire_wttj(
        row["salary_period"], row["salary_max"], row["salary_min"]
    ), axis = 1)

    df = rename_and_reorder_cols("wttj", df)

    df["contrat"] = df["contrat"].str.replace(
        "full_time", "CDI"
        ).str.replace("internship", "Stage"
        ).str.replace("apprenticeship", "Alternance"
        ).str.replace("temporary", "CDD"
        ).str.replace("other", "Autre"
        ).str.replace("vie", "CDI"
    )
    df["niveau_etudes"] = df["niveau_etudes"].astype(str)
    df["niveau_etudes"] = df["niveau_etudes"].apply(clean_experience)

    df["secteur_activite"] = df["secteur_activite"].apply(clean_secteur_activite)

    return df


# Pole Emploi
def job_offers_pole_emploi(
        params : dict
    ) -> pd.DataFrame:
    '''
    Scrapping pour les offres d'emploi pour l'intitulé du job demandé
    en utilisant l'API de pole emploi.
    ---
    Paramètres:
    ---
    params: dict: Dictionnaire contenant les différents paramètres pour
    les requêtes APIs.
    ---
    Retourne:
    ---
    df_final: pd.DataFrame: dataframe contenant les informations pour
    toutes les offres d'emplois demandées.
    '''
    load_dotenv()
    client_id = os.getenv('USER_POLE_EMPLOI')
    api_key = os.getenv('API_KEY_POLE_EMPLOI')
    # Initialisation des variables locales
    # cols_to_keep = create_cols_to_keep('pole emploi')
    results = []
    start_range = 0
    max_results = float('inf')
    api_client = Api(client_id=client_id,
             client_secret=api_key)
    logging.info("Requesting Pole Emploi API...")
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
    except Exception as e:
        print(f"Une erreur s'est produite lors de la recherche : {e}")
    if max_results > 0:
        df_emploi = pd.DataFrame(results)
        df_final = global_clean_pe(df_emploi)
        logging.info("Pole Emploi DataFrame done!")

        # df_final = clean_dict_columns(df_emploi)
        # cols_to_drop = [col for col in df_final.columns if col not in cols_to_keep]
        # df_final.drop(cols_to_drop, axis=1, inplace=True)
        # df_final = rename_and_reorder_cols("pole emploi", df_final)

        # df_final["description"] = df_final["description"].apply(clean_html)
        # df_final["niveau_etudes"] = df_final["niveau_etudes"].astype(str)
        # df_final["niveau_etudes"] = df_final["niveau_etudes"].apply(clean_experience)
        # df_final["ville"] = df_final["ville"].str.title().str.replace(r"\d+", "", regex=True).str.replace("-", "").str.strip()
        # df_final["contrat"] = df_final["contrat"].str.replace("MIS", "Interim").str.replace("FRA", "Autre").str.replace("LIB", "Autre")
        # df_final[df_final["salaire"].isna()] = None
        # df_final["salaire"] = df_final["salaire"].apply(clean_salaire_pe)
        return df_final
    else:
        print("Aucune offre d'emploi trouvée.")
        return pd.DataFrame()

def clean_dict_columns(
        df: pd.DataFrame
    ) -> pd.DataFrame:
    '''
    Extrait les informations des colonnes contenus dans un dictionnaire
    sur plusieurs colonnes
    ---
    Paramètres:
    ---
    df: pd.DataFrame: Le dataframe dans lequel extraire les dictionnaires.
    ---
    Retourne:
    ---
    df_final: pd.DataFrame: dataframe avec les dictionnaires séparés en
    plusieurs colonnes.
    '''
    dict_cols = [
        ("lieuTravail", ["latitude", "longitude", "commune"]),
        ("entreprise", ["entrepriseAdaptee", "url"]),
        ("salaire", ["complement1", "complement2", "commentaire"]),
        ("formations", ["codeFormation", "domaineLibelle", "exigence", "commentaire"]),
        ("origineOffre", ["origine", "partenaires"]),
    ]

    if dict_cols[0][0] in df.columns:
        df_lieu_travail = pd.json_normalize(df["lieuTravail"])
    for cols in dict_cols[0][1]:
        if cols in df_lieu_travail.columns:
            df_lieu_travail.drop(cols, axis=1, inplace=True)
    if "libelle" in df_lieu_travail.columns:
        df_lieu_travail.rename({"libelle" : "ville"}, axis = 1, inplace = True)

    if dict_cols[1][0] in df.columns:
        df_entreprise = pd.json_normalize(df["entreprise"])
    for cols in dict_cols[1][1]:
        if cols in df_entreprise.columns:
            df_entreprise.drop(cols, axis=1, inplace=True)
    if "description" in df_entreprise:
        df_entreprise.rename({"description" : "description_entreprise"}, axis=1, inplace=True)

    if dict_cols[2][0] in df.columns:
        df_salaire = pd.json_normalize(df["salaire"])
    for cols in dict_cols[2][1]:
        if cols in df_salaire.columns:
            df_salaire.drop(cols, axis=1, inplace=True)

    if dict_cols[3][0] in df.columns:
        df_formations = df["formations"].explode()
        df_formations = pd.json_normalize(df_formations)
    for cols in dict_cols[3][1]:
        if cols in df_formations:
            df_formations.drop(cols, axis=1, inplace=True)

    if dict_cols[4][0] in df.columns:
        df_origine = pd.json_normalize(df["origineOffre"])
    for cols in dict_cols[4][1]:
        if cols in df_origine.columns:
            df_origine.drop(cols, axis=1, inplace=True)

    df_final = pd.concat(
        [
            df,
            df_formations,
            df_salaire,
            df_entreprise,
            df_lieu_travail,
            df_origine
    ], axis=1)
    return df_final

def global_clean_pe(df_to_clean):
    # Separating dictionnaries in columns
    df_to_clean = clean_dict_columns(df_to_clean)
    cols_to_keep = create_cols_to_keep('pole emploi', df_to_clean)
    cols_to_drop = [col for col in df_to_clean.columns if col not in cols_to_keep]
    # Dropping unused columns
    df_to_clean.drop(cols_to_drop, axis=1, inplace=True)
    df = rename_and_reorder_cols("pole emploi", df_to_clean)
    # Cleaning remaining columns
    df["niveau_etudes"] = df["niveau_etudes"].astype(str)
    df["niveau_etudes"] = df["niveau_etudes"].apply(clean_experience)
    df["ville"] = df["ville"].str.title().str.replace(r"\d+", "", regex=True).str.replace("-", "").str.strip()
    df["contrat"] = df["contrat"].str.replace("MIS", "Interim").str.replace("FRA", "Autre").str.replace("LIB", "Autre")
    df[df["salaire"].isna()] = None
    df["salaire"] = df["salaire"].apply(clean_salaire_pe)
    df["secteur_activite"] = df["secteur_activite"].apply(clean_secteur_activite)

    return df

# Data Cleaning
def clean_html(text):
    '''
    Clean le html dans la description de certaines offres d'emplois.
    ---
    Paramètres
    ---
    text: texte dans lequel clean le html.
    '''
    if pd.isna(text):
        return ""

    soup = BeautifulSoup(str(text), 'html.parser')
    cleaned_text = soup.get_text(separator=" ")
    cleaned_text = cleaned_text.replace("\xa0", " ").replace("\n", "")
    return cleaned_text

def clean_experience(text) -> str:
    '''
    Nettoie la colonne "niveau_etudes".
    '''
    text = text.lower().strip()
    if "bac+5" in text or "bac_5" in text:
        return "Bac +5"
    if "bac+4" in text or "bac_4" in text:
        return "Bac +4"
    if "bac+3" in text or "bac_3" in text:
        return "Bac +3"
    if "bac+2" in text or "bac_2" in text:
        return "Bac +2"
    else:
        return "Aucune information"

def clean_date(
        df: pd.DataFrame,
    ):
    if "date_publication" in df.columns:
        df["date_publication"] = pd.to_datetime(df["date_publication"])
        df["date_publication"] = df["date_publication"].dt.strftime("%Y-%m-%d")
    return df

def clean_salaire_pe(text):
    if text is not None:
        if "Annuel" in text:
            matches = re.findall(r'\d+,\d+', text)
            if matches:
                salaries = [float(match.replace(',', '.')) for match in matches]
                average_salary = sum(salaries) / len(salaries)
                if "13 mois" in text:
                    months = "13 mois"
                else:
                    months = "12 mois"
                return f'{int(average_salary)} sur {months}'
        elif "Mensuel" in text:
            matches = re.findall(r'\d+,\d+', text)
            if matches:
                monthly_salaries = [float(match.replace(',', '.')) for match in matches]
                average_salary = sum(monthly_salaries) / len(monthly_salaries)
                average_annual_salary = average_salary * 12
                if "13 mois" in text:
                    months = "13 mois"
                else:
                    months = "12 mois"
                return f'{int(average_annual_salary)} sur {months}'
        elif "Horaire" in text:
            matches = re.findall(r'\d+,\d+', text)
            if matches:
                hourly_salaries = [float(match.replace(',', '.')) for match in matches]
                average_salary = sum(hourly_salaries) / len(hourly_salaries)
                average_annual_salary = average_salary * 35 * 52
                return f'{int(average_annual_salary)} sur 12 mois'
    return f'Salaire non indiqué'

def clean_salaire_wttj(salary_period, salary_max, salary_min):
    if salary_period is not None:
        if salary_period == "yearly":
            if salary_max is not None and salary_min is not None:
                salary = (salary_max + salary_min) / 2
                if salary < 100:
                    salary *= 1000
                return f'{int(salary)} sur 12 mois'
            else:
                return f'Salaire non indiqué'
        elif salary_period == "monthly":
            if salary_max is not None and salary_min is not None:
                monthly_max = salary_max * 12
                monthly_min = salary_min * 12
                monthly_salary = (monthly_max + monthly_min) / 2
                if monthly_salary < 100:
                    salary *= 1000
                return f'{int(monthly_salary)} sur 12 mois'
            else:
                return f'Salaire non indiqué'
    else:
        return f'Salaire non indiqué'

def clean_secteur_activite(text):
    if text == None:
        return "Aucune information"
    else:
        return text


# Reordering/renaming
def create_cols_to_keep(
        site: str,
        df: pd.DataFrame
    ) -> list:
    '''
    Créer la liste des colonnes à garder et/ou à drop pour le
    dataframe final.
    ---
    Paramètres:
    ---
    site: str: Nom du site pour lequel générer la liste (wttj,
    pole emploi ou linkedin)
    '''
    if site == "wttj":
        cols = [
        "published_at",                 # date_publication
        "contract_type",                # contract
        "name",                         # intitule
        "description",                  # description
        "organization.industry",        # secteur_activite
        "education_level",              # niveau_etudes
        "salary_period",                # salaire
        "organization.name",            # entreprise
        "organization.description",     # description_entreprise
        "office.city",                  # ville
        "link",                         # link
        "organization.logo.url",        # logo
        "salary_min",                   # salaire(2)
        "salary_max",                   # salaire(3)
        "experience_level",             # experience
    ]
    if site == "pole emploi":
        cols = [
                "dateCreation",             # date_publication
                "typeContrat",              # contrat
                "intitule",                 # intitule
                "description",              # description
                "secteurActiviteLibelle",   # secteur_activite
                "niveauLibelle",            # niveau_etudes
                "libelle",                  # salaire
                "nom",                      # entreprise
                "description_entreprise",   # description_entreprise
                "ville",                    # ville
                "urlOrigine",               # link
                "logo",                     # logo
                "experienceLibelle",        # experience
            ]
    cols_to_keep = [col for col in cols if col in df.columns]
    return cols_to_keep

def rename_and_reorder_cols(
        source: str,
        df: pd.DataFrame
    ) -> pd.DataFrame:
    '''
    Renomme les noms de colonnes du dataframe.
    ---
    Paramètres:
    ---
    source: str: Nom du site d'où proviennent les offres.
    df: pd.DataFrame: dataframe dans lequel renommer les colonnes
    ---
    Retourne
    ---
    df: pd.DataFrame: Retourne le dataframe avec les colonnes renommées.
    '''
    if source == "wttj":
        df.rename(
        {
            'published_at' : 'date_publication',
            'contract_type' : 'contrat',
            'name' : 'intitule',
            'organization.industry' : 'secteur_activite',
            'education_level' : 'niveau_etudes',
            "organization.name" : "entreprise",
            'organization.description' : 'description_entreprise',
            'office.city' : 'ville',
            'organization.logo.url' : 'logo',
            'experience_level' : 'experience',
            }, axis = 1, inplace = True
        )

    elif source == "pole emploi":
        df.rename(
                {
                    'dateCreation' : 'date_publication',
                    'urlOrigine' : 'link',
                    'typeContrat' : 'contrat',
                    "dureeTravailLibelle" : "duree_travail",
                    "dureeTravailLibelleConverti" : "type_contrat",
                    'secteurActiviteLibelle' : 'secteur_activite',
                    'niveauLibelle' : 'niveau_etudes',
                    'libelle' : 'salaire',
                    'nom' : 'entreprise',
                    "experienceLibelle" : "experience",
                }, axis = 1, inplace = True
            )
    liste_cols = reorder_cols()
    df = df[liste_cols]
    return df

def reorder_cols(
        # site: str = None
) -> list:
    '''
    Créer la liste pour l'ordre dans lequel afficher les colonnes
    du dataframe.
    ---
    Retourne:
    ---
    liste_cols: list: Liste contenant les colonnes dans l'ordre défini.
    '''
    liste_cols = [
        "date_publication",
        "contrat",
        "intitule",
        "description",
        "secteur_activite",
        "niveau_etudes",
        "experience",
        "salaire",
        "entreprise",
        "description_entreprise",
        "ville",
        "link",
        "logo",
    ]
    return liste_cols


# Skill extract
def tech_skills_list():
    technical_skills = [
        'python',
        'flask',
        'pandas',
        'spark',
        'scikit-learn',
        'numpy',
        'sql',
        'mysql',
        'nltk',
        'fastapi',
        'pytorch',
        'snowflake',
        'pandas',
        'rivery',
        'django',
        'react',
        'html',
        'machine',
        'learning',
        'tableau',
        'power',
        'bi',
        'powerbi',
        'looker',
        'warehouse',
        'dbt',
        'ia',
        'ai',
        'dataiku',
        'iku',
        'r',
        'datalake',
        'lake',
        'lake',
        'data',
        'ml',
        'scala',
        'api',
        'aws'
    ]
    return technical_skills

def soft_skills_list():
    soft_skills = [
        'resolution',
        'probleme',
        'autonomie',
        'organisation',
        'rigueur',
        'initiative',
        "esprit",
        "equipe",
        'communication',
        'creativite',
        'critique',
        'confiance',
        'soi',
        'adaptation',
        'gestion',
        'temps',
        'stress',
        'empathie',
        'curiosite',
    ]
    return soft_skills

def clean_description(df):
    if not nltk.data.find("corpora/stopwords"):
        nltk.download("stopwords")
    stop = stopwords.words("french")
    df["description_cleaned"] = df["description"].apply(
        lambda x: " "
        .join(x.lower() for x in x.split())
    ).str.replace('[^\w\s]', ' ', regex=True).apply(lambda x: ' '.join(
        x for x in x.split() if x not in stop)
    )
    return df

def extract_tech_skills(description):
    skills = set(description.lower().split())
    technical_skills = tech_skills_list()
    extracted_skills = list(skills.intersection(technical_skills))
    extracted_skills = regroup_tech_skills(extracted_skills)
    return extracted_skills

def extract_soft_skills(description):
    skills = set(description.lower().split())
    soft_skills = soft_skills_list()
    extracted_skills = list(skills.intersection(soft_skills))
    extracted_skills = regroup_soft_skills(extracted_skills)
    return extracted_skills

def regroup_tech_skills(tech_skills):
    if "machine" in tech_skills and "learning" in tech_skills:
        tech_skills.append("Machine Learning")
        tech_skills.remove("machine")
        tech_skills.remove("learning")
    if "power" in tech_skills and "bi" in tech_skills:
        tech_skills.append("Power BI")
        tech_skills.remove("power")
        tech_skills.remove("bi")
    if "data" in tech_skills and "iku" in tech_skills:
        tech_skills.append("Dataiku")
        tech_skills.remove("iku")
    if "data" in tech_skills and "lake" in tech_skills:
        tech_skills.append("Datalake")
        tech_skills.remove("lake")
    return tech_skills

def regroup_soft_skills(soft_skills):
    if "resolution" in soft_skills and "probleme" in soft_skills:
        soft_skills.append("Resolution de problemes")
        soft_skills.remove("resolution")
        soft_skills.remove("probleme")
    if "esprit" in soft_skills and "equipe" in soft_skills:
        soft_skills.append("Esprit d'equipe")
        soft_skills.remove("equipe")
    if "esprit" in soft_skills and "critique" in soft_skills:
        soft_skills.append("Esprit critique")
        soft_skills.remove("critique")
    if "confiance" in soft_skills and "soi" in soft_skills:
        soft_skills.append("Confiance en soi")
        soft_skills.remove("confiance")
        soft_skills.remove("soi")
    if "gestion" in soft_skills and "temps" in soft_skills:
        soft_skills.append("Gestion du temps")
        soft_skills.remove("gestion")
        soft_skills.remove("temps")
    if "esprit" in soft_skills:
        soft_skills.remove("esprit")
    if "gestion" in soft_skills:
        soft_skills.remove("gestion")
    return soft_skills

def extract_skills(df):
    logging.info("Extracting Skills from description...")
    if "description" in df.columns:
        df.dropna(subset="description", inplace = True)
        df = clean_description(df)
        df["tech_skills"] = df["description_cleaned"].apply(extract_tech_skills)
        df["soft_skills"] = df["description_cleaned"].apply(extract_soft_skills)
        df.drop("description_cleaned", axis=1, inplace=True)
        logging.info("Skills extracted!")
    else:
        logging.info("""
                     The dataframe does not have a description
                     to extract skills from.
                     """)

    return df


# SQL
def create_sql_table(df):
    df['tech_skills'] = df['tech_skills'].apply(json.dumps)
    df['soft_skills'] = df['soft_skills'].apply(json.dumps)
    engine = sqlalchemy.create_engine('sqlite:///database/job_offers.sqlite')
    df.to_sql('all_jobs', con=engine, index=False, if_exists='replace')

# def create_sql_table(
#         source: str,
#         df: pd.DataFrame,
#         job_title: str = 'data analyst'
#     ):
#     '''
#     Créer ou remplace les tables dans la Base de Données SQL.
#     ---
#     Paramètres
#     ---
#     source: str: Site d'où proviennent les offres.
#     df: pd.DataFrame: Dataframe à partir du quel créer la table.
#     job_title: Intitulé du poste auquel les offres correspondent.
#     '''
#     logging.info("Creating SQL Database...")
#     engine = sqlalchemy.create_engine('sqlite:///database/job_offers.db')
#     if source == "pole emploi":
#         df.to_sql(f'pole_emploi_{job_title}', con=engine, index=False, if_exists='replace')
#         logging.info(f"Table {source} for {job_title} created!")
#     elif source == "wttj":
#         df.to_sql(f'wttj_{job_title}', con=engine, index=False, if_exists='replace')
#         logging.info(f"Table {source} for {job_title} created!")
#     else:
#         logging.info('Source not found.')



