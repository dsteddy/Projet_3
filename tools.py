from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options

from nltk.corpus import stopwords

from offres_emploi.utils import dt_to_str_iso

from dotenv import load_dotenv
import os

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
def scrapping(
        job_title: str,
        page : int = None
    ):
    '''
    Fonction principale pour récupérer les infos et créer les bases de
    données.
    ---
    Paramètres:
    ---
    job_title: str: Intitulé du poste pour lequel rechercher les offres
    d'emplois sur les différents sites.
    '''
    if job_title == 'all':
        # Data Analyst
        logging.info("-----")
        logging.info("Data Analyst")
        wttj_analyst = job_offers_wttj("data+analyst")
        wttj_analyst = clean_date(wttj_analyst)

        params = {
            "motsCles": "data analyst",
            'minCreationDate': dt_to_str_iso(datetime.datetime(2023, 12, 1, 12, 30)),
            'maxCreationDate': dt_to_str_iso(datetime.datetime.today()),
        }
        pe_analyst = job_offers_pole_emploi(params)
        pe_analyst = clean_date(pe_analyst)

        # Data Engineer
        logging.info("-----")
        logging.info("Data Engineer")
        wttj_engineer = job_offers_wttj("data+engineer")
        wttj_engineer = clean_date(wttj_engineer)

        params = {
            "motsCles": "data engineer",
            'minCreationDate': dt_to_str_iso(datetime.datetime(2023, 9, 1, 12, 30)),
            'maxCreationDate': dt_to_str_iso(datetime.datetime.today()),
        }
        pe_engineer = job_offers_pole_emploi(params)
        pe_engineer = clean_date(pe_engineer)

        # Data Scientist
        logging.info("-----")
        logging.info("Data Scientist")
        wttj_scientist = job_offers_wttj("data+scientist")
        wttj_scientist = clean_date(wttj_scientist)

        params = {
            "motsCles": "data scientist",
            'minCreationDate': dt_to_str_iso(datetime.datetime(2023, 9, 1, 12, 30)),
            'maxCreationDate': dt_to_str_iso(datetime.datetime.today()),
        }
        pe_scientist = job_offers_pole_emploi(params)
        pe_scientist = clean_date(pe_scientist)


        # Concat all
        logging.info("Concat dataframes...")
        df = pd.concat([wttj_analyst, wttj_engineer, wttj_scientist, pe_analyst, pe_engineer, pe_scientist], ignore_index=True)
        logging.info("Dropping duplicates...")
        df = df.drop_duplicates(subset="description", keep="first")
        df.to_parquet(f'datasets/all_jobs.parquet', index=False)
        logging.info("Finished!")
    else:
        job_title_nom_fichier = job_title.replace(" ", "_")

        # WTTJ
        df_wttj = job_offers_wttj(job_title, page)
        df_wttj = clean_date(df_wttj)
        df_wttj.to_parquet(f'datasets/WTTJ_{job_title_nom_fichier}.parquet', index=False)
        # df_wttj.to_csv(f'datasets/WTTJ_{job_title_nom_fichier}.csv', index=False)
        # create_sql_table("wttj", df_wttj, job_title_nom_fichier)

        # Pole Emploi
        params = {
            "motsCles": "data analyst",
            'minCreationDate': dt_to_str_iso(datetime.datetime(2023, 9, 1, 12, 30)),
            'maxCreationDate': dt_to_str_iso(datetime.datetime.today()),
        }
        df_pole_emploi = job_offers_pole_emploi(params)
        df_pole_emploi = clean_date(df_pole_emploi)
        # df_pole_emploi["date_publication"] = pd.to_datetime(df_pole_emploi["date_publication"])
        # df_pole_emploi["date_publication"] = df_pole_emploi["date_publication"].dt.strftime("%Y-%m-%d")
        # df_pole_emploi["date_modif"] = df_pole_emploi["date_modif"].dt.strftime("%Y-%m-%d")
        df_pole_emploi.to_parquet(f'datasets/pole_emploi_{job_title_nom_fichier}.parquet', index=False)
        # df_pole_emploi.to_csv(f'datasets/pole_emploi_{job_title_nom_fichier}.csv', index=False)
        # create_sql_table("pole emploi", df_pole_emploi, job_title_nom_fichier)

        # Concat both
        df = pd.concat([df_wttj, df_pole_emploi], ignore_index=True)
        df.to_parquet(f'datasets/{job_title_nom_fichier}.parquet', index=False)


# Welcome To The Jungle
def job_offers_wttj(
        job_title: str = "data analyst",
        page : int = None,
    ) -> pd.DataFrame:
    '''
    Scrapping de toutes les offres d'emploi du site Welcome To The Jungle
    pour le job indiqué.
    ---
    Paramètres:
    ---
    job_title: str: Nom de l'intitulé du job pour lequel rechercher les
    offres.
    ---
    Retourne:
    ---
    df: pd.DataFrame: dataframe contenant les informations de chaque offres
    d'emploi trouvée.
    '''
    cols_to_keep = create_cols_to_keep('wttj')
    # Instanciation de la liste contenant les liens pour les requêtes APIs.
    api_links = []
    # Lien de l'API de Welcome To The Jungle pour récupérer les données.
    api_link = f"https://api.welcometothejungle.com/api/v1/organizations"
    job = job_title.lower().replace(" ", "+")
    # Instanciation du driver Firefox.
    firefox_options = Options()
    firefox_options.headless = True
    driver = webdriver.Firefox(options=firefox_options)
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
    df = asyncio.run(fetch_all(api_links, cols_to_keep))
    return df

async def fetch_all(
        api_links:list,
        cols_to_keep:list
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
    logging.info("Concatening dataframes...")
    full_df = pd.concat([pd.json_normalize(resp["job"]) for resp in responses if "job" in resp], ignore_index=True)
    cols_to_drop = [col for col in full_df.columns if col not in cols_to_keep]
    df_urls = pd.json_normalize(full_df["urls"])
    df_urls["link"] = df_urls[0]
    df_urls = df_urls.drop(columns=[0,1])
    df_urls["link"] = df_urls["link"].apply(clean_link)
    # full_df.drop("urls", inplace=True, axis=1)
    url_merged = pd.concat([full_df, df_urls], axis=1)
    df = url_merged.drop(columns=cols_to_drop)
    logging.info("DataFrame done!")
    df["description"] = df["description"].apply(clean_html)
    df["organization.description"] = df["organization.description"].apply(clean_html)
    df = rename_and_reorder_cols("wttj", df)
    df["niveau_etudes"] = df["niveau_etudes"].astype(str)
    df["niveau_etudes"] = df["niveau_etudes"].apply(clean_experience)
    df["contrat"] = df["contrat"].str.replace(
        "full_time", "CDI"
        ).str.replace("internship", "Stage"
        ).str.replace("apprenticeship", "Alternance"
        ).str.replace("temporary", "CDD"
        ).str.replace("other", "Autre"
        ).str.replace("vie", "CDI"
    )
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
    cols_to_drop = create_cols_to_keep('pole emploi')
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

        if max_results > 0:
            df_emploi = pd.DataFrame(results)
            df_final = clean_dict_columns(df_emploi)
            df_final.drop(cols_to_drop, axis=1, inplace=True)
            df_final = rename_and_reorder_cols("pole emploi", df_final)
            logging.info("Dataframe Created!")
            df_final["description"] = df_final["description"].apply(clean_html)
            # df_final["code_postal"] = df_final["code_postal"].astype(str)
            df_final["niveau_etudes"] = df_final["niveau_etudes"].astype(str)
            df_final["niveau_etudes"] = df_final["niveau_etudes"].apply(clean_experience)
            df_final["ville"] = df_final["ville"].str.title().str.replace(r"\d+", "", regex=True).str.replace("-", "").str.strip()
            df_final["contrat"] = df_final["contrat"].str.replace("MIS", "Interim").str.replace("FRA", "Autre").str.replace("LIB", "Autre")
            return df_final

        else:
            print("Aucune offre d'emploi trouvée.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Une erreur s'est produite lors de la recherche : {e}")

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
    df_origine = pd.json_normalize(df["origineOffre"])
    df_origine.drop(["origine", "partenaires"], axis=1, inplace=True)
    df_final = pd.concat([df, df_formations, df_salaire, df_entreprise, df_lieu_travail, df_origine], axis=1)
    return df_final


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
        return None

def clean_date(
        df: pd.DataFrame,
    ):
    df["date_publication"] = pd.to_datetime(df["date_publication"])
    df["date_publication"] = df["date_publication"].dt.strftime("%Y-%m-%d")
    return df


# Reordering/renaming
def create_cols_to_keep(
        site: str
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
        cols_to_keep = [
        # "urls",
        "published_at",                 # date_publication
        # "updated_at",                   # date_modif
        "name",                         # intitule
        "salary_period",                # salaire
        "office.city",                  # ville
        "office.zip_code",              # code postal
        "education_level",              # niveau_etudes
        "description",                  # description
        "organization.name",            # entreprise
        "organization.description",     # description_entreprise
        "organization.industry",        # secteur_activite
        "contract_type",                # contract_type
        "salary_min",                   # salaire(2)
        "salary_max",                   # salaire(3)
        "link",
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
                "complementExercice",
                "competences",
                "agence",
                "dateActualisation",
            ]
        return cols_to_drop

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
            # 'urls' : 'link',
            'education_level' : 'niveau_etudes',
            'contract_type' : 'contrat',
            'name' : 'intitule',
            'published_at' : 'date_publication',
            # 'updated_at' : 'date_modif',
            'experience_level' : 'experience',
            'office.city' : 'ville',
            'office.zip_code' : 'code_postal',
            "organization.name" : "entreprise",
            'organization.description' : 'description_entreprise',
            'organization.industry' : 'secteur_activite',
            }, axis = 1, inplace = True
        )
        liste_cols = reorder_cols(source)
        df = df[liste_cols]
        return df
    elif source == "pole emploi":
        df.rename(
                {
                    'dateCreation' : 'date_publication',
                    # 'dateActualisation' : 'date_modif',
                    'urlOrigine' : 'link',
                    'typeContrat' : 'contrat',
                    "dureeTravailLibelle" : "duree_travail",
                    "dureeTravailLibelleConverti" : "type_contrat",
                    'secteurActiviteLibelle' : 'secteur_activite',
                    'niveauLibelle' : 'niveau_etudes',
                    'libelle' : 'salaire',
                    'nom' : 'entreprise',
                    # 'codePostal' : 'code_postal'
                }, axis = 1, inplace = True
            )
        liste_cols = reorder_cols()
        df = df[liste_cols]
        return df

def reorder_cols(
        site: str = None
) -> list:
    '''
    Créer la liste pour l'ordre dans lequel afficher les colonnes
    du dataframe.
    ---
    Retourne:
    ---
    liste_cols: list: Liste contenant les colonnes dans l'ordre défini.
    '''
    if site == "wttj":
        liste_cols = [
            "date_publication",
            "contrat",
            # "type_contrat",
            "intitule",
            "description",
            "secteur_activite",
            "niveau_etudes",
            "salary_period",
            "salary_min",                   # salaire(2)
            "salary_max",                   # salaire(3)
            "entreprise",
            "description_entreprise",
            "ville",
            "link",
            # "code_postal",
            # "date_modif",
        ]

    else:
        liste_cols = [
            "date_publication",
            "contrat",
            # "type_contrat",
            "intitule",
            "description",
            "secteur_activite",
            "niveau_etudes",
            "salaire",
            # "salary_min",                   # salaire(2)
            # "salary_max",                   # salaire(3)
            "entreprise",
            "description_entreprise",
            "ville",
            "link",
            # "code_postal",
            # "date_modif",
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
    return extracted_skills

def extract_soft_skills(description):
    skills = set(description.lower().split())
    soft_skills = soft_skills_list()
    extracted_skills = list(skills.intersection(soft_skills))
    return extracted_skills

def regroup_tech_skills(tech_skills):
    if "machine" in tech_skills and "learning" in tech_skills:
        tech_skills.append("machine learning")
        tech_skills.remove("machine")
        tech_skills.remove("learning")
    if "power" in tech_skills and "bi" in tech_skills:
        tech_skills.append("power bi")
        tech_skills.remove("power")
        tech_skills.remove("bi")
    if "data" in tech_skills and "iku" in tech_skills:
        tech_skills.append("dataiku")
        tech_skills.remove("iku")
    if "data" in tech_skills and "lake" in tech_skills:
        tech_skills.append("datalake")
        tech_skills.remove("lake")
    return tech_skills

def regroup_soft_skills(soft_skills):
    if "resolution" in soft_skills and "probleme" in soft_skills:
        soft_skills.append("resolution de problemes")
        soft_skills.remove("resolution")
        soft_skills.remove("probleme")
    if "esprit" in soft_skills and "equipe" in soft_skills:
        soft_skills.append("esprit d'equipe")
        soft_skills.remove("equipe")
    if "esprit" in soft_skills and "critique" in soft_skills:
        soft_skills.append("esprit critique")
        soft_skills.remove("critique")
    if "confiance" in soft_skills and "soi" in soft_skills:
        soft_skills.append("confiance en soi")
        soft_skills.remove("confiance")
        soft_skills.remove("soi")
    if "gestion" in soft_skills and "temps" in soft_skills:
        soft_skills.append("gestion du temps")
        soft_skills.remove("gestion")
        soft_skills.remove("temps")
    if "esprit" in soft_skills:
        soft_skills.remove("esprit")
    if "gestion" in soft_skills:
        soft_skills.remove("gestion")
    return soft_skills


# SQL
def create_sql_table(
        source: str,
        df: pd.DataFrame,
        job_title: str = 'data analyst'
    ):
    '''
    Créer ou remplace les tables dans la Base de Données SQL.
    ---
    Paramètres
    ---
    source: str: Site d'où proviennent les offres.
    df: pd.DataFrame: Dataframe à partir du quel créer la table.
    job_title: Intitulé du poste auquel les offres correspondent.
    '''
    logging.info("Creating SQL Database...")
    engine = sqlalchemy.create_engine('sqlite:///database/job_offers.db')
    if source == "pole emploi":
        df.to_sql(f'pole_emploi_{job_title}', con=engine, index=False, if_exists='replace')
        logging.info(f"Table {source} for {job_title} created!")
    elif source == "wttj":
        df.to_sql(f'wttj_{job_title}', con=engine, index=False, if_exists='replace')
        logging.info(f"Table {source} for {job_title} created!")
    else:
        logging.info('Source not found.')



