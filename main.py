from tools import scrapping

import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Choix de l'intitulé du poste
job_title = "data analyst"
# job_title = "all"
logging.info(f"Scraping job offers for {job_title}")

scrapping(
    job_title,
    # page=1
)
