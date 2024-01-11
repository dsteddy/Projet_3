import asyncio
# from offres_emploi.utils import dt_to_str_iso
# import datetime

from scrapping import (
    create_cols_to_keep,
    fetch_all,
    job_offers_wttj,
    job_offers_pole_emploi,
)

import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Choix de l'intitulé du poste
job_title = "data analyst"

# Scrapping Welcome To The Jungle
cols_to_keep = create_cols_to_keep('wttj')
api_links = job_offers_wttj(job_title)

df_wttj = asyncio.run(fetch_all(api_links, cols_to_keep))

# Scrapping Pole Emploi
cols_to_drop = create_cols_to_keep('pole emploi')

# start_dt = datetime.datetime(2023,9,1,12,30)
# end_dt = datetime.datetime.today()
params = {
    "motsCles": job_title,
    # "minCreationDate" : dt_to_str_iso(start_dt),
    # "maxCreationDate" : dt_to_str_iso(end_dt),
}
df_pole_emploi = job_offers_pole_emploi(params, cols_to_drop)

logging.info("Saving CSV files...")
df_pole_emploi.to_parquet('datasets/pole_emploi_offers.parquet', index=False)
df_wttj.to_parquet('datasets/WTTJ_offers.parquet', index=False)
logging.info("CSV file created!")