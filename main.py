import asyncio
from offres_emploi.utils import dt_to_str_iso
import datetime

from scrapping import (
    create_cols_to_keep,
    fetch_all,
    job_offers_wtj,
    job_offers_pole_emploi,
)

# Choix de l'intitulé du poste
job_title = "data analyst"

# Scrapping Welcome To The Jungle
cols_to_keep = create_cols_to_keep('wtt')
api_links = job_offers_wtj(job_title, 10)

df_wtt = asyncio.run(fetch_all(api_links, cols_to_keep))
df_wtt.to_csv('WTT_offers.csv', index=False)

# Scrapping Pole Emploi
cols_to_drop = create_cols_to_keep('pole emploi')

start_dt = datetime.datetime(2023,9,1,12,30)
end_dt = datetime.datetime.today()
params = {
    "motsCles": job_title,
    "minCreationDate" : dt_to_str_iso(start_dt),
    "maxCreationDate" : dt_to_str_iso(end_dt),
}
df_pole_emploi = job_offers_pole_emploi(params, cols_to_drop)
df_pole_emploi.to_csv('Pole_Emploi_offers.csv', index=False)
