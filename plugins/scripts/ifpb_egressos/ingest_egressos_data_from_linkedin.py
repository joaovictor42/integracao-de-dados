from datetime import datetime
import logging
from airflow.hooks.base import BaseHook
import pandas as pd
import s3fs
import settings
from utils import LinkedinFix
from pyiceberg.catalog import load_catalog


log = logging.getLogger(__name__)

def get_profiles():
    log.info("Lendo nome da tabela trusted.egressos_suap")
    linkedin_connection = BaseHook.get_connection('linkedin')
    linkedin = LinkedinFix(linkedin_connection.login, linkedin_connection.password)

    catalog = load_catalog("glue", **{"type": "glue"})
    egressos_suap_table = catalog.load_table("trusted.egressos_suap")
    egressos_suap_df = egressos_suap_table.scan(selected_fields=('nome',)).to_pandas()

    log.info("Pesquisando Pessoas Linkedin")
    egressos = []
    for nome in egressos_suap_df["nome"].unique():
        log.info(f"Pesquisando {nome}")
        response = linkedin.search_people(
            keyword_first_name=nome,
            schools=['33252864'],  # IFPB
            include_private_profiles=True
        )
    egressos.extend(response)

    log.info("Extraindo Perfis")
    perfil_egressos = []
    for egresso in egressos:
        log.info(f"Extraindo perfil {egresso["urn_id"]}")
        perfil = linkedin.get_profile(egresso["urn_id"])
        perfil_egressos.append(perfil)

    return pd.DataFrame(perfil_egressos)


def main():
    log.info("Script started")
    output_prefix = "s3://ppgti-igd-landing-zone/linkedin/egressos/"
    current_date = datetime.now().strftime('%Y-%m-%d')
    output_key = output_prefix + f"{current_date}/egressos.csv"

    log.info("Scraping Linkedin")
    df = get_profiles()

    log.info("Writing dataset on Landing Zone")
    df.to_csv(output_key, index=False)
    log.info("Finished")