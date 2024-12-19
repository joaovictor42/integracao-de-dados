import logging
import pandas as pd
import settings
from pyiceberg.catalog import load_catalog


log = logging.getLogger(__name__)


def create_json(row):
    return {
        "id": row["matricula"],
        "nomeCompactado": row["nome"].capitalize(),
        "nome": (row["nome"] + " " + row["sobrenome"]).title(),
        "email": row["email"],
        "curso": row["singla_curso"],
        "campus": "ifpb-jp",
        "egresso": True,
        "linkedin": row["url"]
    }


def main():
    log.info("Script started")
    output_object = 's3://ppgti-igd-2024/tmp/github_ifpb/egressos/egressos.json'

    log.info("Reading tables")
    catalog = load_catalog("glue", **{"type": "glue"})
    egressos_suap_table = catalog.load_table("trusted.egressos_suap")
    egressos_linkedin_table = catalog.load_table("trusted.egressos_linkedin")

    egressos_suap_df = egressos_suap_table.scan().to_pandas()
    egressos_linkedin_df = egressos_linkedin_table.scan().to_pandas()

    log.info("Integrating dataset Egressos")
    integrated_df = egressos_suap_df.merge(egressos_linkedin_df, on=['nome', 'sobrenome'], how='inner')

    id2sigla = {
        "984": 'mpti',
        "38": 'cstrc',
        "37": 'cstsi',
    }

    integrated_df['id_curso'] = integrated_df['curso'].str.split(" ", n=1).str[0]
    integrated_df['singla_curso'] = integrated_df['id_curso'].map(id2sigla)

    log.info("Writing dataset on Landing Zone")
    integrated_df["json"] = integrated_df.apply(create_json, axis=1)
    json_df = pd.DataFrame(integrated_df["json"].to_list())
    json_df.to_json(output_object, orient='records')
    log.info("Finished")