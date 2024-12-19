import logging
from ast import literal_eval
import s3fs
import pandas as pd
import pyarrow as pa
from dateutil.parser import parse
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    LongType,
    StringType,
    DateType
)
from pyiceberg.exceptions import (
    TableAlreadyExistsError, 
    NamespaceAlreadyExistsError
)
import settings
from scripts.utils import remove_acento


log = logging.getLogger(__name__)


def main():
    log.info("Script started")
    database = "trusted"
    
    egressos_table_name = "egressos_linkedin"
    egressos_educacao_table_name = "egressos_linkedin_educacao"
    egressos_experiencia_table_name = "egressos_linkedin_experiencia"
    
    egressos_table_location = "s3://ppgti-igd-trusted-zone/linkedin/egressos_linkedin/"
    egressos_educacao_table_location = "s3://ppgti-igd-trusted-zone/linkedin/egressos_linkedin_educacao/"
    egressos_experiencia_table_location = "s3://ppgti-igd-trusted-zone/linkedin/egressos_linkedin_experiencia/"

    input_prefix = "s3://ppgti-igd-raw-zone/linkedin/egressos/"

    
    log.info("Creating Icebeg Table on Glue Catalog")
    catalog = load_catalog("glue", **{"type": "glue"})
    try:
        catalog.create_namespace(database)
    except NamespaceAlreadyExistsError:
        pass
    
    log.info("Reading dataset from Raw Zone")
    s3 = s3fs.S3FileSystem()
    str_dates = [key.split("/")[-1] for key in s3.ls(input_prefix)]
    max_date = max(map(parse, str_dates)).strftime("%Y-%m-%d")
    object_key = input_prefix + f"{max_date}/egressos.parquet"

    log.info("Reading dataset from Raw Zone")
    egressos_df = pd.read_parquet(object_key)

    ############ Egressos Linkedin ############

    log.info("Transforming dataset Egressos Linkedin")
    egressos_df = egressos_df.rename(columns={
        "firstName": "nome",
        "lastName": "sobrenome",
        "projects": "projetos",
        "certifications": "certificacoes",
        "education": "educacao",
        "experience": "experiencia",
        "summary": "descricao",
        "publications": "publicacoes",
        "honors": "premios",
        "languages": "idiomas",
        "volunteer": "voluntariados",
    })
    create_url = lambda urn: f"https://www.linkedin.com/in/{urn}/"
    egressos_df["url"] = egressos_df["profile_id"].apply(create_url)
 
    # Normalização do nome completo
    egressos_df["nome"] = egressos_df["nome"] + " " + egressos_df["sobrenome"]
    egressos_df.loc[~egressos_df["nome"].isna(), "nome"] = egressos_df.loc[~egressos_df["nome"].isna(), "nome"].str.strip().str.lower().apply(remove_acento)

    egressos_df.loc[~egressos_df["nome"].isna(), "sobrenome"] = egressos_df.loc[~egressos_df["nome"].isna(), "nome"].str.split(" ", n=1).str[1]
    egressos_df.loc[~egressos_df["nome"].isna(), "nome"] = egressos_df.loc[~egressos_df["nome"].isna(), "nome"].str.split(" ", n=1).str[0]

    egressos_df = egressos_df[["url", "nome", "sobrenome", "headline", "projetos", "certificacoes", "educacao", "experiencia", "descricao", "publicacoes", "premios", "idiomas", "voluntariados"]]

    
    egressos_table_schema = Schema(
        NestedField(field_id=1, name="url", field_type=StringType()),
        NestedField(field_id=2, name="nome", field_type=StringType()),
        NestedField(field_id=3, name="sobrenome", field_type=StringType()),
        NestedField(field_id=4, name="headline", field_type=StringType()),
        NestedField(field_id=5, name="projetos", field_type=StringType()),
        NestedField(field_id=6, name="certificacoes", field_type=StringType()),
        NestedField(field_id=7, name="educacao", field_type=StringType()),
        NestedField(field_id=8, name="experiencia", field_type=StringType()),
        NestedField(field_id=9, name="descricao", field_type=StringType()),
        NestedField(field_id=10, name="publicacoes", field_type=StringType()),
        NestedField(field_id=10, name="premios", field_type=StringType()),
        NestedField(field_id=10, name="idiomas", field_type=StringType()),
        NestedField(field_id=10, name="voluntariados", field_type=StringType()),
    )
    
    try:
        egressos_table = catalog.create_table(
            f"{database}.{egressos_table_name}",
            schema=egressos_table_schema,
            location=egressos_table_location,
        )
    except TableAlreadyExistsError:
        egressos_table = catalog.load_table(f"{database}.{egressos_table_name}")

    egressos_pylist = egressos_df.astype(str).to_dict(orient="records")
    egressos_pa = pa.Table.from_pylist(egressos_pylist)
    egressos_table.overwrite(egressos_pa)

    ############ Egressos Linkedin Educacao ############

    log.info("Transforming dataset Egressos Linkedin Educacao")
    egressos_df["educacao"] = egressos_df["educacao"].apply(literal_eval)
    egressos_educacao_df = egressos_df.explode("educacao")[["url", "educacao"]]
    educacao_attibutos_df = egressos_educacao_df["educacao"].apply(pd.Series)
    egressos_educacao_df = pd.concat([egressos_educacao_df, educacao_attibutos_df], axis=1)

    egressos_educacao_df = egressos_educacao_df.rename(columns={
        "fieldOfStudy": "curso",
        "schoolName": "instituicao",
    })
    egressos_educacao_df = egressos_educacao_df[["url", "curso", "instituicao"]]

    
    egressos_educacao_table_schema = Schema(
        NestedField(field_id=1, name="url", field_type=StringType()),
        NestedField(field_id=2, name="curso", field_type=StringType()),
        NestedField(field_id=3, name="instituicao", field_type=StringType()),
    )

    try:
        egressos_educacao_table = catalog.create_table(
            f"{database}.{egressos_educacao_table_name}",
            schema=egressos_educacao_table_schema,
            location=egressos_educacao_table_location,
        )
    except TableAlreadyExistsError:
        egressos_educacao_table = catalog.load_table(f"{database}.{egressos_educacao_table_name}")

    egressos_educacao_df = egressos_educacao_df.astype(str)
    egressos_educacao_pylist = egressos_educacao_df.to_dict(orient="records")
    egressos_educacao_pa = pa.Table.from_pylist(egressos_educacao_pylist)
    egressos_educacao_table.overwrite(egressos_educacao_pa)

    ############ Egressos Linkedin Experiencia ############

    log.info("Transforming dataset Egressos Linkedin Experiencia")
    egressos_df.loc[~egressos_df["experiencia"].isna(), "experiencia"] = egressos_df.loc[~egressos_df["experiencia"].isna(), "experiencia"].apply(literal_eval)
    egressos_experiencia_df = egressos_df.explode("experiencia")[["url", "experiencia"]]
    experiencia_attibutos_df = egressos_experiencia_df["experiencia"].apply(pd.Series)
    egressos_experiencia_df = pd.concat([egressos_experiencia_df, experiencia_attibutos_df], axis=1)

    egressos_experiencia_df = egressos_experiencia_df.rename(columns={
        "companyName": "empresa",
        "title": "titulo",
        "geoLocationName": "local",
    })
    egressos_experiencia_df = egressos_experiencia_df[["url", "titulo", "empresa", "local"]]


    egressos_experiencia_table_schema = Schema(
        NestedField(field_id=1, name="url", field_type=StringType()),
        NestedField(field_id=2, name="titulo", field_type=StringType()),
        NestedField(field_id=3, name="empresa", field_type=StringType()),
        NestedField(field_id=4, name="local", field_type=StringType()),
    )

    try:
        egressos_experiencia_table = catalog.create_table(
            f"{database}.{egressos_experiencia_table_name}",
            schema=egressos_experiencia_table_schema,
            location=egressos_experiencia_table_location,
        )
    except TableAlreadyExistsError:
        egressos_experiencia_table = catalog.load_table(f"{database}.{egressos_experiencia_table_name}")

    egressos_experiencia_df = egressos_experiencia_df.astype(str)
    egressos_experiencia_pylist = egressos_experiencia_df.to_dict(orient="records")
    egressos_experiencia_pa = pa.Table.from_pylist(egressos_experiencia_pylist)
    egressos_experiencia_table.overwrite(egressos_experiencia_pa)

    log.info(f"Finished")