import logging
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
    table_name = "egressos_suap"
    input_prefix = "s3://ppgti-igd-raw-zone/suap/egressos/"
    output_key = "s3://ppgti-igd-trusted-zone/suap/egressos_suap/"
    
    # Icebeg Schema egressos_suap (trusted zone)
    schema = Schema(
        NestedField(field_id=1, name="matricula", field_type=LongType()),
        NestedField(field_id=2, name="nome", field_type=StringType()),
        NestedField(field_id=3, name="sobrenome", field_type=StringType()),
        NestedField(field_id=4, name="email", field_type=StringType()),
        NestedField(field_id=5, name="campus", field_type=StringType()),
        NestedField(field_id=6, name="curso", field_type=StringType()),
        NestedField(field_id=7, name="situacao", field_type=StringType()),
        NestedField(field_id=8, name="situacao_ultimo_periodo", field_type=StringType()),
        NestedField(field_id=9, name="ano_conclusao", field_type=LongType()),
        NestedField(field_id=10, name="dt_matricula", field_type=StringType()),
    )

    s3 = s3fs.S3FileSystem()
    str_dates = [key.split("/")[-1] for key in s3.ls(input_prefix)]
    max_date = max(map(parse, str_dates)).strftime("%Y-%m-%d")
    object_key = input_prefix + f"{max_date}/egressos.parquet"
    
    log.info("Reading dataset from Raw Zone")
    df = pd.read_parquet(object_key)

    log.info("Transforming dataset")
    df = df.rename(columns={
            "Matrícula": "matricula",
            "Nome": "nome",
            "Campus": "campus",
            "Curso": "curso",
            "Situação": "situacao",
            "Situação no último período": "situacao_ultimo_periodo",
            "Ano de conclusão": "ano_conclusao",
            "Data da matrícula": "dt_matricula",
            "E-mail pessoal": "email"
        },
    )

    # Normalização do nome completo
    df.loc[~df["nome"].isna(), "nome"] = df.loc[~df["nome"].isna(), "nome"].str.strip().str.lower().apply(remove_acento)

    df.loc[~df["nome"].isna(), "sobrenome"] = df.loc[~df["nome"].isna(), "nome"].str.split(" ", n=1).str[1]
    df.loc[~df["nome"].isna(), "nome"] = df.loc[~df["nome"].isna(), "nome"].str.split(" ", n=1).str[0]

    df = df[["matricula", "nome", "sobrenome", "email", "campus", "curso", "situacao", "situacao_ultimo_periodo", "ano_conclusao", "dt_matricula"]]

    # Define Schema
    df["matricula"] = df["matricula"].astype(int)
    df["ano_conclusao"] = df["matricula"].astype(int)

    log.info("Creating Icebeg Table on Glue Catalog")
    catalog = load_catalog("glue", **{"type": "glue"})
    try:
        catalog.create_namespace(database)
    except NamespaceAlreadyExistsError:
        pass
    
    try:
        table = catalog.create_table(
            f"{database}.{table_name}",
            schema=schema,
            location=output_key,
        )
    except TableAlreadyExistsError:
        table = catalog.load_table(f"{database}.{table_name}")

    log.info("Writing Iceberg Table on Trusted Zone")
    
    df = df.fillna("")
    pylist = df.to_dict(orient="records")
    pa_df = pa.Table.from_pylist(pylist)
    table.overwrite(pa_df)

    log.info(f"Finished")