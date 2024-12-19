import logging
import pandas as pd
import s3fs
from dateutil.parser import parse
import settings


log = logging.getLogger(__name__)


def main():    
    log.info("Script started")
    input_prefix = "s3://ppgti-igd-landing-zone/suap/egressos/"
    output_prefix = "s3://ppgti-igd-raw-zone/suap/egressos/"

    log.info("Reading dataset from Landing Zone")
    s3 = s3fs.S3FileSystem()
    str_dates = [key.split("/")[-1] for key in s3.ls(input_prefix)]
    max_date = max(map(parse, str_dates)).strftime('%Y-%m-%d')
    
    object_key = input_prefix + f"{max_date}/egressos.xlsx"
    output_key = output_prefix + f"{max_date}/egressos.parquet"

    df = pd.read_excel(object_key, dtype=str)

    log.info("Writing dataset on Raw Zone")
    df.to_parquet(output_key, index=False)
    log.info("Finished")