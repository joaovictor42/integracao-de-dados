from datetime import datetime
import logging
import pandas as pd
import s3fs
import settings


log = logging.getLogger(__name__)


def main():
    log.info("Script started")
    input_prefix = "s3://ppgti-igd-2024/suap/"
    output_prefix = "s3://ppgti-igd-landing-zone/suap/egressos/"
    current_date = datetime.now().strftime('%Y-%m-%d')
    output_key = output_prefix + f"{current_date}/egressos.xlsx"

    log.info("Reading data from Suap")
    s3 = s3fs.S3FileSystem()
    obj_keys = ["s3://" + key for key in s3.ls(input_prefix) if key.endswith(".xls")]

    if not obj_keys:
        raise Exception('No file found')
    
    dataframes = []
    for obj_key in obj_keys:
        log.info(f"Reading {obj_key}")
        dataframes.append(pd.read_excel(obj_key))
    
    df = pd.concat(dataframes, ignore_index=True)

    log.info("Writing dataset on Landing Zone")
    df.to_excel(output_key, index=False)
    log.info("Finished")