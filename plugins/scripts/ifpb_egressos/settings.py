from airflow.hooks.base import BaseHook
import os


aws_connection = BaseHook.get_connection('aws')
os.environ["AWS_ACCESS_KEY_ID"] = aws_connection.login
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_connection.password
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

BUCKET = 'ppgti-igd-2024'