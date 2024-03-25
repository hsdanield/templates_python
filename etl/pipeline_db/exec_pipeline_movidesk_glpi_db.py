from etl import SimpleEtl, SimplePipeline

from sqlalchemy import create_engine

from glob import glob
from pathlib import Path

SQL_PATH = "sql/"


def read_sql_files(path, pattern):
    contents = {}
    fdir = glob(path + pattern)
    for file in fdir:
        with open(file) as fread:
            content = fread.read()
            name = file.replace("sql", "").replace("/", "").replace(".", "")
            contents[name] = content
    return contents


def create_pipeline_with_files(source_engine, target_engine) -> SimplePipeline:
    pipeline = SimplePipeline(source_engine, target_engine)
    contents = read_sql_files(SQL_PATH, "*.sql")
    for key in contents:
        pipeline.add_stmt(stmt=contents[key], target_name=key, if_exists="replace")
    return pipeline


if __name__ == "__main__":

    import os
    from dotenv import load_dotenv

    # Carregando Variaveis de ambiente (.env)
    load_dotenv()

    source_url = os.getenv("MYSQL_URL")
    source_engine = create_engine(source_url)

    target_url = os.getenv("SQLSERVER_URL")
    target_engine = create_engine(target_url)
    simple_pipeline = create_pipeline_with_files(source_engine, target_engine)
    simple_pipeline.execute()

