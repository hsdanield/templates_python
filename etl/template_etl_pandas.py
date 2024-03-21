from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import pandas as pd

class SimpleEtl:
    def __init__(self, source: Engine, target: Engine):
        self.source = source
        self.target = target
        
    def extract_transform(self, stmt: str) -> pd.DataFrame:
        try:
            with self.source.connect() as con:
                df = pd.read_sql_query(sql=stmt, con=con)
                print(f"Extract ok - {len(df)} registros")
                return df
        except Exception as e:
            print("Erro ao conectar ao Source:", e)

    # Not Use
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
        
    
    def load(self, df: pd.DataFrame) -> None:
        try:
            with self.target.connect() as con:
                df.to_sql(name="modulos", con=con, if_exists="replace", index=False)
                print(f"Load ok - {len(df)} registros")
        except Exception as e:
            print("Erro ao conectar ao Target:", e)       
        

if __name__ == "__main__":
        

    source_url = ""
    source_engine = create_engine(source_url)

    target_url = ""
    target_engine = create_engine(target_url)

    stmt = """
        your select here
    """

    simple_etl = SimpleEtl(source_engine, target_engine)
    data = simple_etl.extract_transform(stmt)
    simple_etl.load(data)
