from sqlalchemy.engine import Engine

import pandas as pd
from time import sleep
import pandas as pd


class SimpleEtl:
    """_summary_
    Classe base para ETL simples
    """

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
    # def transform(self, df: pd.DataFrame) -> pd.DataFrame:
    #     return df

    def load(self, table_name, df: pd.DataFrame, if_exists: str = "replace") -> None:
        """_summary_
        Args:
            table_name (_type_): _description_
            df (pd.DataFrame): _description_
            if_exists (_type_, optional):
                None: não executa. Defaults to None.
        """

        try:
            with self.target.connect() as con:
                df.to_sql(name=table_name, con=con, if_exists=if_exists, index=False)
                print(f"Load ok - {len(df)} registros")

        except Exception as e:
            print("Erro ao conectar ao Target:", e)
            return False


class SimplePipeline(SimpleEtl):
    """_summary_
    Classe para executar uma cadeia de ETL
    """

    def __init__(self, source: Engine, target: Engine):
        self.commands = []
        super().__init__(source, target)

    def add_stmt(self, stmt: str, target_name: str, if_exists: str = "replace"):
        if stmt and target_name:
            self.commands.append(
                {"table": target_name, "stmt": stmt, "replace": if_exists}
            )

    def execute(self):
        for command in self.commands:
            data = self.extract_transform(command["stmt"])
            self.load(command["table"], data, command["replace"])
            sleep(2)
