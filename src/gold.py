import io
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine


class Gold:
    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parent.parent
        self.env_file = self.base_dir / ".env"
        self.silver_file = self.base_dir / "data" / "silver" / "geracao_usina_silver.parquet"
        self.log_dir = self.base_dir / "logs"
        self.log_file = self.log_dir / "gold.log"
        self.interface_table = "interface_geracao_usina"
        self.transfer_procedure = "prc_carga_fato_geracao"
        self.logger = self.setup_logger()

    def setup_logger(self) -> logging.Logger:
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)

            logger = logging.getLogger("gold")
            logger.setLevel(logging.INFO)

            if not logger.handlers:
                file_handler = logging.FileHandler(self.log_file, encoding="utf-8")
                formatter = logging.Formatter(
                    "%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

            return logger
        except Exception as exc:
            raise OSError(f"Erro ao configurar o logger Gold: {exc}") from exc

    def get_engine(self) -> Engine:
        try:
            load_dotenv(dotenv_path=self.env_file)

            user_postgres = os.getenv("USER_POSTGRES", "").strip()
            password_postgres = os.getenv("PASSWORD_POSTGRES", "").strip()
            database_postgres = os.getenv("DATABASE_POSTGRES", "").strip()

            if not user_postgres:
                raise ValueError("A variavel USER_POSTGRES nao foi definida no .env.")

            if not password_postgres:
                raise ValueError("A variavel PASSWORD_POSTGRES nao foi definida no .env.")

            if not database_postgres:
                raise ValueError("A variavel DATABASE_POSTGRES nao foi definida no .env.")

            connection_url = URL.create(
                drivername="postgresql+psycopg2",
                username=user_postgres,
                password=password_postgres,
                host="127.0.0.1",
                port=5432,
                database=database_postgres,
            )

            return create_engine(connection_url)
        except Exception as exc:
            raise ConnectionError(f"Erro ao criar engine do Postgres: {exc}") from exc

    def read_silver_data(self) -> pd.DataFrame:
        try:
            if not self.silver_file.exists():
                raise FileNotFoundError(
                    "O arquivo Parquet da Silver nao foi encontrado em data/silver."
                )

            dataframe = pd.read_parquet(self.silver_file)
            self.logger.info("Arquivo Silver lido com %s linhas", len(dataframe))
            return dataframe
        except Exception as exc:
            raise IOError(f"Erro ao ler o Parquet da Silver: {exc}") from exc

    def build_interface_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        try:
            interface_dataframe = (
                dataframe[
                    [
                        "din_instante",
                        "id_subsistema",
                        "nom_subsistema",
                        "id_estado",
                        "nom_estado",
                        "cod_modalidadeoperacao",
                        "nom_tipousina",
                        "nom_tipocombustivel",
                        "nom_usina",
                        "id_ons",
                        "ceg",
                        "val_geracao",
                    ]
                ]
                .reset_index(drop=True)
            )
            return interface_dataframe
        except Exception as exc:
            raise ValueError(f"Erro ao montar a interface da Gold: {exc}") from exc

    def truncate_interface_table(self, engine: Engine) -> None:
        try:
            with engine.begin() as connection:
                connection.execute(text(f"TRUNCATE TABLE {self.interface_table};"))

            self.logger.info("Tabela de interface truncada com sucesso")
        except Exception as exc:
            raise RuntimeError(f"Erro ao truncar a tabela de interface: {exc}") from exc

    def copy_dataframe(
        self,
        engine: Engine,
        dataframe: pd.DataFrame,
        table_name: str,
        columns: list[str],
    ) -> None:
        try:
            dataframe_to_copy = dataframe[columns].copy()
            buffer = io.StringIO()
            dataframe_to_copy.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            raw_connection = engine.raw_connection()
            try:
                with raw_connection.cursor() as cursor:
                    cursor.copy_expert(
                        sql=f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV",
                        file=buffer,
                    )
                raw_connection.commit()
            finally:
                raw_connection.close()

            self.logger.info(
                "Carga executada com sucesso para %s com %s linhas",
                table_name,
                len(dataframe_to_copy),
            )
        except Exception as exc:
            raise RuntimeError(f"Erro ao carregar a tabela {table_name}: {exc}") from exc

    def call_transfer_procedure(self, engine: Engine) -> None:
        try:
            with engine.begin() as connection:
                connection.execute(text(f"CALL {self.transfer_procedure}();"))

            self.logger.info("Procedure de transferencia executada com sucesso")
        except Exception as exc:
            raise RuntimeError(f"Erro ao executar a procedure de transferencia: {exc}") from exc

    def run(self) -> dict[str, int]:
        try:
            self.logger.info("Iniciando processamento da camada Gold")
            engine = self.get_engine()
            dataframe = self.read_silver_data()
            interface_dataframe = self.build_interface_dataframe(dataframe)
            self.copy_dataframe(
                engine=engine,
                dataframe=interface_dataframe,
                table_name=self.interface_table,
                columns=[
                    "din_instante",
                    "id_subsistema",
                    "nom_subsistema",
                    "id_estado",
                    "nom_estado",
                    "cod_modalidadeoperacao",
                    "nom_tipousina",
                    "nom_tipocombustivel",
                    "nom_usina",
                    "id_ons",
                    "ceg",
                    "val_geracao",
                ],
            )
            self.call_transfer_procedure(engine)
            self.truncate_interface_table(engine)
            self.logger.info("Processamento Gold concluido com sucesso")
            return {"interface_geracao_usina": len(interface_dataframe)}
        except Exception as exc:
            self.logger.exception("Falha na camada Gold: %s", exc)
            raise
