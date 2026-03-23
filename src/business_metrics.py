import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine


class BusinessMetrics:
    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parent.parent
        self.env_file = self.base_dir / ".env"
        self.query_dir = self.base_dir / "sql" / "queries"
        self.out_dir = self.base_dir / "out"
        self.log_dir = self.base_dir / "logs"
        self.log_file = self.log_dir / "business_metrics.log"
        self.logger = self.setup_logger()
        self.query_catalog = [
            {
                "file_name": "001_fonte_maior_crescimento_fev_mar_2026.sql",
                "title": "Fonte com maior crescimento percentual de fevereiro para marco de 2026",
                "question": "Qual fonte de geracao apresentou o maior crescimento percentual de geracao media entre fevereiro e marco de 2026?",
                "output_name": "query_01_fonte_maior_crescimento.md",
            },
            {
                "file_name": "002_estado_maior_participacao_trimestre.sql",
                "title": "Participacao dos estados na geracao total do trimestre",
                "question": "Qual estado teve a maior participacao percentual na geracao total do primeiro trimestre de 2026?",
                "output_name": "query_02_estado_maior_participacao.md",
            },
            {
                "file_name": "003_subsistema_dependencia_termica.sql",
                "title": "Dependencia termica por subsistema",
                "question": "Qual subsistema apresentou a maior dependencia de geracao termica no primeiro trimestre de 2026?",
                "output_name": "query_03_subsistema_dependencia_termica.md",
            },
            {
                "file_name": "004_combustivel_termico_por_mes.sql",
                "title": "Combustivel termico lider por mes",
                "question": "Qual combustivel foi o mais representativo entre as usinas termicas em cada mes do primeiro trimestre de 2026?",
                "output_name": "query_04_combustivel_termico_mes.md",
            },
            {
                "file_name": "005_top_10_usinas_trimestre.sql",
                "title": "Top 10 usinas por geracao media no trimestre",
                "question": "Quais foram as 10 usinas com maior geracao media no primeiro trimestre de 2026?",
                "output_name": "query_05_top_10_usinas.md",
            },
            {
                "file_name": "006_top_3_usinas_por_fonte.sql",
                "title": "Top 3 usinas por fonte",
                "question": "Quais foram as 3 usinas com maior geracao media dentro de cada tipo de usina no primeiro trimestre de 2026?",
                "output_name": "query_06_top_3_usinas_por_fonte.md",
            },
        ]

    def setup_logger(self) -> logging.Logger:
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)

            logger = logging.getLogger("business_metrics")
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
            raise OSError(f"Erro ao configurar o logger de metricas: {exc}") from exc

    def get_engine(self) -> Engine:
        try:
            load_dotenv(dotenv_path=self.env_file)

            user_postgres = os.getenv("USER_POSTGRES", "").strip()
            password_postgres = os.getenv("PASSWORD_POSTGRES", "").strip()
            database_postgres = os.getenv("DATABASE_POSTGRES", "").strip()

            if not user_postgres or not password_postgres or not database_postgres:
                raise ValueError(
                    "As variaveis USER_POSTGRES, PASSWORD_POSTGRES e DATABASE_POSTGRES precisam estar preenchidas no .env."
                )

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
            raise ConnectionError(f"Erro ao criar conexao para as metricas: {exc}") from exc

    def read_query_text(self, file_name: str) -> str:
        query_file = self.query_dir / file_name
        if not query_file.exists():
            raise FileNotFoundError(f"Arquivo SQL nao encontrado: {query_file}")
        return query_file.read_text(encoding="utf-8")

    def execute_query(self, engine: Engine, query_text: str) -> pd.DataFrame:
        try:
            return pd.read_sql(query_text, engine)
        except Exception as exc:
            raise RuntimeError(f"Erro ao executar query de negocio: {exc}") from exc

    def write_markdown_report(
        self,
        title: str,
        question: str,
        query_text: str,
        dataframe: pd.DataFrame,
        output_name: str,
    ) -> Path:
        try:
            self.out_dir.mkdir(parents=True, exist_ok=True)
            output_file = self.out_dir / output_name

            result_table = self.build_markdown_table(dataframe)

            markdown_content = f"""# {title}

## Pergunta de negocio

{question}

## SQL utilizada

```sql
{query_text.strip()}
```

## Resultado

{result_table}
"""

            output_file.write_text(markdown_content, encoding="utf-8")
            self.logger.info("Relatorio gerado em %s", output_file)
            return output_file
        except Exception as exc:
            raise IOError(f"Erro ao escrever relatorio de metricas: {exc}") from exc

    def build_markdown_table(self, dataframe: pd.DataFrame) -> str:
        if dataframe.empty:
            return "Sem resultados."

        header = "| " + " | ".join(dataframe.columns.astype(str)) + " |"
        separator = "| " + " | ".join(["---"] * len(dataframe.columns)) + " |"
        rows = []

        for row in dataframe.itertuples(index=False, name=None):
            row_values = [str(value) for value in row]
            rows.append("| " + " | ".join(row_values) + " |")

        return "\n".join([header, separator, *rows])

    def run(self) -> list[Path]:
        try:
            self.logger.info("Iniciando geracao das metricas de negocio")
            engine = self.get_engine()
            generated_reports = []

            for query_item in self.query_catalog:
                query_text = self.read_query_text(query_item["file_name"])
                result_dataframe = self.execute_query(engine, query_text)
                report_path = self.write_markdown_report(
                    title=query_item["title"],
                    question=query_item["question"],
                    query_text=query_text,
                    dataframe=result_dataframe,
                    output_name=query_item["output_name"],
                )
                generated_reports.append(report_path)

            self.logger.info("Metricas de negocio geradas com sucesso")
            return generated_reports
        except Exception as exc:
            self.logger.exception("Falha na geracao das metricas de negocio: %s", exc)
            raise
