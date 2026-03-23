import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd

from src.silver_charts import generate_charts
from src.silver_summary import build_quality_summary, write_markdown_report


class Silver:
    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parent.parent
        self.raw_dir = self.base_dir / "data" / "raw"
        self.silver_dir = self.base_dir / "data" / "silver"
        self.report_dir = self.base_dir / "reports"
        self.figure_dir = self.report_dir / "figures"
        self.log_dir = self.base_dir / "logs"
        self.log_file = self.log_dir / "silver.log"
        self.parquet_file = self.silver_dir / "geracao_usina_silver.parquet"
        self.report_file = self.report_dir / "silver_report.md"
        self.logger = self.setup_logger()

    def setup_logger(self) -> logging.Logger:
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)

            logger = logging.getLogger("silver")
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
            raise OSError(f"Erro ao configurar o logger Silver: {exc}") from exc

    def read_raw_files(self) -> pd.DataFrame:
        try:
            self.silver_dir.mkdir(parents=True, exist_ok=True)
            self.report_dir.mkdir(parents=True, exist_ok=True)
            self.figure_dir.mkdir(parents=True, exist_ok=True)

            raw_files = sorted(self.raw_dir.glob("*.csv"))

            if not raw_files:
                raise FileNotFoundError("Nenhum arquivo CSV foi encontrado em data/raw.")

            self.logger.info("Lendo %s arquivos raw para a camada Silver", len(raw_files))
            dataframes = [pd.read_csv(file_path, sep=";") for file_path in raw_files]
            return pd.concat(dataframes, ignore_index=True)
        except Exception as exc:
            raise IOError(f"Erro ao ler os arquivos raw: {exc}") from exc

    def normalize_text_value(self, text_value: str) -> str:
        normalized = unicodedata.normalize("NFKD", str(text_value))
        normalized = normalized.encode("ascii", "ignore").decode("utf-8")
        return normalized.lower().strip()

    def standardize_column_names(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        try:
            dataframe = dataframe.copy()
            standardized_columns = []

            for column_name in dataframe.columns:
                normalized = unicodedata.normalize("NFKD", column_name)
                normalized = normalized.encode("ascii", "ignore").decode("utf-8")
                normalized = normalized.lower()
                normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
                standardized_columns.append(normalized.strip("_"))

            dataframe.columns = standardized_columns
            return dataframe
        except Exception as exc:
            raise ValueError(f"Erro ao padronizar nomes de colunas: {exc}") from exc

    def clean_values(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        try:
            dataframe = dataframe.copy()

            text_columns = dataframe.select_dtypes(include=["object", "string"]).columns
            for column_name in text_columns:
                dataframe[column_name] = dataframe[column_name].astype("string").str.strip()
                dataframe[column_name] = dataframe[column_name].replace({"": pd.NA, "-": pd.NA})

            dataframe["din_instante"] = pd.to_datetime(
                dataframe["din_instante"],
                errors="coerce",
            )
            dataframe["val_geracao"] = pd.to_numeric(
                dataframe["val_geracao"],
                errors="coerce",
            )

            dataframe["val_geracao"] = dataframe["val_geracao"].fillna(0)

            for column_name in text_columns:
                dataframe[column_name] = dataframe[column_name].fillna("nao_informado")

            dataframe = dataframe.dropna(subset=["din_instante"])
            dataframe = dataframe.drop_duplicates()

            return dataframe
        except Exception as exc:
            raise ValueError(f"Erro ao limpar os dados da Silver: {exc}") from exc

    def save_parquet(self, dataframe: pd.DataFrame) -> Path:
        try:
            dataframe.to_parquet(self.parquet_file, index=False)
            self.logger.info("Arquivo Silver salvo em %s", self.parquet_file)
            return self.parquet_file
        except Exception as exc:
            raise IOError(f"Erro ao salvar o Parquet da Silver: {exc}") from exc

    def run(self) -> dict[str, Path]:
        try:
            self.logger.info("Iniciando processamento da camada Silver")
            raw_dataframe = self.read_raw_files()
            standardized_dataframe = self.standardize_column_names(raw_dataframe)
            clean_dataframe = self.clean_values(standardized_dataframe)
            quality_summary = build_quality_summary(raw_dataframe, clean_dataframe)
            parquet_path = self.save_parquet(clean_dataframe)
            chart_files = generate_charts(clean_dataframe, self.figure_dir)
            report_path = write_markdown_report(
                quality_summary=quality_summary,
                chart_files=chart_files,
                report_file=self.report_file,
            )
            self.logger.info("Relatorio Silver salvo em %s", report_path)
            self.logger.info("Processamento Silver concluido com sucesso")
            return {"parquet": parquet_path, "report": report_path}
        except Exception as exc:
            self.logger.exception("Falha na camada Silver: %s", exc)
            raise
