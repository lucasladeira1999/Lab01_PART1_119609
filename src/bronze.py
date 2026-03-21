import logging
import os
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv


class Bronze:
    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parent.parent
        self.env_file = self.base_dir / ".env"
        self.raw_dir = self.base_dir / "data" / "raw"
        self.log_dir = self.base_dir / "logs"
        self.log_file = self.log_dir / "bronze.log"
        self.timeout = 30
        self.logger = self.setup_logger()

    def setup_logger(self) -> logging.Logger:
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)

            logger = logging.getLogger("bronze")
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
            raise OSError(f"Erro ao configurar o logger Bronze: {exc}") from exc

    def get_config(self) -> tuple[str, str, list[str], list[str]]:
        try:
            load_dotenv(dotenv_path=self.env_file)
            base_url = os.getenv("BRONZE_BASE_URL", "").strip().rstrip("/")
            file_pattern = os.getenv("BRONZE_FILE_PATTERN", "").strip()
            years_raw = os.getenv("BRONZE_YEARS", "").strip()
            months_raw = os.getenv("BRONZE_MONTHS", "").strip()

            if not base_url:
                raise ValueError("A variavel BRONZE_BASE_URL nao foi definida no .env.")

            if not file_pattern:
                raise ValueError(
                    "A variavel BRONZE_FILE_PATTERN nao foi definida no .env."
                )

            if not years_raw:
                raise ValueError("A variavel BRONZE_YEARS nao foi definida no .env.")

            if not months_raw:
                raise ValueError("A variavel BRONZE_MONTHS nao foi definida no .env.")

            years = [year.strip() for year in years_raw.split(",") if year.strip()]
            months = [month.strip().zfill(2) for month in months_raw.split(",") if month.strip()]

            if not years or not months:
                raise ValueError("Anos ou meses invalidos no arquivo .env.")

            return base_url, file_pattern, years, months
        except Exception as exc:
            raise ValueError(f"Erro ao ler a configuracao Bronze no .env: {exc}") from exc

    def build_source_url(self, base_url: str, file_pattern: str, year: str, month: str) -> str:
        try:
            file_name = file_pattern.format(year=year, month=month)
            return f"{base_url}/{file_name}"
        except Exception as exc:
            raise ValueError(f"Erro ao montar a URL para {year}-{month}: {exc}") from exc

    def poll_source(self, url: str) -> None:
        try:
            response = requests.head(url, allow_redirects=True, timeout=self.timeout)
            response.raise_for_status()
        except Exception as exc:
            raise ConnectionError(f"Erro ao validar a URL com HEAD: {exc}") from exc

    def generate_output_path(self, url: str) -> Path:
        try:
            self.raw_dir.mkdir(parents=True, exist_ok=True)

            parsed_url = urlparse(url)
            file_name = Path(parsed_url.path).name or "bronze_file"

            return self.raw_dir / file_name
        except Exception as exc:
            raise OSError(f"Erro ao preparar o caminho em data/raw: {exc}") from exc

    def download_bronze(self, url: str, destination: Path) -> Path:
        try:
            with requests.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()

                with destination.open("wb") as file_obj:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file_obj.write(chunk)

            return destination
        except Exception as exc:
            raise IOError(f"Erro ao baixar e salvar o arquivo raw: {exc}") from exc

    def download_file(self, base_url: str, file_pattern: str, year: str, month: str) -> Path:
        try:
            source_url = self.build_source_url(base_url, file_pattern, year, month)
            self.logger.info("Iniciando ingestao Bronze para a URL %s", source_url)

            self.poll_source(source_url)
            output_path = self.generate_output_path(source_url)
            saved_file = self.download_bronze(source_url, output_path)

            self.logger.info(
                "Ingestao Bronze concluida com sucesso. Arquivo salvo em %s",
                saved_file,
            )
            return saved_file
        except Exception as exc:
            self.logger.exception(
                "Falha na ingestao Bronze para a referencia %s-%s: %s",
                year,
                month,
                exc,
            )
            raise

    def run(self) -> list[Path]:
        try:
            base_url, file_pattern, years, months = self.get_config()
            saved_files = []

            for year in years:
                for month in months:
                    saved_file = self.download_file(
                        base_url=base_url,
                        file_pattern=file_pattern,
                        year=year,
                        month=month,
                    )
                    saved_files.append(saved_file)

            return saved_files
        except Exception as exc:
            self.logger.exception("Falha geral na camada Bronze: %s", exc)
            raise
