from src.bronze import Bronze
from src.business_metrics import BusinessMetrics
from src.gold import Gold
from src.silver import Silver


def main() -> None:
    # bronze = Bronze()
    # saved_files = bronze.run()
    # print(f"Arquivos salvos em data/raw: {len(saved_files)}")

    # silver = Silver()
    # outputs = silver.run()
    # print(f"Parquet salvo em: {outputs['parquet']}")
    # print(f"Relatorio salvo em: {outputs['report']}")

    # gold = Gold()
    # gold_outputs = gold.run()
    # print(f"Interface Gold carregada: {gold_outputs}")

    business_metrics = BusinessMetrics()
    reports = business_metrics.run()
    print(f"Relatorios de negocio gerados: {len(reports)}")


if __name__ == "__main__":
    main()
