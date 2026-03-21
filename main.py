from src.bronze import Bronze
from src.silver import Silver


def main() -> None:
    # bronze = Bronze()
    # saved_files = bronze.run()
    # print(f"Arquivos salvos em data/raw: {len(saved_files)}")

    silver = Silver()
    outputs = silver.run()
    print(f"Parquet salvo em: {outputs['parquet']}")
    print(f"Relatorio salvo em: {outputs['report']}")


if __name__ == "__main__":
    main()
