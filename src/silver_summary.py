from pathlib import Path

import pandas as pd


def build_quality_summary(
    raw_dataframe: pd.DataFrame,
    clean_dataframe: pd.DataFrame,
) -> dict[str, pd.DataFrame | int]:
    null_counts = clean_dataframe.isna().sum().rename("null_count").to_frame()
    data_types = clean_dataframe.dtypes.astype(str).rename("dtype").to_frame()
    numeric_summary = (
        clean_dataframe.select_dtypes(include=["number"])
        .agg(["count", "mean", "std", "min", "max"])
        .transpose()
    )

    return {
        "raw_rows": len(raw_dataframe),
        "silver_rows": len(clean_dataframe),
        "removed_rows": len(raw_dataframe) - len(clean_dataframe),
        "duplicates_removed": int(raw_dataframe.duplicated().sum()),
        "null_counts": null_counts,
        "data_types": data_types,
        "numeric_summary": numeric_summary,
    }


def write_markdown_report(
    quality_summary: dict[str, pd.DataFrame | int],
    chart_files: list[str],
    report_file: Path,
) -> Path:
    null_counts = quality_summary["null_counts"].to_string()
    data_types = quality_summary["data_types"].to_string()
    numeric_summary = quality_summary["numeric_summary"].round(2).to_string()

    markdown_content = f"""# Relatorio Silver

## Resumo da camada

- Linhas na Bronze: {quality_summary["raw_rows"]}
- Linhas na Silver: {quality_summary["silver_rows"]}
- Linhas removidas no tratamento: {quality_summary["removed_rows"]}
- Duplicatas removidas: {quality_summary["duplicates_removed"]}
- Arquivo Parquet: `data/silver/geracao_usina_silver.parquet`

## Contagem de nulos

```text
{null_counts}
```

## Tipos de colunas

```text
{data_types}
```

## Estatisticas descritivas

```text
{numeric_summary}
```

## Graficos

### 1. Geracao por hora e por mes com `nom_tipousina` na legenda
![Geracao por hora e por mes](figures/{chart_files[0]})

### 2. Participacao de cada fonte por mes
![Participacao de cada fonte por mes](figures/{chart_files[1]})

### 3. Participacao por estado
![Participacao por estado](figures/{chart_files[2]})

### 4. Participacao por combustivel nas termicas
![Participacao por combustivel nas termicas](figures/{chart_files[3]})

### 5. Geracao mensal por subsistema
![Geracao mensal por subsistema](figures/{chart_files[4]})
"""

    report_file.write_text(markdown_content, encoding="utf-8")
    return report_file
