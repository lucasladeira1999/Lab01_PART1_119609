from pathlib import Path
import unicodedata

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def normalize_text_value(text_value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(text_value))
    normalized = normalized.encode("ascii", "ignore").decode("utf-8")
    return normalized.lower().strip()


def save_hourly_generation_chart(dataframe: pd.DataFrame, figure_dir: Path) -> str:
    chart_path = figure_dir / "geracao_por_hora_e_mes.png"

    chart_dataframe = dataframe.copy()
    chart_dataframe = (
        chart_dataframe.groupby(["din_instante", "nom_tipousina"], as_index=False)["val_geracao"]
        .sum()
    )
    chart_dataframe["mes_referencia"] = chart_dataframe["din_instante"].dt.strftime("%Y-%m")
    chart_dataframe["hora"] = chart_dataframe["din_instante"].dt.hour
    chart_dataframe = (
        chart_dataframe.groupby(["mes_referencia", "hora", "nom_tipousina"], as_index=False)[
            "val_geracao"
        ]
        .mean()
    )

    months = sorted(chart_dataframe["mes_referencia"].unique())
    fig, axes = plt.subplots(len(months), 1, figsize=(14, 5 * len(months)), sharex=True)

    if len(months) == 1:
        axes = [axes]

    for axis, month in zip(axes, months):
        month_dataframe = chart_dataframe[chart_dataframe["mes_referencia"] == month]
        for source_name, source_dataframe in month_dataframe.groupby("nom_tipousina"):
            axis.plot(
                source_dataframe["hora"],
                source_dataframe["val_geracao"],
                marker="o",
                linewidth=2,
                label=source_name,
            )

        axis.set_title(f"Geracao media por hora em {month}")
        axis.set_xlabel("Hora do dia")
        axis.set_ylabel("Geracao media (MWmed)")
        axis.grid(True, alpha=0.3)
        axis.legend()

    fig.tight_layout()
    fig.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return chart_path.name


def save_source_share_chart(dataframe: pd.DataFrame, figure_dir: Path) -> str:
    chart_path = figure_dir / "participacao_fonte_por_mes.png"

    chart_dataframe = dataframe.copy()
    chart_dataframe["mes_referencia"] = chart_dataframe["din_instante"].dt.strftime("%Y-%m")
    chart_dataframe = (
        chart_dataframe.groupby(["mes_referencia", "nom_tipousina"], as_index=False)["val_geracao"]
        .sum()
        .pivot(index="mes_referencia", columns="nom_tipousina", values="val_geracao")
        .fillna(0)
    )
    chart_dataframe = chart_dataframe.div(chart_dataframe.sum(axis=1), axis=0) * 100

    ax = chart_dataframe.plot(kind="bar", stacked=True, figsize=(14, 7), colormap="tab20")
    ax.set_title("Participacao percentual de cada fonte por mes")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Participacao (%)")
    ax.legend(title="Tipo de usina", bbox_to_anchor=(1.02, 1), loc="upper left")

    for container in ax.containers:
        labels = []
        for bar in container:
            height = bar.get_height()
            labels.append(f"{height:.1f}%" if height >= 5 else "")
        ax.bar_label(container, labels=labels, label_type="center", fontsize=8, color="white")

    plt.tight_layout()
    plt.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close()
    return chart_path.name


def save_state_share_chart(dataframe: pd.DataFrame, figure_dir: Path) -> str:
    chart_path = figure_dir / "participacao_por_estado.png"

    total_generation = dataframe["val_geracao"].sum()
    chart_dataframe = (
        dataframe.groupby("nom_estado", as_index=False)["val_geracao"]
        .sum()
        .sort_values("val_geracao", ascending=False)
        .head(15)
    )
    chart_dataframe["participacao"] = (chart_dataframe["val_geracao"] / total_generation) * 100

    fig, ax = plt.subplots(figsize=(12, 8))
    bars = ax.bar(chart_dataframe["nom_estado"], chart_dataframe["participacao"], color="#1f77b4")
    ax.set_title("Participacao dos principais estados na geracao")
    ax.set_xlabel("Estado")
    ax.set_ylabel("Participacao (%)")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True, axis="y", alpha=0.3)
    labels = [f"{value:.2f}%" for value in chart_dataframe["participacao"]]
    ax.bar_label(bars, labels=labels, padding=3, fontsize=8)
    fig.tight_layout()
    fig.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return chart_path.name


def save_thermal_fuel_chart(dataframe: pd.DataFrame, figure_dir: Path) -> str:
    chart_path = figure_dir / "participacao_termicas_combustivel.png"

    chart_dataframe = dataframe.copy()
    chart_dataframe["nom_tipousina_normalizado"] = chart_dataframe["nom_tipousina"].map(
        normalize_text_value
    )
    chart_dataframe = chart_dataframe[
        chart_dataframe["nom_tipousina_normalizado"].str.contains("termica", na=False)
    ].copy()
    chart_dataframe = (
        chart_dataframe.groupby("nom_tipocombustivel", as_index=False)["val_geracao"]
        .sum()
        .sort_values("val_geracao", ascending=False)
    )
    chart_dataframe["participacao"] = (
        chart_dataframe["val_geracao"] / chart_dataframe["val_geracao"].sum()
    ) * 100

    fig, ax = plt.subplots(figsize=(12, 7))
    bars = ax.bar(chart_dataframe["nom_tipocombustivel"], chart_dataframe["participacao"], color="#ff7f0e")
    ax.set_title("Participacao por combustivel nas usinas termicas")
    ax.set_xlabel("Combustivel")
    ax.set_ylabel("Participacao (%)")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True, axis="y", alpha=0.3)
    labels = [f"{value:.2f}%" for value in chart_dataframe["participacao"]]
    ax.bar_label(bars, labels=labels, padding=3, fontsize=8)
    fig.tight_layout()
    fig.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return chart_path.name


def save_subsystem_chart(dataframe: pd.DataFrame, figure_dir: Path) -> str:
    chart_path = figure_dir / "geracao_por_subsistema.png"

    chart_dataframe = dataframe.copy()
    chart_dataframe = (
        chart_dataframe.groupby(["din_instante", "nom_subsistema"], as_index=False)["val_geracao"]
        .sum()
    )
    chart_dataframe["mes_referencia"] = chart_dataframe["din_instante"].dt.strftime("%Y-%m")
    chart_dataframe = (
        chart_dataframe.groupby(["mes_referencia", "nom_subsistema"], as_index=False)["val_geracao"]
        .mean()
    )

    pivot_dataframe = (
        chart_dataframe.pivot(
            index="mes_referencia",
            columns="nom_subsistema",
            values="val_geracao",
        )
        .fillna(0)
    )

    fig, ax = plt.subplots(figsize=(14, 7))
    pivot_dataframe.plot(kind="bar", colormap="Set2", ax=ax)
    ax.set_title("Geracao media mensal por subsistema")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Geracao media (MWmed)")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(title="Subsistema")
    fig.tight_layout()
    fig.savefig(chart_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return chart_path.name


def generate_charts(dataframe: pd.DataFrame, figure_dir: Path) -> list[str]:
    return [
        save_hourly_generation_chart(dataframe, figure_dir),
        save_source_share_chart(dataframe, figure_dir),
        save_state_share_chart(dataframe, figure_dir),
        save_thermal_fuel_chart(dataframe, figure_dir),
        save_subsystem_chart(dataframe, figure_dir),
    ]
