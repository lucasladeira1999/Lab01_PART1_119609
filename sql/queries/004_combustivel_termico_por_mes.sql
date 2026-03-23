WITH combustivel_mes AS (
    SELECT
        DATE_TRUNC('month', fg.din_instante)::date AS mes_referencia,
        du.nom_tipocombustivel,
        SUM(fg.val_geracao) AS geracao_total
    FROM fato_geracao fg
    INNER JOIN dim_usina du
        ON du.id_ons = fg.id_ons
    WHERE fg.din_instante >= TIMESTAMP '2026-01-01 00:00:00'
      AND fg.din_instante < TIMESTAMP '2026-04-01 00:00:00'
      AND du.nom_tipousina = 'TÉRMICA'
    GROUP BY
        DATE_TRUNC('month', fg.din_instante)::date,
        du.nom_tipocombustivel
),
ranking AS (
    SELECT
        mes_referencia,
        nom_tipocombustivel,
        geracao_total,
        ROW_NUMBER() OVER (
            PARTITION BY mes_referencia
            ORDER BY geracao_total DESC
        ) AS posicao_mes
    FROM combustivel_mes
)
SELECT
    mes_referencia,
    nom_tipocombustivel,
    ROUND(geracao_total, 6) AS geracao_total
FROM ranking
WHERE posicao_mes = 1
ORDER BY mes_referencia;
