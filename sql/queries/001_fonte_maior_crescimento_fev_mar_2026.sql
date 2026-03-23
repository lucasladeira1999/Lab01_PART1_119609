WITH geracao_fonte_instante AS (
    SELECT
        du.nom_tipousina,
        fg.din_instante,
        SUM(fg.val_geracao) AS geracao_total_instante
    FROM fato_geracao fg
    INNER JOIN dim_usina du
        ON du.id_ons = fg.id_ons
    WHERE fg.din_instante >= TIMESTAMP '2026-02-01 00:00:00'
      AND fg.din_instante < TIMESTAMP '2026-04-01 00:00:00'
    GROUP BY
        du.nom_tipousina,
        fg.din_instante
),
media_mensal AS (
    SELECT
        du.nom_tipousina,
        DATE_TRUNC('month', du.din_instante)::date AS mes_referencia,
        AVG(du.geracao_total_instante) AS geracao_media
    FROM geracao_fonte_instante du
    GROUP BY
        du.nom_tipousina,
        DATE_TRUNC('month', du.din_instante)::date
),
pivot_mensal AS (
    SELECT
        nom_tipousina,
        MAX(CASE WHEN mes_referencia = DATE '2026-02-01' THEN geracao_media END) AS media_fevereiro,
        MAX(CASE WHEN mes_referencia = DATE '2026-03-01' THEN geracao_media END) AS media_marco
    FROM media_mensal
    GROUP BY nom_tipousina
)
SELECT
    nom_tipousina,
    ROUND(media_fevereiro, 6) AS media_fevereiro,
    ROUND(media_marco, 6) AS media_marco,
    ROUND(((media_marco - media_fevereiro) / NULLIF(media_fevereiro, 0)) * 100, 2) AS crescimento_percentual
FROM pivot_mensal
WHERE media_fevereiro IS NOT NULL
  AND media_marco IS NOT NULL
ORDER BY crescimento_percentual DESC;
