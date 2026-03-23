SELECT
    de.nom_estado,
    ROUND(SUM(fg.val_geracao), 6) AS geracao_total,
    ROUND(
        (SUM(fg.val_geracao) / SUM(SUM(fg.val_geracao)) OVER ()) * 100,
        2
    ) AS participacao_percentual
FROM fato_geracao fg
INNER JOIN dim_estado de
    ON de.id_estado = fg.id_estado
WHERE fg.din_instante >= TIMESTAMP '2026-01-01 00:00:00'
  AND fg.din_instante < TIMESTAMP '2026-04-01 00:00:00'
GROUP BY de.nom_estado
ORDER BY participacao_percentual DESC;
