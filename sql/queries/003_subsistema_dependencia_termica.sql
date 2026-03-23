SELECT
    ds.nom_subsistema,
    ROUND(SUM(CASE WHEN du.nom_tipousina = 'TÉRMICA' THEN fg.val_geracao ELSE 0 END), 6) AS geracao_termica,
    ROUND(SUM(fg.val_geracao), 6) AS geracao_total,
    ROUND(
        (
            SUM(CASE WHEN du.nom_tipousina = 'TÉRMICA' THEN fg.val_geracao ELSE 0 END)
            / NULLIF(SUM(fg.val_geracao), 0)
        ) * 100,
        2
    ) AS dependencia_termica_percentual
FROM fato_geracao fg
INNER JOIN dim_subsistema ds
    ON ds.id_subsistema = fg.id_subsistema
INNER JOIN dim_usina du
    ON du.id_ons = fg.id_ons
WHERE fg.din_instante >= TIMESTAMP '2026-01-01 00:00:00'
  AND fg.din_instante < TIMESTAMP '2026-04-01 00:00:00'
GROUP BY ds.nom_subsistema
ORDER BY dependencia_termica_percentual DESC;
