SELECT
    du.id_ons,
    du.nom_usina,
    de.nom_estado,
    du.nom_tipousina,
    ROUND(AVG(fg.val_geracao), 6) AS geracao_media_trimestre
FROM fato_geracao fg
INNER JOIN dim_usina du
    ON du.id_ons = fg.id_ons
INNER JOIN dim_estado de
    ON de.id_estado = fg.id_estado
WHERE fg.din_instante >= TIMESTAMP '2026-01-01 00:00:00'
  AND fg.din_instante < TIMESTAMP '2026-04-01 00:00:00'
GROUP BY
    du.id_ons,
    du.nom_usina,
    de.nom_estado,
    du.nom_tipousina
ORDER BY geracao_media_trimestre DESC
LIMIT 10;
