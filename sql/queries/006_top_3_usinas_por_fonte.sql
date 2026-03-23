WITH media_usina AS (
    SELECT
        du.nom_tipousina,
        du.id_ons,
        du.nom_usina,
        de.nom_estado,
        AVG(fg.val_geracao) AS geracao_media_trimestre
    FROM fato_geracao fg
    INNER JOIN dim_usina du
        ON du.id_ons = fg.id_ons
    INNER JOIN dim_estado de
        ON de.id_estado = fg.id_estado
    WHERE fg.din_instante >= TIMESTAMP '2026-01-01 00:00:00'
      AND fg.din_instante < TIMESTAMP '2026-04-01 00:00:00'
    GROUP BY
        du.nom_tipousina,
        du.id_ons,
        du.nom_usina,
        de.nom_estado
),
ranking AS (
    SELECT
        nom_tipousina,
        id_ons,
        nom_usina,
        nom_estado,
        geracao_media_trimestre,
        ROW_NUMBER() OVER (
            PARTITION BY nom_tipousina
            ORDER BY geracao_media_trimestre DESC
        ) AS posicao_fonte
    FROM media_usina
)
SELECT
    nom_tipousina,
    posicao_fonte,
    id_ons,
    nom_usina,
    nom_estado,
    ROUND(geracao_media_trimestre, 6) AS geracao_media_trimestre
FROM ranking
WHERE posicao_fonte <= 3
ORDER BY nom_tipousina, posicao_fonte;
