# Top 3 usinas por fonte

## Pergunta de negocio

Quais foram as 3 usinas com maior geracao media dentro de cada tipo de usina no primeiro trimestre de 2026?

## SQL utilizada

```sql
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
```

## Resultado

| nom_tipousina | posicao_fonte | id_ons | nom_usina | nom_estado | geracao_media_trimestre |
| --- | --- | --- | --- | --- | --- |
| EOLIELÉTRICA | 1 | CJU_RN5ETGS | Conj. Serra do Tigre | RIO GRANDE DO NORTE | 241.688662 |
| EOLIELÉTRICA | 2 | CJU_PI4ECNP | Conj. Curral Novo do Piauí II 230 kV | PIAUI | 213.781982 |
| EOLIELÉTRICA | 3 | CJU_RNCAJ1 | Conj. Caju | RIO GRANDE DO NORTE | 211.203822 |
| FOTOVOLTAICA | 1 | PQU_SPSP_GD | PQU SPSP FOTOV MMGD | SAO PAULO | 1063.972554 |
| FOTOVOLTAICA | 2 | PQU_MGMG_GD | PQU MGMG FOTOV MMGD | MINAS GERAIS | 896.189337 |
| FOTOVOLTAICA | 3 | PQU_RSRS_GD | PQU RSRS FOTOV MMGD | RIO GRANDE DO SUL | 727.331239 |
| HIDROELÉTRICA | 1 | PABM | Belo Monte | PARA | 5525.835451 |
| HIDROELÉTRICA | 2 | PRIT60 | Itaipu 60 Hz | PARANA | 4082.978529 |
| HIDROELÉTRICA | 3 | PATU | Tucuruí | PARA | 3984.12856 |
| NUCLEAR | 1 | RJUTN1 | Angra I | RIO DE JANEIRO | 464.414606 |
| NUCLEAR | 2 | RJUTN2 | Angra II | RIO DE JANEIRO | 338.987008 |
| TÉRMICA | 1 | RJMAE | Marlim Azul | RIO DE JANEIRO | 513.917667 |
| TÉRMICA | 2 | SEUPS1 | Porto de Sergipe I | SERGIPE | 501.459504 |
| TÉRMICA | 3 | MAUTM3 | Maranhão III | MARANHAO | 463.898712 |
