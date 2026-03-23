# Participacao dos estados na geracao total do trimestre

## Pergunta de negocio

Qual estado teve a maior participacao percentual na geracao total do primeiro trimestre de 2026?

## SQL utilizada

```sql
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
```

## Resultado

| nom_estado | geracao_total | participacao_percentual |
| --- | --- | --- |
| PARA | 22664063.864 | 14.34 |
| MINAS GERAIS | 16900205.881 | 10.69 |
| PARANA | 16657055.433 | 10.54 |
| SAO PAULO | 14975384.227 | 9.47 |
| RONDONIA | 11774126.261 | 7.45 |
| BAHIA | 10889875.745 | 6.89 |
| RIO DE JANEIRO | 7778979.191 | 4.92 |
| RIO GRANDE DO NORTE | 7155799.245 | 4.53 |
| SANTA CATARINA | 6243681.492 | 3.95 |
| RIO GRANDE DO SUL | 6094373.727 | 3.86 |
| GOIAS | 4595475.401 | 2.91 |
| MATO GROSSO | 4364536.868 | 2.76 |
| MARANHAO | 4074512.333 | 2.58 |
| CEARA | 3567756.135 | 2.26 |
| PIAUI | 3378753.709 | 2.14 |
| ALAGOAS | 2801045.342 | 1.77 |
| TOCANTINS | 2437773.031 | 1.54 |
| PERNAMBUCO | 2252027.522 | 1.42 |
| AMAZONAS | 1892449.511 | 1.2 |
| z - INTERNACIONAL | 1683773.272 | 1.07 |
| MATO GROSSO DO SUL | 1371319.037 | 0.87 |
| ESPIRITO SANTO | 1318091.656 | 0.83 |
| SERGIPE | 1037463.048 | 0.66 |
| PARAIBA | 831563.597 | 0.53 |
| AMAPA | 821072.189 | 0.52 |
| RORAIMA | 276413.47 | 0.17 |
| DISTRITO FEDERAL | 196689.346 | 0.12 |
| ACRE | 41582.194 | 0.03 |
