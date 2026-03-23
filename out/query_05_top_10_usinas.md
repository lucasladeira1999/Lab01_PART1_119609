# Top 10 usinas por geracao media no trimestre

## Pergunta de negocio

Quais foram as 10 usinas com maior geracao media no primeiro trimestre de 2026?

## SQL utilizada

```sql
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
```

## Resultado

| id_ons | nom_usina | nom_estado | nom_tipousina | geracao_media_trimestre |
| --- | --- | --- | --- | --- |
| PABM | Belo Monte | PARA | HIDROELÉTRICA | 5525.835451 |
| PRIT60 | Itaipu 60 Hz | PARANA | HIDROELÉTRICA | 4082.978529 |
| PATU | Tucuruí | PARA | HIDROELÉTRICA | 3984.12856 |
| ROUHJI | Jirau | RONDONIA | HIDROELÉTRICA | 3026.008997 |
| ROUHSN | Santo Antônio | RONDONIA | HIDROELÉTRICA | 2867.815986 |
| PAUHTP | Teles Pires | PARA | HIDROELÉTRICA | 1531.114795 |
| SPILS | Ilha Solteira | SAO PAULO | HIDROELÉTRICA | 1368.510602 |
| ALUXG | Xingó | ALAGOAS | HIDROELÉTRICA | 1163.05409 |
| PQU_SPSP_GD | PQU SPSP FOTOV MMGD | SAO PAULO | FOTOVOLTAICA | 1063.972554 |
| MGSSUS | São Simão | MINAS GERAIS | HIDROELÉTRICA | 908.373545 |
