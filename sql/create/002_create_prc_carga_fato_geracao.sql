CREATE OR REPLACE PROCEDURE prc_carga_fato_geracao()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dim_subsistema (id_subsistema, nom_subsistema)
    SELECT src.id_subsistema, src.nom_subsistema
    FROM (
        SELECT
            itf.id_subsistema,
            MAX(itf.nom_subsistema) AS nom_subsistema
        FROM interface_geracao_usina itf
        GROUP BY itf.id_subsistema
    ) src
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_subsistema dim
        WHERE dim.id_subsistema = src.id_subsistema
    );

    UPDATE dim_subsistema dim
    SET nom_subsistema = src.nom_subsistema
    FROM (
        SELECT
            id_subsistema,
            MAX(nom_subsistema) AS nom_subsistema
        FROM interface_geracao_usina
        GROUP BY id_subsistema
    ) src
    WHERE dim.id_subsistema = src.id_subsistema
      AND dim.nom_subsistema IS DISTINCT FROM src.nom_subsistema;

    INSERT INTO dim_estado (id_estado, nom_estado)
    SELECT src.id_estado, src.nom_estado
    FROM (
        SELECT
            itf.id_estado,
            MAX(itf.nom_estado) AS nom_estado
        FROM interface_geracao_usina itf
        GROUP BY itf.id_estado
    ) src
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_estado dim
        WHERE dim.id_estado = src.id_estado
    );

    UPDATE dim_estado dim
    SET nom_estado = src.nom_estado
    FROM (
        SELECT
            id_estado,
            MAX(nom_estado) AS nom_estado
        FROM interface_geracao_usina
        GROUP BY id_estado
    ) src
    WHERE dim.id_estado = src.id_estado
      AND dim.nom_estado IS DISTINCT FROM src.nom_estado;

    INSERT INTO dim_usina (
        id_ons,
        nom_usina,
        nom_tipousina,
        nom_tipocombustivel,
        ceg,
        cod_modalidadeoperacao
    )
    SELECT
        src.id_ons,
        src.nom_usina,
        src.nom_tipousina,
        src.nom_tipocombustivel,
        src.ceg,
        src.cod_modalidadeoperacao
    FROM (
        SELECT
            itf.id_ons,
            MAX(itf.nom_usina) AS nom_usina,
            MAX(itf.nom_tipousina) AS nom_tipousina,
            MAX(itf.nom_tipocombustivel) AS nom_tipocombustivel,
            MAX(itf.ceg) AS ceg,
            MAX(itf.cod_modalidadeoperacao) AS cod_modalidadeoperacao
        FROM interface_geracao_usina itf
        GROUP BY itf.id_ons
    ) src
    WHERE NOT EXISTS (
        SELECT 1
        FROM dim_usina dim
        WHERE dim.id_ons = src.id_ons
    );

    UPDATE dim_usina dim
    SET
        nom_usina = src.nom_usina,
        nom_tipousina = src.nom_tipousina,
        nom_tipocombustivel = src.nom_tipocombustivel,
        ceg = src.ceg,
        cod_modalidadeoperacao = src.cod_modalidadeoperacao
    FROM (
        SELECT
            id_ons,
            MAX(nom_usina) AS nom_usina,
            MAX(nom_tipousina) AS nom_tipousina,
            MAX(nom_tipocombustivel) AS nom_tipocombustivel,
            MAX(ceg) AS ceg,
            MAX(cod_modalidadeoperacao) AS cod_modalidadeoperacao
        FROM interface_geracao_usina
        GROUP BY id_ons
    ) src
    WHERE dim.id_ons = src.id_ons
      AND (
          dim.nom_usina IS DISTINCT FROM src.nom_usina
          OR dim.nom_tipousina IS DISTINCT FROM src.nom_tipousina
          OR dim.nom_tipocombustivel IS DISTINCT FROM src.nom_tipocombustivel
          OR dim.ceg IS DISTINCT FROM src.ceg
          OR dim.cod_modalidadeoperacao IS DISTINCT FROM src.cod_modalidadeoperacao
      );

    INSERT INTO fato_geracao (
        din_instante,
        id_ons,
        id_subsistema,
        id_estado,
        val_geracao
    )
    SELECT
        src.din_instante,
        src.id_ons,
        src.id_subsistema,
        src.id_estado,
        src.val_geracao
    FROM (
        SELECT
            itf.din_instante,
            itf.id_ons,
            itf.id_subsistema,
            itf.id_estado,
            MAX(itf.val_geracao) AS val_geracao
        FROM interface_geracao_usina itf
        GROUP BY
            itf.din_instante,
            itf.id_ons,
            itf.id_subsistema,
            itf.id_estado
    ) src
    WHERE NOT EXISTS (
        SELECT 1
        FROM fato_geracao fato
        WHERE fato.din_instante = src.din_instante
          AND fato.id_ons = src.id_ons
          AND fato.id_subsistema = src.id_subsistema
          AND fato.id_estado = src.id_estado
    );

    UPDATE fato_geracao fato
    SET val_geracao = src.val_geracao
    FROM (
        SELECT
            din_instante,
            id_ons,
            id_subsistema,
            id_estado,
            MAX(val_geracao) AS val_geracao
        FROM interface_geracao_usina
        GROUP BY
            din_instante,
            id_ons,
            id_subsistema,
            id_estado
    ) src
    WHERE fato.din_instante = src.din_instante
      AND fato.id_ons = src.id_ons
      AND fato.id_subsistema = src.id_subsistema
      AND fato.id_estado = src.id_estado
      AND fato.val_geracao IS DISTINCT FROM src.val_geracao;
END;
$$;
