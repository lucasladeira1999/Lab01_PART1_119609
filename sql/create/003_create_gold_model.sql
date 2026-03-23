CREATE TABLE IF NOT EXISTS dim_subsistema (
    id_subsistema VARCHAR(3) PRIMARY KEY,
    nom_subsistema VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_estado (
    id_estado VARCHAR(3) PRIMARY KEY,
    nom_estado VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_usina (
    id_ons VARCHAR(32) PRIMARY KEY,
    nom_usina VARCHAR(64) NOT NULL,
    nom_tipousina VARCHAR(32) NOT NULL,
    nom_tipocombustivel VARCHAR(32) NOT NULL,
    ceg VARCHAR(32) NOT NULL,
    cod_modalidadeoperacao VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS fato_geracao (
    din_instante TIMESTAMP NOT NULL,
    id_ons VARCHAR(32) NOT NULL,
    id_subsistema VARCHAR(3) NOT NULL,
    id_estado VARCHAR(3) NOT NULL,
    val_geracao NUMERIC(19,6) NOT NULL,
    CONSTRAINT pk_fato_geracao
        PRIMARY KEY (din_instante, id_ons, id_subsistema, id_estado),
    CONSTRAINT fk_fato_geracao_usina
        FOREIGN KEY (id_ons)
        REFERENCES dim_usina (id_ons),
    CONSTRAINT fk_fato_geracao_subsistema
        FOREIGN KEY (id_subsistema)
        REFERENCES dim_subsistema (id_subsistema),
    CONSTRAINT fk_fato_geracao_estado
        FOREIGN KEY (id_estado)
        REFERENCES dim_estado (id_estado)
);

CREATE INDEX IF NOT EXISTS idx_fato_geracao_id_ons
    ON fato_geracao (id_ons);

CREATE INDEX IF NOT EXISTS idx_fato_geracao_id_subsistema
    ON fato_geracao (id_subsistema);

CREATE INDEX IF NOT EXISTS idx_fato_geracao_id_estado
    ON fato_geracao (id_estado);
