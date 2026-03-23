CREATE TABLE IF NOT EXISTS interface_geracao_usina (
    din_instante TIMESTAMP NOT NULL,
    id_subsistema VARCHAR(3) NOT NULL,
    nom_subsistema VARCHAR(32) NOT NULL,
    id_estado VARCHAR(3) NOT NULL,
    nom_estado VARCHAR(32) NOT NULL,
    cod_modalidadeoperacao VARCHAR(32) NOT NULL,
    nom_tipousina VARCHAR(32) NOT NULL,
    nom_tipocombustivel VARCHAR(32) NOT NULL,
    nom_usina VARCHAR(64) NOT NULL,
    id_ons VARCHAR(32) NOT NULL,
    ceg VARCHAR(32) NOT NULL,
    val_geracao NUMERIC(19,6) NOT NULL
);
