# Lab01_PART1_119609

Laboratorio de Fundamentos de Engenharia de Dados - Lucas Menezes Ladeira

## Visao Geral

Este projeto implementa um pipeline em arquitetura Medallion com as camadas Bronze, Silver e Gold para dados de geracao de usinas do ONS. O fluxo principal faz a ingestao dos arquivos brutos, o tratamento analitico em Parquet, a carga para o Postgres e a geracao de metricas de negocio.

## Arquitetura

Fluxo do dado:

```text
Fonte ONS (CSV)
    ->
Python Bronze 
    ->
data/raw/*.csv
    ->
Python Silver
    ->
data/silver/geracao_usina_silver.parquet
    ->
Python Gold + Interface Postgres
    ->
Procedure SQL
    ->
dimensoes + fato_geracao
    ->
queries de negocio
    ->
out/*.md
```

Componentes principais:

- `src/bronze.py`: ingestao dos arquivos raw a partir do `.env`
- `src/silver.py`: orquestracao da camada Silver
- `src/silver_charts.py`: geracao dos graficos da Silver
- `src/silver_summary.py`: resumo de qualidade e relatorio Markdown
- `src/gold.py`: carga da interface no Postgres e chamada da procedure
- `main.py`: orquestra as rodadas dos códigos
- `src/business_metrics.py`: execucao das queries de negocio e geracao dos relatorios em `out/`

## Estrutura do Projeto

```text
data/
  raw/
  silver/
logs/
out/
reports/
  figures/
sql/
  create/
  drop/
  queries/
src/
main.py
requirements.txt
```

## Documentacao da Tarefa

### 1. Bronze

Objetivo:
- baixar os arquivos CSV do ONS sem alterar o conteudo

Entrada:
- URL montada a partir do `.env`

Saida:
- arquivos CSV em `data/raw/`

Script principal:
- `src/bronze.py`

Evidencias geradas:
- arquivos como `data/raw/GERACAO_USINA-2_2026_01.csv`
- log em `logs/bronze.log`

### 2. Silver

Objetivo:
- tratar os dados da Bronze e gerar uma base analitica em Parquet

Transformacoes aplicadas:
- padronizacao de nomes de colunas para `snake_case`
- conversao de `din_instante` para data/hora
- conversao de `val_geracao` para numerico
- preenchimento de `val_geracao` nulo com `0`
- substituicao de campos textuais vazios por `nao_informado`
- remocao de duplicatas

Saidas:
- `data/silver/geracao_usina_silver.parquet`
- `reports/silver_report.md`
- graficos em `reports/figures/`

Scripts principais:
- `src/silver.py`
- `src/silver_charts.py`
- `src/silver_summary.py`

Graficos gerados:
- geracao por hora e por mes com legenda por tipo de usina
- participacao de cada fonte por mes
- participacao por estado
- participacao por combustivel nas termicas
- geracao media mensal por subsistema

### 3. Gold

Objetivo:
- carregar o dado tratado no Postgres usando uma tabela de interface e uma procedure de transferencia

Fluxo:
- `src/gold.py` le o Parquet da Silver
- carrega a tabela `interface_geracao_usina`
- chama `CALL prc_carga_fato_geracao()`

Scripts SQL:
- `sql/create/001_create_interface_geracao.sql`
- `sql/create/002_create_prc_carga_fato_geracao.sql`
- `sql/create/003_create_gold_model.sql`
- `sql/drop/001_drop_gold_objects.sql`

Modelo final:
- `dim_usina`
- `dim_subsistema`
- `dim_estado`
- `fato_geracao`

### 4. Metricas de Negocio

Objetivo:
- responder perguntas analiticas usando SQL e gerar relatorios em Markdown

Consultas criadas:
- `001_fonte_maior_crescimento_fev_mar_2026.sql`
- `002_estado_maior_participacao_trimestre.sql`
- `003_subsistema_dependencia_termica.sql`
- `004_combustivel_termico_por_mes.sql`
- `005_top_10_usinas_trimestre.sql`
- `006_top_3_usinas_por_fonte.sql`

Script executor:
- `src/business_metrics.py`

Saida:
- relatorios em `out/`

## Dicionario de Dados

Base utilizada: geracao de usinas do ONS

Colunas da base tratada:

- `din_instante`: instante de referencia da medicao de geracao
- `id_subsistema`: identificador do subsistema eletrico
- `nom_subsistema`: nome do subsistema eletrico
- `id_estado`: sigla ou identificador do estado
- `nom_estado`: nome do estado
- `cod_modalidadeoperacao`: modalidade de operacao da usina
- `nom_tipousina`: tipo da usina, como hidreletrica, termica, eolica, fotovoltaica ou nuclear
- `nom_tipocombustivel`: combustivel ou fonte energetica principal da usina
- `nom_usina`: nome da usina
- `id_ons`: identificador da usina no ONS
- `ceg`: codigo de empreendimento de geracao
- `val_geracao`: valor de geracao no instante observado

## Qualidade de Dados

Problemas identificados durante a etapa Silver:

- a coluna `val_geracao` possuia 648 valores nulos na Bronze
- campos textuais continham ocorrencias de `-` e strings vazias
- havia necessidade de padronizar colunas para `snake_case`
- `din_instante` precisava ser convertido de texto para data/hora

Tratamentos aplicados:

- `val_geracao` nulo foi preenchido com `0`
- valores `-` e vazios em campos textuais foram convertidos para `nao_informado`
- `din_instante` foi convertido para `datetime`
- duplicatas foram verificadas e nao houve linhas duplicadas no recorte processado

Resumo observado no processamento atual:

- linhas na Bronze: `1.245.707`
- linhas na Silver: `1.245.707`
- duplicatas removidas: `0`
- nulos finais em todas as colunas da Silver: `0`

O resumo completo pode ser consultado em `reports/silver_report.md`.

## Instrucoes de Execucao

### 1. Criar e ativar a venv

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Instalar dependencias

```powershell
pip install -r requirements.txt
```

### 3. Configurar o `.env`

Preencher:

```env
BRONZE_BASE_URL=https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho
BRONZE_FILE_PATTERN=GERACAO_USINA-2_{year}_{month}.csv
BRONZE_YEARS=2026
BRONZE_MONTHS=01,02,03
USER_POSTGRES=...
PASSWORD_POSTGRES=...
DATABASE_POSTGRES=...
```

### 4. Ordem recomendada de execucao

1. Executar a Bronze para baixar os arquivos raw
2. Executar a Silver para gerar o Parquet e o relatorio
3. Criar objetos no Postgres com os scripts em `sql/create/`
4. Executar a Gold para carregar a interface e chamar a procedure
5. Executar as metricas de negocio para gerar os relatórios em `out/`

### 5. Scripts SQL no banco

Criacao:

1. `sql/create/003_create_gold_model.sql`
2. `sql/create/001_create_interface_geracao.sql`
3. `sql/create/002_create_prc_carga_fato_geracao.sql`

Drop:

1. `sql/drop/001_drop_gold_objects.sql`

### 6. Execucao via Python

Entrada principal:

```powershell
python main.py
```

Observacao:
- a `main.py` pode ser ajustada para comentar ou descomentar Bronze, Silver, Gold e metricas conforme a etapa desejada

## Artefatos Gerados

- `data/raw/`: arquivos CSV da Bronze
- `data/silver/`: Parquet tratado
- `reports/silver_report.md`: relatorio da Silver
- `reports/figures/`: graficos da Silver
- `logs/`: logs das camadas
- `out/`: relatorios de metricas de negocio

## Observacoes Finais

- a Gold foi implementada com tabela de interface e procedure de transferencia, deixando a criacao fisica das tabelas no banco fora do pipeline Python
- as metricas de negocio ficaram separadas da carga, em `sql/queries/` e `src/business_metrics.py`
- os relatorios finais podem ser usados como evidencias da execucao do pipeline
