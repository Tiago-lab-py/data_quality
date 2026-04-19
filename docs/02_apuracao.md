# Guia de Apuracao DEC/FEC por Regional

## Visao Geral

Este guia descreve o fluxo da apuracao de indicadores DEC/FEC na pagina de Apuracao.

A apuracao consolida interrupcoes no nivel percebido e calcula indicadores por regional, usando a base de consumidores faturados do mesmo periodo.

## Objetivo

Calcular, por regional:
- DEC
- FEC
- componentes Liquido e Expurgo
- total Bruto

Com as regras atuais:
- BRUTO = LIQUIDO + EXPURGO
- DEC = CHI / UC_faturada
- FEC = CI / UC_faturada
- A apuracao considera apenas interrupcoes longas com duracao maior ou igual a 3 minutos.

## Fontes de Dados

### 1) Base processada de interrupcoes

Arquivo principal:
- `data/processed/dic_fic_uc_processed.parquet`

Tambem sao aceitas particoes em:
- `data/processed/dic_fic_uc_processed/`
- `data/processed/dic_fic_uc/`

### 2) Base de UC faturada

Arquivo:
- `data/raw/UC_faturada.parquet`

Colunas esperadas:
- `REGIONAL_TOTAL`
- `ANOMES` (formato `YYYY-MM`)
- `UC_FATURADA`

## Pre-requisitos

Antes de rodar a apuracao, execute:

1. Importacao/processamento da base em `data/processed`.
2. Materializacao das colunas de qualidade/percebido em `3_Qualidade_dados`, usando:
3. `Verificar sobreposicao e gravar colunas`

Sem isso, a apuracao pode ficar sem dados validos.

## Colunas Necessarias no Processed

A apuracao usa principalmente:
- `NUM_UC_UCI`
- `NUM_INTRP_PERCEBIDA`
- `TIPO_PROTOC_JUSTIF_UCI`
- `DURACAO_PERCEBIDA_MINUTOS`
- `verificado`
- inicio percebido:
  - `DTHR_INICIO_INTRP_PERCEBIDO` ou `DTHR_INICIO_INTRP_PERCEBIDA`
- fim percebido:
  - `DATA_HORA_FIM_INTRP_PERCEBIDO`
  - `DATA_HORA_FIM_INTR_PERCEBIDO`
  - `DATA_HORA_FIM_INTRP_PERCEBIDA`
  - `DATA_HORA_FIM_INTR_PERCEBIDA`
- regional:
  - `REGIONAL_TOTAL` (preferencial)
  - ou `SIGLA_REGIONAL` (com mapeamento para `REGIONAL_TOTAL`)

## Regras de Negocio da Apuracao

### Filtro de elegibilidade

Uma linha entra na apuracao quando:
- mes da interrupcao = `ANOMES` selecionado
- `verificado = true`
- UC e numero de interrupcao percebida preenchidos
- `DURACAO_PERCEBIDA_MINUTOS >= 3.0`

### Consolidacao

Para evitar duplicidade, consolida no nivel:
- `REGIONAL_TOTAL + NUM_UC_UCI + NUM_INTRP_PERCEBIDA`

Agregacoes nessa etapa:
- `DUR_MIN = max(duracao)`
- `TIPO_0_ALL = all(TIPO_PROTOC_JUSTIF_UCI == "0")`

### Classificacao

Depois de consolidar:
- `LIQUIDO` se `TIPO_0_ALL = true`
- `EXPURGO` caso contrario

### Calculo de CI/CHI por natureza

Por regional e natureza:
- `CI = quantidade de interrupcoes`
- `CHI_HORAS = soma(DUR_MIN) / 60`

### Calculo de indicadores finais

Por regional:
- `DEC_LIQUIDO = CHI_LIQUIDO / UC_FATURADA`
- `FEC_LIQUIDO = CI_LIQUIDO / UC_FATURADA`
- `DEC_EXPURGO = CHI_EXPURGO / UC_FATURADA`
- `FEC_EXPURGO = CI_EXPURGO / UC_FATURADA`
- `DEC_BRUTO = (CHI_LIQUIDO + CHI_EXPURGO) / UC_FATURADA`
- `FEC_BRUTO = (CI_LIQUIDO + CI_EXPURGO) / UC_FATURADA`

Linha `COPEL`:
- agregacao total de todas as regionais.

## Como Usar na Interface

Pagina:
- `4_Apuracao`

Passos:
1. Verifique se `UC_faturada.parquet` existe.
2. Selecione o periodo em `Periodo (ANOMES)`.
3. Clique em `Calcular DEC/FEC`.

Saidas:
- quadro resumido DEC/FEC por regional
- tabela tecnica com metricas numericas
- base de calculo (CI/CHI liquido, expurgo, bruto)
- graficos DEC e FEC

## Diagnostico e Erros Comuns

### Mensagem: sem dados para o periodo

Causas comuns:
- `verificado` nao foi materializado
- colunas percebidas ausentes
- periodo sem linhas elegiveis
- todas as linhas com duracao menor que 3 minutos

Acoes:
1. Rode `Verificar sobreposicao e gravar colunas` em `3_Qualidade_dados`.
2. Confirme no resumo se `verificado_true` > 0.
3. Confira se o periodo existe em `UC_faturada.parquet` e no processed.
4. Verifique se ha linhas com `DURACAO_PERCEBIDA_MINUTOS >= 3`.

### Fim percebido nulo

O pipeline tenta preencher automaticamente com fallback de colunas de fim original.
Se ainda houver nulos, revisar qualidade dos campos de data/hora de origem.

## Arquivos de Implementacao

Principais modulos:
- `app/services/apuracao_percebido.py`
- `app/services/quality_interruptions.py`
- `app/pages/4_Apuracao.py`

## Checklist de Operacao

Antes de fechar uma execucao:
1. Processed carregado para o mes correto.
2. Colunas percebidas materializadas.
3. `verificado_true` maior que zero.
4. `ANOMES` alinhado entre processed e UC faturada.
5. Calculo DEC/FEC concluido sem erro.

