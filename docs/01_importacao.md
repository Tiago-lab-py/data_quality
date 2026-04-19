# 📥 Guia de Importação e Processamento de Dados HCAI

## Visão Geral

Este guia documenta o processo completo de importação, processamento e validação de dados HCAI, incluindo:
- Upload de arquivos (CSV, Excel, Parquet)
- Processamento de dados extraídos do Oracle
- Conversão de tipos de dados
- Validação de sobreposições temporais
- Consolidação de registros duplicados

## Aba 1: Upload de Arquivo

### Funcionalidade

Permite enviar arquivos de dados locais para processamento.

### Arquivos Suportados

- **CSV** (.csv)
- **Excel** (.xlsx, .xls)
- **Parquet** (.parquet)

### Procedimento

1. Clique em "Escolha um arquivo"
2. Selecione um arquivo suportado
3. O arquivo será salvo em `data/raw/` automaticamente
4. Uma pré-visualização será exibida com as primeiras linhas

### Saída

```
data/raw/[nome_do_arquivo]
```

Exemplo: `data/raw/dados_vendas.csv`

## Aba 3: Processamento de Dados HCAI

### Visão Geral

Processa dados extraídos do Oracle aplicando transformações e validações.

### Pré-requisitos

- Arquivo `data/raw/dic_fic_uc.parquet` deve existir
- Executar primeiro a extração Oracle (ver [Guia de Extração](00_extracao.md))

### Informações do Arquivo

Ao navegar para esta aba, você verá:
- 📊 Número total de registros
- 📋 Número de colunas
- 💾 Tamanho do arquivo em MB

### Configurações de Processamento

#### 1. 📅 Converter para Datetime

Converte as colunas de data/hora para formato datetime:
- **DTHR_INICIO_INTRP_UC**: Data/hora de início da interpretação
- **DATA_HORA_FIM_INT**: Data/hora de fim da interpretação

**Formatos suportados:**
- YYYY-MM-DD HH:MM:SS
- DD/MM/YYYY HH:MM:SS
- YYYYMMDD HH:MM:SS

#### 2. 🔢 Converter para Decimal

Converte a coluna numérica para formato decimal:
- **DURACAO_PERCEBIDA_MINUTOS**: Duração em minutos (Float64)

#### 3. ⏰ Validar Sobreposição Temporal

Remove registros com sobreposição temporal por NUM_UC_UCI:

**Algoritmo:**
1. Agrupa registros por NUM_UC_UCI
2. Ordena cronologicamente
3. Identifica períodos sobrepostos
4. Mantém o registro com **maior duração**

**Exemplo:**

```
NUM_UC_UCI | DTHR_INICIO | DATA_HORA_FIM | DURACAO
------------|------------|-------------|----------
     100   | 2026-03-01 10:00 | 2026-03-01 12:00 | 120 min (MANTÉM)
     100   | 2026-03-01 11:00 | 2026-03-01 11:30 | 30 min  (REMOVE)
     100   | 2026-03-01 13:00 | 2026-03-01 15:00 | 120 min (MANTÉM)
```

#### 4. 🔗 Consolidar Duplicados

Remove registros duplicados baseado em:
- NUM_UC_UCI
- NUM_INTRP_INIC_MAN

**Comportamento:**
- Mantém apenas a **primeira ocorrência**
- Remove duplicatas exatas

**Exemplo:**

```
NUM_UC_UCI | NUM_INTRP_INIC_MAN | Ação
-----------|-------------------|-------
     100   |        10101       | MANTÉM (1ª ocorrência)
     100   |        10101       | REMOVE (duplicada)
     100   |        10102       | MANTÉM (diferente)
```

### Procedimento Completo

1. **Marque as opções de processamento desejadas**
   ```
   ☑️ Converter para Datetime
   ☑️ Converter para Decimal
   ☑️ Validar Sobreposição Temporal
   ☑️ Consolidar Duplicados
   ```

2. **Clique em "🚀 Iniciar Processamento"**

3. **Acompanhe os logs em tempo real**
   - ✅ Confirmações de sucesso (verde)
   - ⚠️  Avisos (amarelo)
   - ❌ Erros (vermelho)

4. **Verifique o resumo final**
   - Registros Iniciais
   - Registros Finais
   - Percentual de Redução

### Saída

Arquivo processado salvo em:
```
data/processed/dic_fic_uc_processed.parquet
```

**Conteúdo:**
- Todos os registros após validações
- Colunas com tipos de dados corretos
- Sem duplicatas
- Sem sobreposições temporais

## Logs de Processamento

### Informações Registradas

Para cada etapa, logs detalhados incluem:

#### 📖 Carregamento
```
📖 Carregando dados do arquivo Parquet...
✅ Dados carregados: 1,250,000 registros
```

#### 📅 Conversão Datetime
```
📅 Convertendo colunas para datetime...
✅ DTHR_INICIO_INTRP_UC: conversão direta realizada
✅ DATA_HORA_FIM_INT: conversão DD/MM/YYYY realizada
```

#### 🔢 Conversão Decimal
```
🔢 Convertendo colunas para decimal...
✅ DURACAO_PERCEBIDA_MINUTOS: convertido para decimal
```

#### ⏰ Sobreposição Temporal
```
⏰ Validando sobreposição temporal por NUM_UC_UCI...
✅ Sobreposições validadas: 15,234 registros removidos
```

#### 🔗 Consolidação
```
🔗 Consolidando registros duplicados...
✅ Duplicados consolidados: 8,456 registros removidos
```

#### 💾 Salvamento
```
💾 Salvando arquivo processado...
✅ Arquivo salvo: data/processed/dic_fic_uc_processed.parquet
📊 Registros finais: 1,226,310
```

### Métricas Finais

```
======================================================================
✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!
======================================================================
📊 Registros Iniciais:    1,250,000
📊 Registros Finais:      1,226,310
📉 Redução:               1.9%
📁 Arquivo: data/processed/dic_fic_uc_processed.parquet
======================================================================
```

## Tratamento de Erros

### Arquivo não encontrado

```
❌ Arquivo não encontrado: data\raw\dic_fic_uc.parquet
```

**Solução:**
- Execute a extração Oracle primeiro (aba 2)
- Verifique se o arquivo existe em `data/raw/`

### Erro ao converter datetime

```
⚠️  DTHR_INICIO_INTRP_UC: falha na conversão - [mensagem de erro]
```

**Possíveis causas:**
- Formato de data não reconhecido
- Valores NULL ou vazios
- Dados corrompidos

**Solução:**
- Verifique o formato das datas nos dados brutos
- Use SQL para limpar valores inválidos

### Erro ao processar sobreposições

```
⚠️  Erro na validação de sobreposição: [mensagem de erro]
```

**Solução:**
- Verifique se as colunas necessárias existem
- Valide o tipo de dados das colunas de data
- Consulte a estrutura do arquivo com `df.schema`

## Fluxo Completo Recomendado

### Cenário: Processar dados HCAI do Oracle

```
1. EXTRAÇÃO (Aba 2)
   ↓
   - Configurar período (ex: 202603)
   - Executar extração completa
   - Arquivo: data/raw/dic_fic_uc.parquet
   
   ↓
2. PROCESSAMENTO (Aba 3)
   ↓
   - Ativar todas as opções de processamento
   - Executar processamento
   - Arquivo: data/processed/dic_fic_uc_processed.parquet
   
   ↓
3. ANÁLISE/QUALIDADE
   ↓
   - Usar dados processados para análise
   - Aplicar regras de qualidade
   - Gerar relatórios
```

## Performance e Otimizações

### Processamento em Memória

O Polars é otimizado para grandes datasets:
- Processamento paralelo automático
- Lazy evaluation quando possível
- Baixo consumo de memória

### Tempos Estimados

| Tamanho do Arquivo | Conversão Datetime | Validação Sobreposição | Consolidação | Total |
|---|---|---|---|---|
| 100k registros | ~2s | ~3s | ~1s | ~6s |
| 1M registros | ~5s | ~15s | ~3s | ~23s |
| 10M registros | ~30s | ~120s | ~20s | ~170s |

### Dicas de Otimização

1. **Divida processamentos muito grandes**
   - Processe por período (ex: mês a mês)
   - Combine resultados depois

2. **Valide antes de processar completo**
   - Use modo teste com 1k registros primeiro
   - Verifique logs detalhadamente

3. **Mantenha limpeza periódica**
   - Remova arquivos intermediários
   - Arquive dados processados antigos

## Próximas Etapas

Após processar os dados:
1. Aplique [Regras de Qualidade](2_Regras_de_Qualidade.md)
2. Visualize [Resultados](3_Resultados.md)
3. Consulte [Histórico](4_Historico.md) de execuções

## Referências

- [Guia de Extração Oracle](00_extracao.md)
- [Documentação Polars](https://pola-rs.github.io/)
- [Documentação Streamlit](https://docs.streamlit.io/)
# Atualizacao 2026-04-19 - Fluxo de Importacao

## Objetivo
- Evitar falhas por clique/rerun do Streamlit durante a carga.
- Garantir processamento de 1 mes por vez.

## Fluxo da tela
1. Usuario clica em `Mostrar conexao Oracle`.
2. A tela apenas exibe os controles Oracle, sem testar automaticamente.
3. Usuario escolhe uma acao explicita:
4. `Testar conexao Oracle`
5. `Iniciar importacao`

## Validacoes obrigatorias
- Antes de iniciar, validar que apenas 1 mes foi selecionado.
- Se a selecao incluir mais de 1 mes, bloquear a execucao e exibir erro.

## Persistencia do bruto
- Gravar os dados por particao mensal:
- `data/raw/dic_fic_uc/ano=YYYY/mes=MM/dic_fic_uc.parquet`
- Evitar sobrescrita de meses diferentes no mesmo arquivo unico.
- Extrair tambem o historico de UC faturada e salvar em:
- `data/raw/UC_faturada.parquet`
- SQL oficial:
- `SQLs/IQS_Historico_UC_faturado.sql`
- A tela de importacao possui acao independente:
- `Extrair Historico UC Faturada`
- Essa acao nao depende da importacao mensal do dic_fic_uc.
- Conexao Oracle dessa acao: somente variaveis do `.env`
- `ORACLE_UID`, `ORACLE_PWD`, `ORACLE_DB`, `ORACLE_LIB_DIR`, `ORACLE_CONFIG_DIR`

## Convivencia com a pagina de previa
- Enquanto existir lock de importacao (`dic_fic_uc.lock` ou `dic_fic_uc_YYYYMM.lock`), a pagina de previa deve ser interrompida.
# Atualizacao 2026-04-19 (2) - Ajuste de UX Oracle

## Mudanca aplicada
- Botao `Mostrar conexao Oracle` removido da interface para evitar disparo indevido de teste.
- A acao de importacao permanece explicita por botao de inicio de extracao.

## Status de execucao
- Quando a execucao eh apenas disparada pelo fluxo da pagina, a mensagem agora fica como `Extracao disparada... acompanhe no terminal`.
- Mensagem `concluida` so deve aparecer quando o processamento realmente ficou sob controle do worker e finalizou.
